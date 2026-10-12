using System.Globalization;
using System.Net.Http.Json;
using System.Text.Json;

namespace SalesforceSync.Salesforce;

/// <summary>
/// Reads Account + Profile__c by external id, diffs, and writes one Composite Graph per pair
/// (each graph is all-or-nothing in Salesforce).
/// </summary>
public sealed class AccountProfileSync(HttpClient http)
{
    public const string ApiVersion = "v62.0";
    private const string AccountExtId = "External_Id__c";
    private const string ProfileObject = "Profile__c";
    private const string ProfileExtId = "External_Id__c";
    private const string ProfileAccountLookup = "Account__c";
    private const string SyncVersionField = "Sync_Version__c";

    private const int MaxGraphsPerRequest = 75;
    private const int MaxNodesPerRequest = 500;

    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    public async Task<List<SyncResult>> SyncAsync(IReadOnlyList<SyncItem> items, CancellationToken ct = default)
    {
        var results = new List<SyncResult>();
        if (items.Count == 0) return results;

        // 1. Read current state
        var accounts = await QueryByExternalIdAsync("Account", AccountExtId,
            items.SelectMany(i => i.Account.Keys).Append(SyncVersionField),
            items.Select(i => i.AccountExternalId), ct);
        var profiles = await QueryByExternalIdAsync(ProfileObject, ProfileExtId,
            items.SelectMany(i => i.Profile.Keys).Append(ProfileAccountLookup).Append(SyncVersionField),
            items.Select(i => i.ProfileExternalId), ct);

        // 2. Diff + build graphs
        var pending = new Queue<PendingGraph>();
        for (var i = 0; i < items.Count; i++)
        {
            var item = items[i];
            var currentAccount = accounts.GetValueOrDefault(item.AccountExternalId);
            var currentProfile = profiles.GetValueOrDefault(item.ProfileExternalId);

            // Cheap pre-check: Salesforce already holds a newer version
            if (IsNewer(currentAccount, item.Version) || IsNewer(currentProfile, item.Version))
            {
                results.Add(new(item.AccountExternalId, SyncOutcome.Superseded, []));
                continue;
            }

            var graph = BuildGraph($"g{i}", item, currentAccount, currentProfile);
            if (graph is null) results.Add(new(item.AccountExternalId, SyncOutcome.Unchanged, []));
            else pending.Enqueue(graph);
        }

        // 3. Send in batches (<= 75 graphs, <= 500 nodes)
        while (pending.Count > 0)
        {
            var batch = new List<PendingGraph>();
            var nodeCount = 0;
            while (pending.Count > 0
                   && batch.Count < MaxGraphsPerRequest
                   && nodeCount + pending.Peek().Nodes.Count <= MaxNodesPerRequest)
            {
                var g = pending.Dequeue();
                batch.Add(g);
                nodeCount += g.Nodes.Count;
            }

            var payload = new { graphs = batch.Select(g => new { graphId = g.GraphId, compositeRequest = g.Nodes }) };
            using var res = await http.PostAsJsonAsync($"/services/data/{ApiVersion}/composite/graph", payload, Json, ct);
            if (!res.IsSuccessStatusCode)
                throw new HttpRequestException(
                    $"Salesforce {(int)res.StatusCode}: {await res.Content.ReadAsStringAsync(ct)}");

            var body = await res.Content.ReadFromJsonAsync<GraphEnvelope>(Json, ct)
                       ?? throw new InvalidOperationException("Empty composite graph response");

            foreach (var gr in body.Graphs)
            {
                var source = batch.Single(b => b.GraphId == gr.GraphId);
                if (gr.IsSuccessful)
                {
                    results.Add(new(source.Item.AccountExternalId, SyncOutcome.Saved, []));
                    continue;
                }

                var errors = gr.GraphResponse.CompositeResponse
                    .Where(s => s.HttpStatusCode >= 400 && s.Body.ValueKind == JsonValueKind.Array)
                    .SelectMany(s => s.Body.EnumerateArray().Select(e => (
                        Ref: s.ReferenceId,
                        Code: e.TryGetProperty("errorCode", out var c) ? c.GetString() : null,
                        Msg: e.TryGetProperty("message", out var m) ? m.GetString() : null)))
                    .Where(e => e.Code != "PROCESSING_HALTED")
                    .Select(e => $"{e.Ref}: {e.Code} {e.Msg}")
                    .ToList();

                if (errors.Count == 0) pending.Enqueue(source); // halted, not failed -> retry
                else results.Add(new(source.Item.AccountExternalId, SyncOutcome.Failed, errors));
            }
        }

        return results;
    }

    private static bool IsNewer(Dictionary<string, JsonElement>? current, long version)
        => current is not null
           && current.TryGetValue(SyncVersionField, out var v)
           && v.ValueKind == JsonValueKind.Number
           && v.GetDecimal() > version;

    private static PendingGraph? BuildGraph(string graphId, SyncItem item,
        Dictionary<string, JsonElement>? currentAccount, Dictionary<string, JsonElement>? currentProfile)
    {
        var nodes = new List<object>();

        // Account
        var acctChanges = Diff(item.Account, currentAccount);
        string accountIdOrRef;
        if (currentAccount is null)
        {
            acctChanges[AccountExtId] = item.AccountExternalId;
            acctChanges[SyncVersionField] = item.Version;
            nodes.Add(Node("POST", "/sobjects/Account", "acct", acctChanges));
            accountIdOrRef = "@{acct.id}";
        }
        else
        {
            accountIdOrRef = currentAccount["Id"].GetString()!;
            if (acctChanges.Count > 0)
            {
                acctChanges[SyncVersionField] = item.Version;
                nodes.Add(Node("PATCH", $"/sobjects/Account/{accountIdOrRef}", "acct", acctChanges));
            }
        }

        // Profile
        var profChanges = Diff(item.Profile, currentProfile);
        if (currentProfile is null)
        {
            profChanges[ProfileExtId] = item.ProfileExternalId;
            profChanges[ProfileAccountLookup] = accountIdOrRef;
            profChanges[SyncVersionField] = item.Version;
            nodes.Add(Node("POST", $"/sobjects/{ProfileObject}", "prof", profChanges));
        }
        else if (profChanges.Count > 0)
        {
            profChanges[SyncVersionField] = item.Version;
            nodes.Add(Node("PATCH", $"/sobjects/{ProfileObject}/{currentProfile["Id"].GetString()}", "prof", profChanges));
        }

        return nodes.Count == 0 ? null : new PendingGraph(graphId, item, nodes);
    }

    private static object Node(string method, string path, string referenceId, Dictionary<string, object?> body)
        => new { method, url = $"/services/data/{ApiVersion}{path}", referenceId, body };

    // ---------- Read ----------

    private async Task<Dictionary<string, Dictionary<string, JsonElement>>> QueryByExternalIdAsync(
        string sobject, string extField, IEnumerable<string> fields, IEnumerable<string> externalIds, CancellationToken ct)
    {
        var result = new Dictionary<string, Dictionary<string, JsonElement>>(StringComparer.Ordinal);
        var select = string.Join(",", fields.Append("Id").Append(extField).Distinct(StringComparer.OrdinalIgnoreCase));

        foreach (var chunk in externalIds.Distinct().Chunk(100))
        {
            var inList = string.Join(",", chunk.Select(id => $"'{EscapeSoql(id)}'"));
            var soql = $"SELECT {select} FROM {sobject} WHERE {extField} IN ({inList})";
            string? url = $"/services/data/{ApiVersion}/query?q={Uri.EscapeDataString(soql)}";

            while (url is not null)
            {
                var page = await http.GetFromJsonAsync<QueryPage>(url, Json, ct)
                           ?? throw new InvalidOperationException("Empty query response");
                foreach (var rec in page.Records)
                {
                    var dict = rec.EnumerateObject()
                        .Where(p => p.Name != "attributes")
                        .ToDictionary(p => p.Name, p => p.Value.Clone(), StringComparer.OrdinalIgnoreCase);
                    result[dict[extField].GetString()!] = dict;
                }
                url = page.NextRecordsUrl;
            }
        }
        return result;
    }

    private static string EscapeSoql(string s) => s.Replace("\\", "\\\\").Replace("'", "\\'");

    // ---------- Diff ----------

    private static Dictionary<string, object?> Diff(
        IReadOnlyDictionary<string, object?> desired, Dictionary<string, JsonElement>? current)
    {
        if (current is null) return new Dictionary<string, object?>(desired);
        return desired
            .Where(kv => !current.TryGetValue(kv.Key, out var cur) || !ValuesEqual(kv.Value, cur))
            .ToDictionary(kv => kv.Key, kv => kv.Value);
    }

    internal static bool ValuesEqual(object? desired, JsonElement current)
    {
        var curNull = current.ValueKind == JsonValueKind.Null;
        var curStr = current.ValueKind == JsonValueKind.String ? current.GetString() : null;

        return desired switch
        {
            null => curNull,
            string { Length: 0 } => curNull,                       // Salesforce stores "" as null
            string s => curStr == s,
            bool b => current.ValueKind is JsonValueKind.True or JsonValueKind.False && current.GetBoolean() == b,
            DateOnly d => curStr is not null
                          && DateOnly.TryParse(curStr, CultureInfo.InvariantCulture, DateTimeStyles.None, out var cd)
                          && cd == d,
            DateTime dt => SameInstant(new DateTimeOffset(dt.ToUniversalTime()), curStr),
            DateTimeOffset dto => SameInstant(dto, curStr),
            byte or short or int or long or float or double or decimal =>
                current.ValueKind == JsonValueKind.Number
                && current.GetDecimal() == Convert.ToDecimal(desired, CultureInfo.InvariantCulture),
            _ => JsonSerializer.Serialize(desired) == current.GetRawText()
        };
    }

    private static bool SameInstant(DateTimeOffset want, string? sf)
    {
        if (sf is null) return false;
        if (sf.Length > 5 && sf[^5] is '+' or '-') sf = sf.Insert(sf.Length - 2, ":"); // "+0000" -> "+00:00"
        return DateTimeOffset.TryParse(sf, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var cur)
               && cur.UtcTicks / TimeSpan.TicksPerMillisecond == want.UtcTicks / TimeSpan.TicksPerMillisecond;
    }

    // ---------- DTOs ----------

    private sealed record PendingGraph(string GraphId, SyncItem Item, List<object> Nodes);
    private sealed record QueryPage(List<JsonElement> Records, string? NextRecordsUrl);
    private sealed record GraphEnvelope(List<GraphResult> Graphs);
    private sealed record GraphResult(string GraphId, bool IsSuccessful, GraphBody GraphResponse);
    private sealed record GraphBody(List<SubResponse> CompositeResponse);
    private sealed record SubResponse(string ReferenceId, int HttpStatusCode, JsonElement Body);
}
