using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using SalesforceSync.Sync;

namespace SalesforceSync.Benchmarks;

/// <summary>
/// Shape of published aggregates: example JSON <see cref="AggregateWorkload"/> copies and modifies. Built-in example by
/// default; <c>--template my.json</c> (or env var <see cref="Variable"/>) swaps in yours, so events get your
/// <c>AccountEvent</c> fields.
/// <para>
/// File = account aggregate message <c>{ "meta": ..., "before": ..., "after": { the account, "profile",
/// "accountRelationships": [ one example ], "contacts": [ one example ] } }</c> (only <c>after</c> read; bare snapshot
/// ok). Every published event is a copy with:
/// </para>
/// <list type="bullet">
/// <item>ids set (account <c>externalId</c>, each child's, relationship <c>relatedAccountExternalId</c>);</item>
/// <item>version markers set (<see cref="Markers"/>): account <c>description</c> "vN" and <c>profile.score</c> N,
/// relationship <c>name</c> "vN", contact <c>lastName</c> "vN";</item>
/// <item>other text: <c>{key}</c> → entity key (unique emails, names);</item>
/// <item>everything else as in file.</item>
/// </list>
/// Nonconforming file refused, problems listed: property payload class lacks (typo, field removed from
/// <c>AccountEvent</c>), wrong value type, missing marker.
/// </summary>
public sealed class AggregateTemplate
{
    /// <summary>Env var naming template file (child benchmark processes inherit it).</summary>
    public const string Variable = "SFSYNC_BENCH_TEMPLATE";

    /// <summary>Properties workload sets itself; end-state check reads them back from org.</summary>
    public static class Markers
    {
        public const string Account = "description", ProfileScore = "score", Relationship = "name", Contact = "lastName";
    }

    private const string BuiltInSource = "the built-in example";

    private const string DefaultJson = /*lang=json,strict*/ """
        {
          "after": {
            "externalId": "",
            "name": "Account {key}",
            "description": "",
            "profile": { "score": 0 },
            "accountRelationships": [
              { "externalId": "", "relatedAccountExternalId": "", "name": "", "type": "Partner", "isActive": true }
            ],
            "contacts": [
              { "externalId": "", "firstName": "Contact", "lastName": "" }
            ]
          }
        }
        """;

    /// <summary>Example benchmark always used.</summary>
    public static AggregateTemplate Default { get; } = Parse(DefaultJson, BuiltInSource);

    private static readonly Lazy<AggregateTemplate> FromEnvironment = new(() =>
        Environment.GetEnvironmentVariable(Variable) is { Length: > 0 } path ? Load(path) : Default);

    private readonly JsonObject _account;
    private readonly JsonObject _relationship;
    private readonly JsonObject _contact;

    private AggregateTemplate(JsonObject account, JsonObject relationship, JsonObject contact)
    {
        _account = account;
        _relationship = relationship;
        _contact = contact;
    }

    /// <summary>Run's template: file named by <see cref="Variable"/>, else <see cref="Default"/>.</summary>
    public static AggregateTemplate Current => FromEnvironment.Value;

    /// <exception cref="InvalidDataException">File doesn't conform.</exception>
    public static AggregateTemplate Load(string path)
    {
        return Parse(File.ReadAllText(path), path);
    }

    /// <exception cref="InvalidDataException">JSON doesn't conform; one problem per line.</exception>
    public static AggregateTemplate Parse(string json, string source)
    {
        var problems = new List<string>();
        JsonObject? root = null;
        try
        {
            root = JsonNode.Parse(json) as JsonObject;
            if (root is null)
            {
                problems.Add("the JSON is not an object");
            }
        }
        catch (JsonException ex)
        {
            problems.Add($"not valid JSON: {ex.Message}");
        }

        if (root is null)
        {
            throw Refused(source, problems);
        }

        // Aggregate message (account = "after", else "before"), or the snapshot itself
        if ((root["after"] ?? root["before"] ?? (root.ContainsKey("meta") ? null : root)) is not JsonObject snapshot)
        {
            throw Refused(source, ["there is no account: the message needs an \"after\" (or \"before\") object"]);
        }

        // Example without a child → built-in child
        var builtIn = source == BuiltInSource ? null : Default;
        var relationship = FirstOf(snapshot, "accountRelationships") ?? builtIn?._relationship ?? [];
        var contact = FirstOf(snapshot, "contacts") ?? builtIn?._contact ?? [];
        var account = (JsonObject)snapshot.DeepClone();
        account.Remove("accountRelationships");
        account.Remove("contacts");

        CheckShape(account, typeof(AccountPayload), "after", problems);
        CheckShape(relationship, typeof(AccountRelationshipPayload), "after.accountRelationships[0]", problems);
        CheckShape(contact, typeof(ContactPayload), "after.contacts[0]", problems);
        CheckMarker(typeof(AccountPayload), Markers.Account, problems);
        CheckMarker(typeof(ProfilePayload), Markers.ProfileScore, problems);
        CheckMarker(typeof(AccountRelationshipPayload), Markers.Relationship, problems);
        CheckMarker(typeof(ContactPayload), Markers.Contact, problems);
        if (account["profile"] is not JsonObject)
        {
            problems.Add("after.profile: needs a \"profile\" object: its \"score\" carries the account's version for the check");
        }

        var template = new AggregateTemplate(account, (JsonObject)relationship.DeepClone(), (JsonObject)contact.DeepClone());
        if (problems.Count == 0)
        {
            // What consumer would make of it: valid aggregate, values of payload classes' types
            var sample = new JsonObject
            {
                ["meta"] = new JsonObject { ["time"] = "2026-01-01T00:00:00Z", ["type"] = "inserted" },
                ["after"] = template.Account("ACC-000001", 1, [template.Relationship("REL-000001-000002", "ACC-000002", 1)], [template.Contact("CON-000001-0", 1)])
            };
            if (!new AccountAggregate().TryRead(sample.ToJsonString(), out _, out var problem))
            {
                problems.Add($"it is not an aggregate the sync can read: {problem}");
            }
        }

        return problems.Count == 0 ? template : throw Refused(source, problems);
    }

    /// <summary>Account snapshot at <paramref name="version"/>, with these children.</summary>
    public JsonObject Account(string key, long version, JsonArray relationships, JsonArray contacts)
    {
        var account = Expand((JsonObject)_account.DeepClone(), key);
        account["externalId"] = key;
        account[Markers.Account] = $"v{version}";
        account["profile"]![Markers.ProfileScore] = version;
        account["accountRelationships"] = relationships;
        account["contacts"] = contacts;
        return account;
    }

    /// <summary>Relationship at next <paramref name="version"/>, naming <paramref name="related"/>.</summary>
    public JsonObject Relationship(string id, string related, long version)
    {
        var relationship = Expand((JsonObject)_relationship.DeepClone(), id);
        relationship["externalId"] = id;
        relationship["relatedAccountExternalId"] = related;
        relationship[Markers.Relationship] = $"v{version}";
        return relationship;
    }

    /// <summary>Contact at next <paramref name="version"/>.</summary>
    public JsonObject Contact(string id, long version)
    {
        var contact = Expand((JsonObject)_contact.DeepClone(), id);
        contact["externalId"] = id;
        contact[Markers.Contact] = $"v{version}";
        return contact;
    }

    private static JsonObject? FirstOf(JsonObject snapshot, string list)
    {
        return snapshot[list] is JsonArray { Count: > 0 } items ? items[0] as JsonObject : null;
    }

    /// <summary>Replaces <c>{key}</c> in every text of copy.</summary>
    private static JsonObject Expand(JsonObject copy, string key)
    {
        foreach (var (name, value) in copy.ToList())
        {
            copy[name] = value switch
            {
                JsonObject o => Expand(o, key),
                JsonValue v when v.TryGetValue<string>(out var text) => text.Replace("{key}", key, StringComparison.Ordinal),
                _ => value?.DeepClone()
            };
        }

        return copy;
    }

    /// <summary>Every example property exists on payload class (case-insensitive), at every level.</summary>
    private static void CheckShape(JsonNode? node, Type type, string path, List<string> problems)
    {
        if (node is JsonArray items)
        {
            var element = type.IsGenericType ? type.GetGenericArguments()[0] : type;
            for (var i = 0; i < items.Count; i++)
            {
                CheckShape(items[i], element, $"{path}[{i}]", problems);
            }
        }
        else if (node is JsonObject obj)
        {
            foreach (var (name, value) in obj)
            {
                var property = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .FirstOrDefault(p => p.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
                if (property is null)
                {
                    problems.Add($"{path}.{name}: {type.Name} has no such property");
                }
                else
                {
                    CheckShape(value, property.PropertyType, $"{path}.{name}", problems);
                }
            }
        }
    }

    private static void CheckMarker(Type type, string property, List<string> problems)
    {
        if (type.GetProperties().All(p => !p.Name.Equals(property, StringComparison.OrdinalIgnoreCase)))
        {
            problems.Add($"{type.Name} has no \"{property}\" property, which the check reads back as the version: " +
                         $"change {nameof(AggregateTemplate)}.{nameof(Markers)} to a property that is synced to Salesforce");
        }
    }

    private static InvalidDataException Refused(string source, IEnumerable<string> problems)
    {
        return new InvalidDataException($"The aggregate template {source} doesn't conform:{Environment.NewLine}  - " +
                                        string.Join($"{Environment.NewLine}  - ", problems));
    }
}
