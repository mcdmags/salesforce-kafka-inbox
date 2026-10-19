using System.Globalization;
using System.Text.Json.Nodes;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.Sync;
using SalesforceSync.ToSalesforce;

namespace SalesforceSync.Benchmarks;

/// <summary>
/// Drain benchmark input: account aggregates as upstream publishes them (<c>{ meta, before, after }</c>, see
/// <see cref="AccountAggregate"/>), plus the state the org must end in. Every aggregate changes its Account (Description
/// "vN", Profile score N; N = account version). Now and then also adds/changes/removes a relationship or contact
/// (removed one may return as new record). A few accounts deleted with all children.
/// Aggregates are copies of an example (<see cref="AggregateTemplate"/>), so they have your event's fields.
/// Benchmark stores what the types' ingests make of each aggregate, like the Kafka consumer. Seeded, no I/O: same
/// workload every iteration.
/// </summary>
public sealed class AggregateWorkload
{
    /// <summary>Share of aggregates touching one of the account's relationships / contacts.</summary>
    private const double RelationshipShare = 0.15, ContactShare = 0.15;

    /// <summary>Touched child already present: deleted this often, else changed.</summary>
    private const double RemovalShare = 0.3;

    /// <summary>
    /// Aggregate of a deletable account (<see cref="IsDeletable"/>) with a few versions is its delete this often;
    /// deleted account gets no more aggregates.
    /// </summary>
    private const double AccountDeleteShare = 0.02;

    /// <summary>Contacts per account (by key; each may come and go).</summary>
    private const int ContactsPerAccount = 3;

    public static readonly DateTimeOffset BaseTime = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>Entity as org must end: present at newest version (and Accounts it names), or absent.</summary>
    /// <param name="Account">Account a relationship or contact names (Text foreign key).</param>
    /// <param name="Related">Relationship's other Account.</param>
    /// <param name="Present">Record must exist.</param>
    /// <param name="Version">Newest version: Account Description "vN" and Profile score, child "vN".</param>
    public sealed record Expected(bool Present, long Version, string? Account = null, string? Related = null);

    private readonly Random _random;
    private readonly long[] _versions;
    private readonly JsonObject?[] _snapshots; // each account's last "after", the next aggregate's "before"
    private readonly bool[] _deleted;
    private readonly Dictionary<string, long> _childVersions = new(StringComparer.Ordinal);
    private readonly HashSet<(string Type, string Key)> _children = [];

    private readonly AggregateTemplate _template;

    private AggregateWorkload(int accounts, int seed, AggregateTemplate template)
    {
        _template = template;
        _random = new Random(seed);
        _versions = new long[accounts];
        _snapshots = new JsonObject?[accounts];
        _deleted = new bool[accounts];
    }

    /// <summary>Aggregates (Kafka messages) in publish order; n-th meta.time = <see cref="BaseTime"/> + n ms.</summary>
    public List<string> Aggregates { get; } = [];

    /// <summary>Every entity named, by entity type (<see cref="AccountSync.Type"/>, ...) and key.</summary>
    public Dictionary<(string Type, string Key), Expected> Final { get; } = [];

    /// <summary>
    /// <paramref name="aggregates"/> aggregates over <paramref name="accounts"/> accounts: random account each, sometimes
    /// a burst of 2-5 in a row (coalescing and ordering pressure).
    /// </summary>
    /// <param name="aggregates">Aggregate count (Kafka messages).</param>
    /// <param name="accounts">Distinct account count.</param>
    /// <param name="seed">Random seed: same seed + template = same workload.</param>
    /// <param name="template">Aggregate shape; null = run's (<see cref="AggregateTemplate.Current"/>).</param>
    public static AggregateWorkload Generate(int aggregates, int accounts, int seed = 42, AggregateTemplate? template = null)
    {
        var workload = new AggregateWorkload(accounts, seed, template ?? AggregateTemplate.Current);
        workload.Run(aggregates);
        return workload;
    }

    private void Run(int aggregates)
    {
        while (Aggregates.Count < aggregates)
        {
            var account = _random.Next(_versions.Length);
            var burst = _random.NextDouble() < 0.1 ? _random.Next(2, 6) : 1;
            for (var i = 0; i < burst && Aggregates.Count < aggregates && !_deleted[account]; i++)
            {
                var before = _snapshots[account];
                if (before is not null && _versions[account] >= 2 && IsDeletable(account) && _random.NextDouble() < AccountDeleteShare)
                {
                    _deleted[account] = true;
                    _snapshots[account] = null;
                    Publish("deleted", before, null);
                    continue;
                }

                var after = Next(account, ++_versions[account], before);
                _snapshots[account] = after;
                Publish(before is null ? "inserted" : "updated", before, after);
            }
        }

        Expect();
    }

    private void Publish(string type, JsonObject? before, JsonObject? after)
    {
        var time = BaseTime.AddMilliseconds(Aggregates.Count + 1);
        Aggregates.Add(new JsonObject
        {
            ["meta"] = new JsonObject
            {
                ["time"] = time.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture),
                ["type"] = type
            },
            ["before"] = before?.DeepClone(),
            ["after"] = after?.DeepClone()
        }.ToJsonString());
    }

    /// <summary>Account's next snapshot: Account changed, children as before, maybe one touched.</summary>
    private JsonObject Next(int account, long version, JsonObject? before)
    {
        var relationships = before?["accountRelationships"]?.DeepClone().AsArray() ?? [];
        var contacts = before?["contacts"]?.DeepClone().AsArray() ?? [];

        // Relationship names a second Account: already published, never deleted
        var other = _random.Next(_versions.Length);
        if (_random.NextDouble() < RelationshipShare && other != account && _versions[other] > 0 && !IsDeletable(other))
        {
            Touch(relationships, AccountRelationshipSync.Type, $"REL-{account:D6}-{other:D6}", (id, v) => _template.Relationship(id, Key(other), v));
        }

        if (_random.NextDouble() < ContactShare)
        {
            Touch(contacts, ContactSync.Type, $"CON-{account:D6}-{_random.Next(ContactsPerAccount)}", _template.Contact);
        }

        return _template.Account(Key(account), version, relationships, contacts);
    }

    /// <summary>Removes child if listed (<see cref="RemovalShare"/>), else adds it at next version.</summary>
    private void Touch(JsonArray children, string type, string id, Func<string, long, JsonObject> build)
    {
        var existing = children.FirstOrDefault(c => (string?)c?["externalId"] == id);
        if (existing is not null)
        {
            children.Remove(existing);
            if (_random.NextDouble() < RemovalShare)
            {
                return;
            }
        }

        _children.Add((type, id));
        children.Add(build(id, _childVersions[id] = _childVersions.GetValueOrDefault(id) + 1));
    }

    /// <summary>
    /// A tenth of accounts may be deleted. No other account's relationship names them, so a relationship never
    /// outlives an Account it names (source removes it first).
    /// </summary>
    private static bool IsDeletable(int account)
    {
        return account % 10 == 9;
    }

    /// <summary>
    /// End state: account present at its version unless deleted; child present at its version, naming its Accounts,
    /// if its account's last snapshot has it, else absent (removed, or deleted with account).
    /// </summary>
    private void Expect()
    {
        for (var account = 0; account < _versions.Length; account++)
        {
            if (_versions[account] > 0)
            {
                Final[(AccountSync.Type, Key(account))] = new Expected(!_deleted[account], _versions[account]);
            }
        }

        foreach (var child in _children)
        {
            Final[child] = new Expected(false, _childVersions[child.Key]);
        }

        foreach (var snapshot in _snapshots.OfType<JsonObject>())
        {
            var account = (string)snapshot["externalId"]!;
            foreach (var r in snapshot["accountRelationships"]!.AsArray())
            {
                var key = (string)r!["externalId"]!;
                Final[(AccountRelationshipSync.Type, key)] =
                    new Expected(true, _childVersions[key], account, (string?)r["relatedAccountExternalId"]);
            }

            foreach (var c in snapshot["contacts"]!.AsArray())
            {
                var key = (string)c!["externalId"]!;
                Final[(ContactSync.Type, key)] = new Expected(true, _childVersions[key], account);
            }
        }
    }

    // ---------- The check ----------

    /// <summary>
    /// Compares org with <see cref="Final"/>. Org rejects no out-of-order write, so a race lands. Checks: every entity
    /// at newest version on every record it writes (relationship/contact names right, existing Accounts), every
    /// removed/deleted one absent, each record's write history only moving forward.
    /// </summary>
    /// <returns>What is wrong per entity, and how many records got an older version after a newer one.</returns>
    public (List<string> Wrong, int Regressions) Check(FakeSalesforceStore org)
    {
        var check = new OrgCheck(org);
        var wrong = Final.Select(e => check.Problem(e.Key.Type, e.Key.Key, e.Value)).OfType<string>().ToList();
        return (wrong, check.Regressions);
    }

    private sealed class OrgCheck(FakeSalesforceStore org)
    {
        private const string RelationshipObject = "Account_Relationship__c";

        /// <summary>Object of an entity type's record (Account's Profile__c goes with it).</summary>
        private static readonly Dictionary<string, string> ObjectOf = new()
        {
            [AccountSync.Type] = "Account",
            [AccountRelationshipSync.Type] = RelationshipObject,
            [ContactSync.Type] = "Contact"
        };

        public int Regressions { get; private set; }

        /// <summary>Null when as expected, else what is wrong.</summary>
        public string? Problem(string type, string key, Expected expected)
        {
            return !expected.Present ? Absent(type, key)
                : type == AccountSync.Type ? Account(key, expected.Version)
                : type == AccountRelationshipSync.Type ? Relationship(key, expected)
                : Contact(key, expected);
        }

        /// <summary>Removed child, or deleted account with Profile: no record left.</summary>
        private string? Absent(string type, string key)
        {
            return org.FindByExternalId(ObjectOf[type], key) is null && (type != AccountSync.Type || org.FindByExternalId("Profile__c", key) is null)
                ? null
                : $"{key}: expected deleted, still in the org";
        }

        /// <summary>Newest version on both records (Account Description "vN", Profile Score__c = N).</summary>
        private string? Account(string key, long expected)
        {
            var accountVersions = Versions("Account", key, "Description");
            var profileVersions = Forward([.. (org.History("Profile__c", key) ?? [])
                .Where(w => w.ContainsKey("Score__c"))
                .Select(w => (long)decimal.Parse(w["Score__c"]!.ToJsonString(), NumberStyles.Float, CultureInfo.InvariantCulture))]);
            return accountVersions.LastOrDefault() == expected && profileVersions.LastOrDefault() == expected
                ? null
                : $"{key}: expected v{expected}, account v{accountVersions.LastOrDefault()}, profile v{profileVersions.LastOrDefault()}";
        }

        /// <summary>Newest version (Name "vN"), linked to right two Accounts (Text foreign keys), both existing.</summary>
        private string? Relationship(string key, Expected expected)
        {
            var versions = Versions(RelationshipObject, key, "Name");
            var record = org.FindByExternalId(RelationshipObject, key);
            return versions.LastOrDefault() == expected.Version && record is not null &&
                   org.FindByExternalId("Account", expected.Account!) is not null && org.FindByExternalId("Account", expected.Related!) is not null &&
                   (string?)record["AccountId__c"] == expected.Account && (string?)record["RelatedAccountId__c"] == expected.Related
                ? null
                : $"{key}: expected v{expected.Version} linking {expected.Account}->{expected.Related}, got v{versions.LastOrDefault()} " +
                  $"linking {record?["AccountId__c"]}->{record?["RelatedAccountId__c"]}";
        }

        /// <summary>Newest version (LastName "vN"), naming its Account.</summary>
        private string? Contact(string key, Expected expected)
        {
            var versions = Versions("Contact", key, "LastName");
            var record = org.FindByExternalId("Contact", key);
            return versions.LastOrDefault() == expected.Version && (string?)record?["AccountId__c"] == expected.Account
                ? null
                : $"{key}: expected v{expected.Version} of {expected.Account}, got v{versions.LastOrDefault()} of {record?["AccountId__c"]}";
        }

        /// <summary>
        /// Versions ("vN" in <paramref name="field"/>) in org receive order, of the record with the key now (re-added
        /// child = new record, own history).
        /// </summary>
        private List<long> Versions(string sobject, string key, string field)
        {
            return Forward([.. (org.History(sobject, key) ?? [])
                .Where(w => w.ContainsKey(field))
                .Select(w => long.Parse(((string)w[field]!)[1..], CultureInfo.InvariantCulture))]);
        }

        /// <summary>Counts a regression unless versions only move forward.</summary>
        private List<long> Forward(List<long> versions)
        {
            if (!versions.Zip(versions.Skip(1)).All(p => p.First < p.Second))
            {
                Regressions++;
            }

            return versions;
        }
    }

    private static string Key(int account)
    {
        return $"ACC-{account:D6}";
    }
}