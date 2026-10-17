using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace SalesforceSync.Sync;

/// <summary>
/// The Kafka event: an Account aggregate, its state before and after the change, each snapshot the Account (with its
/// Profile, as an account event has it) and its children, the account relationships and the contacts:
/// <code>{ "meta": { "time": "2026-10-14T01:02:03.456Z", "type": "updated" },
///   "before": { "externalId": "ACC-1", "name": "Acme", "profile": { ... },
///               "accountRelationships": [ { "externalId": "REL-1", "relatedAccountExternalId": "ACC-2", ... } ],
///               "contacts": [ { "externalId": "CON-1", "lastName": "Lovelace", ... } ] },
///   "after": { ... } }</code>
/// <see cref="TrySplit"/> turns it into the per-entity events the inboxes store and the strategies read, by comparing
/// the snapshots: only what changed is stored, so an unchanged child costs no inbox row, no claim and no Salesforce read.
/// No I/O.
/// </summary>
public static class AccountAggregate
{
    public const string DefaultTopic = "account-changes";

    /// <summary>The snapshot's child lists, by property name, and the entity type (and object name) of their events.</summary>
    private static readonly (string List, string EntityType)[] Children =
    [
        ("accountRelationships", AccountRelationshipSync.Type),
        ("contacts", ContactSync.Type)
    ];

    /// <summary>The property every child event names its Account by (a relationship's first Account, a contact's Account).</summary>
    private const string AccountKey = "accountExternalId";

    // Case-insensitive like the typed events (JsonSerializerDefaults.Web): "Before" and "before" are the same
    private static readonly JsonNodeOptions NodeOptions = new() { PropertyNameCaseInsensitive = true };

    /// <summary>
    /// The per-entity events of an aggregate, each <c>{ "meta": { "time", "type" }, "&lt;entityType&gt;": { ... } }</c>
    /// with the aggregate's meta.time, for its entity type's inbox:
    /// <list type="bullet">
    /// <item>After present (and meta.type not deleted): the Account, when it (or its Profile) changed or Before is
    /// missing; each child of After that is new or changed (as it is in After, with <c>accountExternalId</c> = the
    /// Account's key); a delete (<c>"type": "deleted"</c>, the child's <c>externalId</c> only) for each child of Before
    /// that After doesn't have. An unchanged child, or an unchanged Account, is not stored.</item>
    /// <item>After missing or null, or meta.type deleted: the Account is deleted, with its Profile, and so is every
    /// child Before has, each by a delete event of its own (not left to Salesforce: these Text foreign keys
    /// cascade nothing).</item>
    /// </list>
    /// Snapshots are compared as JSON, structurally (property order doesn't matter, array order does).
    /// </summary>
    /// <returns>False with <paramref name="problem"/> when the message is no aggregate the sync can read.</returns>
    public static bool TrySplit(string json, [NotNullWhen(true)] out List<EntityEvent>? events, [NotNullWhen(false)] out string? problem)
    {
        events = null;
        JsonObject? root;
        try
        {
            root = JsonNode.Parse(json, NodeOptions) as JsonObject;
        }
        catch (JsonException ex)
        {
            problem = $"Not a valid account aggregate event: {ex.Message}";
            return false;
        }

        if (root is null)
        {
            problem = "The event is not a JSON object";
            return false;
        }

        if (!TryReadSnapshots(root, out var before, out var after, out var meta, out problem))
        {
            return false;
        }

        var key = Str((after ?? before)!["externalId"]);
        if (string.IsNullOrWhiteSpace(key))
        {
            problem = "The account needs a non-empty externalId";
            return false;
        }

        if (before is not null && after is not null && Str(before["externalId"]) != key)
        {
            problem = $"before and after are different accounts ('{Str(before["externalId"])}', '{key}')";
            return false;
        }

        events = after is null || meta.IsDelete() ? Deleted(key, before ?? after!, meta) : Changed(key, before, after, meta);
        return true;
    }

    /// <summary>The Account (if it or its Profile changed) and every child that changed or went.</summary>
    private static List<EntityEvent> Changed(string key, JsonObject? before, JsonObject after, EventMeta meta)
    {
        var events = new List<EntityEvent>();
        var account = AccountPart(after);
        if (before is null || !JsonNode.DeepEquals(AccountPart(before), account))
        {
            events.Add(Event(AccountSync.Type, meta.Time!, before is null ? "inserted" : "updated", account));
        }

        foreach (var (list, entityType) in Children)
        {
            var was = KeyedChildrenOf(before, list);
            var now = ChildrenOf(after, list);
            foreach (var child in now)
            {
                // A child without an external id can't be matched: stored as a change, and rejected by its type's read
                var old = Str(child["externalId"]) is { } id ? was.GetValueOrDefault(id) : null;
                if (old is null || !JsonNode.DeepEquals(old, child))
                {
                    var payload = child.DeepClone().AsObject();
                    payload[AccountKey] = key;
                    events.Add(Event(entityType, meta.Time!, old is null ? "inserted" : "updated", payload));
                }
            }

            var kept = now.Select(c => Str(c["externalId"])).ToHashSet(StringComparer.Ordinal);
            events.AddRange(was.Keys.Where(id => !kept.Contains(id)).Select(id => Deleting(entityType, id, meta.Time!)));
        }
        return events;
    }

    /// <summary>The Account, with its Profile, and every child of the last snapshot, each deleted on its own.</summary>
    private static List<EntityEvent> Deleted(string key, JsonObject last, EventMeta meta)
    {
        // What the account's delete needs: its key, and its Profile's if the Profile has a key of its own
        var account = new JsonObject { ["externalId"] = key };
        if (Str(last["profile"]?["externalId"]) is { } profileKey)
        {
            account["profile"] = new JsonObject { ["externalId"] = profileKey };
        }

        List<EntityEvent> events = [Event(AccountSync.Type, meta.Time!, "deleted", account)];
        foreach (var (list, entityType) in Children)
        {
            events.AddRange(KeyedChildrenOf(last, list).Keys.Select(id => Deleting(entityType, id, meta.Time!)));
        }
        return events;
    }

    /// <summary>meta (a time with an offset, a type) and the two snapshots, each an object, null or absent; not both missing.</summary>
    private static bool TryReadSnapshots(JsonObject root, out JsonObject? before, out JsonObject? after,
        [NotNullWhen(true)] out EventMeta? meta, [NotNullWhen(false)] out string? problem)
    {
        (before, after, meta, problem) = (null, null, null, null);
        try
        {
            meta = root["meta"]?.Deserialize<EventMeta>(SalesforceBatchSyncService.EventJson);
        }
        catch (JsonException ex)
        {
            problem = $"Not a valid meta: {ex.Message}";
            return false;
        }

        if (meta?.Timestamp() is null)
        {
            problem = "The event time must be an ISO-8601 time with an offset or Z, e.g. 2026-10-14T01:02:03.456Z";
        }
        else if (!meta.IsInsertOrUpdate() && !meta.IsDelete())
        {
            problem = "The event type must be \"inserted\", \"updated\" or \"deleted\"";
        }
        else if (!TrySnapshot(root, "before", out before) || !TrySnapshot(root, "after", out after))
        {
            problem = "before and after must each be an account object, or null";
        }
        else if (before is null && after is null)
        {
            problem = "The event needs before or after";
        }

        return problem is null;
    }

    private static bool TrySnapshot(JsonObject root, string name, out JsonObject? snapshot)
    {
        var node = root[name];
        snapshot = node as JsonObject;
        return node is null or JsonObject;
    }

    /// <summary>The Account and its Profile: the snapshot without its child lists.</summary>
    private static JsonObject AccountPart(JsonObject snapshot)
    {
        var account = new JsonObject(NodeOptions);
        foreach (var (name, value) in snapshot)
        {
            if (!Children.Any(c => c.List.Equals(name, StringComparison.OrdinalIgnoreCase)))
            {
                account[name] = value?.DeepClone();
            }
        }
        return account;
    }

    /// <summary>A snapshot's children of one list (an absent list has none).</summary>
    private static List<JsonObject> ChildrenOf(JsonObject? snapshot, string list)
    {
        return [.. (snapshot?[list] as JsonArray ?? []).OfType<JsonObject>()];
    }

    /// <summary>
    /// A snapshot's children of one list, by external id (the last one, should two share one). A child without one
    /// can't be told apart from another, so it is never taken for a delete.
    /// </summary>
    private static Dictionary<string, JsonObject> KeyedChildrenOf(JsonObject? snapshot, string list)
    {
        var children = new Dictionary<string, JsonObject>(StringComparer.Ordinal);
        foreach (var child in ChildrenOf(snapshot, list))
        {
            if (Str(child["externalId"]) is { Length: > 0 } id)
            {
                children[id] = child;
            }
        }
        return children;
    }

    private static EntityEvent Deleting(string entityType, string id, string time)
    {
        return Event(entityType, time, "deleted", new JsonObject { ["externalId"] = id });
    }

    private static EntityEvent Event(string entityType, string time, string type, JsonObject payload)
    {
        var json = new JsonObject
        {
            ["meta"] = new JsonObject { ["time"] = time, ["type"] = type },
            [entityType] = payload
        };
        return new EntityEvent(entityType, json.ToJsonString());
    }

    private static string? Str(JsonNode? node)
    {
        return node is JsonValue value && value.TryGetValue<string>(out var s) ? s : null;
    }
}

/// <summary>One entity's event out of an aggregate: its type (which inbox stores it) and the event as stored.</summary>
public sealed record EntityEvent(string EntityType, string Json);