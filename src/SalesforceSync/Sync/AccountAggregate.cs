using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace SalesforceSync.Sync;

/// <summary>
/// The Kafka event: an Account aggregate, its state before and after the change. Each snapshot holds the Account (with
/// its Profile, as an account event has it) and its children, the account relationships and the contacts:
/// <code>{ "meta": { "time": "2026-10-14T01:02:03.456Z", "type": "updated" },
///   "before": { "externalId": "ACC-1", "name": "Acme", "profile": { ... },
///               "accountRelationships": [ { "externalId": "REL-1", "relatedAccountExternalId": "ACC-2", ... } ],
///               "contacts": [ { "externalId": "CON-1", "lastName": "Lovelace", ... } ] },
///   "after": { ... } }</code>
/// <see cref="TrySplit"/> compares the snapshots and turns them into the per-entity events the inboxes store and the
/// strategies read. Only what changed is stored: an unchanged child costs no inbox row, no claim, no Salesforce read.
/// No I/O.
/// </summary>
public static class AccountAggregate
{
    public const string DefaultTopic = "account-changes";

    /// <summary>Compact, unset properties left out: what an entity's event looks like in its inbox.</summary>
    private static readonly JsonSerializerOptions StoredJson = new(SalesforceBatchSyncService.EventJson)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    /// <summary>
    /// The per-entity events of an aggregate, for its entity type's inbox. Each is
    /// <c>{ "meta": { "time", "type" }, "&lt;entityType&gt;": { ... } }</c> with the aggregate's meta.time:
    /// <list type="bullet">
    /// <item>After present (and meta.type not deleted): the Account, when it (or its Profile) changed or Before is
    /// missing; each child of After that is new or changed (as in After, with <c>accountExternalId</c> = the
    /// Account's key); a delete (<c>"type": "deleted"</c>, the child's <c>externalId</c> only) for each child of Before
    /// that After lacks. An unchanged child or Account is not stored.</item>
    /// <item>After missing or null, or meta.type deleted: the Account is deleted, with its Profile, and so is every
    /// child Before has, each by its own delete event (not left to Salesforce: these Text foreign keys
    /// cascade nothing).</item>
    /// </list>
    /// Snapshots are compared as the typed payloads they are read into; a property the payload classes lack is ignored.
    /// </summary>
    /// <returns>False with <paramref name="problem"/> when the message is no aggregate the sync can read.</returns>
    public static bool TrySplit(string json, [NotNullWhen(true)] out List<EntityEvent>? events, [NotNullWhen(false)] out string? problem)
    {
        events = null;
        AccountAggregateEvent? aggregate;
        try
        {
            aggregate = JsonSerializer.Deserialize<AccountAggregateEvent>(json, SalesforceBatchSyncService.EventJson);
        }
        catch (JsonException ex)
        {
            problem = $"Not a valid account aggregate event: {ex.Message}";
            return false;
        }

        if (aggregate is null)
        {
            problem = "The event is not a JSON object";
            return false;
        }

        var (meta, before, after) = (aggregate.Meta, aggregate.Before, aggregate.After);
        problem = ProblemWith(meta, before, after);
        if (problem is not null)
        {
            return false;
        }

        var key = (after ?? before)!.ExternalId!;
        events = after is null || meta!.IsDelete() ? Deleted(key, before ?? after!, meta!) : Changed(key, before, after, meta!);
        return true;
    }

    /// <summary>What is wrong with the event's meta (a time with an offset, a type) and snapshots (not both missing, one account), if anything.</summary>
    private static string? ProblemWith(EventMeta? meta, AccountSnapshot? before, AccountSnapshot? after)
    {
        if (meta?.Timestamp() is null)
        {
            return "The event time must be an ISO-8601 time with an offset or Z, e.g. 2026-10-14T01:02:03.456Z";
        }

        if (!meta.IsInsertOrUpdate() && !meta.IsDelete())
        {
            return "The event type must be \"inserted\", \"updated\" or \"deleted\"";
        }

        if (before is null && after is null)
        {
            return "The event needs before or after";
        }

        var (account, other) = (after ?? before, after is null ? null : before);
        return string.IsNullOrWhiteSpace(account!.ExternalId) ? "The account needs a non-empty externalId"
            : other is not null && other.ExternalId != account.ExternalId
                ? $"before and after are different accounts ('{other.ExternalId}', '{account.ExternalId}')"
                : null;
    }

    /// <summary>The Account (if it or its Profile changed) and every child that changed or went.</summary>
    private static List<EntityEvent> Changed(string key, AccountSnapshot? before, AccountSnapshot after, EventMeta meta)
    {
        var events = new List<EntityEvent>();
        if (before is null || Serialize<AccountPayload>(before) != Serialize<AccountPayload>(after))
        {
            events.Add(Event(AccountSync.Type, meta, before is null ? "inserted" : "updated", m => new AccountEvent(m, after)));
        }

        AddChanges(events, key, meta, before?.AccountRelationships, after.AccountRelationships, AccountRelationshipSync.Type,
            c => c.ExternalId, (c, accountKey) => c.AccountExternalId = accountKey,
            (m, c) => new AccountRelationshipEvent(m, c));
        AddChanges(events, key, meta, before?.Contacts, after.Contacts, ContactSync.Type,
            c => c.ExternalId, (c, accountKey) => c.AccountExternalId = accountKey,
            (m, c) => new ContactEvent(m, c));
        return events;
    }

    /// <summary>The Account, with its Profile, and every child of the last snapshot, each deleted on its own.</summary>
    private static List<EntityEvent> Deleted(string key, AccountSnapshot last, EventMeta meta)
    {
        // What the account's delete needs: its key, and its Profile's if the Profile has a key of its own
        var account = new AccountPayload { ExternalId = key };
        if (last.Profile?.ExternalId is { } profileKey)
        {
            account.Profile = new ProfilePayload { ExternalId = profileKey };
        }

        List<EntityEvent> events = [Event(AccountSync.Type, meta, "deleted", m => new AccountEvent(m, account))];
        events.AddRange(KeyedByExternalId(last.AccountRelationships, c => c.ExternalId).Keys
            .Select(id => Deleting(AccountRelationshipSync.Type, id, meta)));
        events.AddRange(KeyedByExternalId(last.Contacts, c => c.ExternalId).Keys.Select(id => Deleting(ContactSync.Type, id, meta)));
        return events;
    }

    /// <summary>One list of children: those of After that are new or changed, then a delete for those that went.</summary>
    private static void AddChanges<TChild>(List<EntityEvent> events, string key, EventMeta meta, List<TChild?>? before, List<TChild?>? after,
        string entityType, Func<TChild, string?> idOf, Action<TChild, string> setAccountKey, Func<EventMeta, TChild, object> toEvent)
        where TChild : class
    {
        var was = KeyedByExternalId(before, idOf);
        var now = (after ?? []).OfType<TChild>().ToList();
        foreach (var child in now)
        {
            // A child without an external id can't be matched: stored as a change, and rejected by its type's read
            var old = idOf(child) is { } id ? was.GetValueOrDefault(id) : null;
            if (old is null || Serialize(old) != Serialize(child))
            {
                setAccountKey(child, key);
                events.Add(Event(entityType, meta, old is null ? "inserted" : "updated", m => toEvent(m, child)));
            }
        }

        var kept = now.Select(idOf).ToHashSet(StringComparer.Ordinal);
        events.AddRange(was.Keys.Where(id => !kept.Contains(id)).Select(id => Deleting(entityType, id, meta)));
    }

    /// <summary>
    /// A snapshot's children of one list, by external id (the last one if two share an id). A child without an id
    /// can't be told apart from another, so it is never taken for a delete.
    /// </summary>
    private static Dictionary<string, TChild> KeyedByExternalId<TChild>(List<TChild?>? children, Func<TChild, string?> idOf)
        where TChild : class
    {
        var keyed = new Dictionary<string, TChild>(StringComparer.Ordinal);
        foreach (var child in (children ?? []).OfType<TChild>())
        {
            if (idOf(child) is { Length: > 0 } id)
            {
                keyed[id] = child;
            }
        }
        return keyed;
    }

    private static EntityEvent Deleting(string entityType, string id, EventMeta meta)
    {
        return entityType == ContactSync.Type
            ? Event(entityType, meta, "deleted", m => new ContactEvent(m, new ContactPayload { ExternalId = id }))
            : Event(entityType, meta, "deleted", m => new AccountRelationshipEvent(m, new AccountRelationshipPayload { ExternalId = id }));
    }

    private static EntityEvent Event(string entityType, EventMeta aggregateMeta, string type, Func<EventMeta, object> toEvent)
    {
        var meta = new EventMeta { Time = aggregateMeta.Time, Type = type };
        return new EntityEvent(entityType, Serialize(toEvent(meta)));
    }

    private static string Serialize<T>(T value)
    {
        return JsonSerializer.Serialize(value, typeof(T), StoredJson);
    }
}

/// <summary>The Kafka message: meta and the Account aggregate's snapshots before and after the change.</summary>
public sealed record AccountAggregateEvent(EventMeta? Meta, AccountSnapshot? Before, AccountSnapshot? After);

/// <summary>An Account as an aggregate snapshot has it: the account event's Account and Profile, plus its children.</summary>
public sealed class AccountSnapshot : AccountPayload
{
    public List<AccountRelationshipPayload?>? AccountRelationships { get; set; }
    public List<ContactPayload?>? Contacts { get; set; }
}

/// <summary>One entity's event out of an aggregate: its type (which inbox stores it) and the event as stored.</summary>
public sealed record EntityEvent(string EntityType, string Json);