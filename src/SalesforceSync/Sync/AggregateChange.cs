using System.Text.Json;
using System.Text.Json.Serialization;

namespace SalesforceSync.Sync;

/// <summary>
/// One Account aggregate message, read and validated: its meta and the two snapshots. Each entity type projects the
/// events it needs from it (<see cref="IAggregateProjection"/>); nothing here knows a type.
/// </summary>
public sealed class AggregateChange(EventMeta meta, AccountSnapshot? before, AccountSnapshot? after)
{
    public EventMeta Meta { get; } = meta;
    public AccountSnapshot? Before { get; } = before;
    public AccountSnapshot? After { get; } = after;

    /// <summary>The Account's external id (the same in Before and After).</summary>
    public string AccountKey => (After ?? Before)!.ExternalId!;

    /// <summary>The Account is gone: After is missing, or meta.type says deleted.</summary>
    public bool IsDelete => After is null || Meta.IsDelete();

    /// <summary>What a delete works from: the last state the sync saw.</summary>
    public AccountSnapshot DeletedFrom => (Before ?? After)!;

    /// <summary>The meta of an entity event: the aggregate's time and the type the entity's event has.</summary>
    public EventMeta MetaOf(string type)
    {
        return new EventMeta { Time = Meta.Time, Type = type };
    }
}

/// <summary>What one entity type takes from an aggregate: its events, as stored in its inbox.</summary>
public interface IAggregateProjection
{
    /// <summary>The type's name, e.g. "account".</summary>
    string EntityType { get; }

    /// <summary>The events (JSON, as stored) of this type that the change calls for; none if it changes nothing for the type.</summary>
    IEnumerable<string> ProjectJson(AggregateChange change);
}

/// <summary>What the types' projections share: how events are written, and how a list of children is compared.</summary>
internal static class AggregateProjection
{
    /// <summary>Compact, unset properties left out: what an entity's event looks like in its inbox.</summary>
    private static readonly JsonSerializerOptions StoredJson = new(SalesforceBatchSyncService.EventJson)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public static string Serialize<T>(T value)
    {
        return JsonSerializer.Serialize(value, typeof(T), StoredJson);
    }

    /// <summary>
    /// A child list's events: each child of After that is new or changed (with the Account's key set), then a delete for
    /// each child of Before that After lacks. A delete of the Account deletes every child Before has. A child without an
    /// external id can't be matched: it is passed on as a change, for its type's read to reject, and never taken for a delete.
    /// </summary>
    public static IEnumerable<TEvent> ChildEvents<TChild, TEvent>(AggregateChange change, Func<AccountSnapshot, List<TChild?>?> list,
        Func<TChild, string?> idOf, Action<TChild, string> setAccountKey, Func<string, TChild> keyOnly, Func<EventMeta, TChild, TEvent> toEvent)
        where TChild : class
    {
        if (change.IsDelete)
        {
            foreach (var id in KeyedById(list(change.DeletedFrom), idOf).Keys)
            {
                yield return toEvent(change.MetaOf("deleted"), keyOnly(id));
            }

            yield break;
        }

        var was = KeyedById(change.Before is null ? null : list(change.Before), idOf);
        var now = (list(change.After!) ?? []).OfType<TChild>().ToList();
        foreach (var child in now)
        {
            var old = idOf(child) is { } id ? was.GetValueOrDefault(id) : null;
            if (old is null || Serialize(old) != Serialize(child))
            {
                setAccountKey(child, change.AccountKey);
                yield return toEvent(change.MetaOf(old is null ? "inserted" : "updated"), child);
            }
        }

        var kept = now.Select(idOf).ToHashSet(StringComparer.Ordinal);
        foreach (var id in was.Keys.Where(id => !kept.Contains(id)))
        {
            yield return toEvent(change.MetaOf("deleted"), keyOnly(id));
        }
    }

    /// <summary>Children by external id (the last one if two share an id); a child without one can't be told apart.</summary>
    private static Dictionary<string, TChild> KeyedById<TChild>(List<TChild?>? children, Func<TChild, string?> idOf) where TChild : class
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
}
