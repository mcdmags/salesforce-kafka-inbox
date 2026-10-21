using System.Text.Json;
using System.Text.Json.Serialization;
using SalesforceSync.Sync;

namespace SalesforceSync.Persistence;

/// <summary>Shared by ingests: event serialization, child list comparison.</summary>
internal static class AggregateProjection
{
    /// <summary>Compact, unset properties omitted: event form in inbox.</summary>
    private static readonly JsonSerializerOptions StoredJson = new(EventJson.Options)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public static string Serialize<T>(T value)
    {
        return JsonSerializer.Serialize(value, typeof(T), StoredJson);
    }

    /// <summary>
    /// Child list events: each new/changed After child (Account key set), then a delete per Before child After lacks.
    /// Account delete → delete every Before child. Child without external id can't be matched: passed on as a change
    /// for its type's read to reject, never taken as a delete.
    /// </summary>
    public static IEnumerable<TEvent> ChildEvents<TChild, TEvent>(AggregateChange change, Func<AccountSnapshot, List<TChild?>?> list,
        Func<TChild, string?> idOf, Action<TChild, string> setAccountKey, Func<string, TChild> keyOnly, Func<EventMeta, TChild, TEvent> toEvent)
        where TChild : class
    {
        if (change.IsDelete)
        {
            foreach (var id in KeyedById(list(change.DeletedFrom), idOf).Keys)
            {
                yield return toEvent(change.MetaOf(EventType.Deleted), keyOnly(id));
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
                yield return toEvent(change.MetaOf(old is null ? EventType.Inserted : EventType.Updated), child);
            }
        }

        var kept = now.Select(idOf).ToHashSet(StringComparer.Ordinal);
        foreach (var id in was.Keys.Where(id => !kept.Contains(id)))
        {
            yield return toEvent(change.MetaOf(EventType.Deleted), keyOnly(id));
        }
    }

    /// <summary>Children by external id (last wins on dupes); child without one can't be told apart.</summary>
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
