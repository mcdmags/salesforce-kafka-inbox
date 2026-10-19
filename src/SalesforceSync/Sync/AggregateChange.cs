namespace SalesforceSync.Sync;

/// <summary>
/// One validated Account aggregate message: meta + two snapshots. Each entity type projects its events from it
/// (<see cref="Persistence.IAggregateIngest"/>); knows no type.
/// </summary>
public sealed class AggregateChange(EventMeta meta, AccountSnapshot? before, AccountSnapshot? after)
{
    public EventMeta Meta { get; } = meta;
    public AccountSnapshot? Before { get; } = before;
    public AccountSnapshot? After { get; } = after;

    /// <summary>Account's external id (same in Before and After).</summary>
    public string AccountKey => (After ?? Before)!.ExternalId!;

    /// <summary>Account gone: After missing, or meta.type deleted.</summary>
    public bool IsDelete => After is null || Meta.IsDelete();

    /// <summary>Delete's source: last state the sync saw.</summary>
    public AccountSnapshot DeletedFrom => (Before ?? After)!;

    /// <summary>Entity event's meta: aggregate's time + entity event's type.</summary>
    public EventMeta MetaOf(string type)
    {
        return new EventMeta { Time = Meta.Time, Type = type };
    }
}
