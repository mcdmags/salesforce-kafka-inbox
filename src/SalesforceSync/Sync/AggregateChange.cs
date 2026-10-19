namespace SalesforceSync.Sync;

/// <summary>
/// One Account aggregate message, read and validated: its meta and the two snapshots. Each entity type projects the
/// events it needs from it (its <see cref="Persistence.IAggregateIngest"/>); nothing here knows a type.
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
