using SalesforceSync.Data.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// The sync of one entity type, run by the engine. A synced object supplies only what varies (how to read an event, the
/// mapping and the queries; e.g. <see cref="AccountSync"/>) and its inbox row type, which selects the inbox table.
/// Registered in <c>AddInboxSync</c>.
/// </summary>
public abstract class EntitySync<TEvent, TRow>(SalesforceBatchSyncService engine, IRepository<TRow> inbox) : IEntitySync
    where TRow : InboxRow, IInboxRow, new()
{
    public string EntityType => Reader.EntityType;

    /// <summary>How an inbox event of this type is read: its entity id and meta (the ingest reads it the same way).</summary>
    protected abstract EventReader<TEvent> Reader { get; }

    /// <summary>
    /// Inbox event (with its entity id) -> what to do with the entity's records: an inserted or updated event upserts them,
    /// a deleted one deletes them (see <see cref="SyncStrategy{TEvent}.Map"/>).
    /// </summary>
    public abstract SyncUnit Map(string entityId, TEvent e);

    /// <summary>The batch's units -> the reads the diff and the foreign keys need (see <see cref="SyncStrategy{TEvent}.Queries"/>).</summary>
    public abstract IEnumerable<SoqlQuery> Queries(IReadOnlyCollection<SyncUnit> units);

    public Task<int> SyncBatchAsync(CancellationToken ct)
    {
        return engine.SyncBatchAsync(Strategy, inbox, ct);
    }

    private SyncStrategy<TEvent> Strategy => _strategy ??= new(Reader, Map, Queries);

    private SyncStrategy<TEvent>? _strategy;
}
