using System.Diagnostics.CodeAnalysis;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// What varies per synced object on the Salesforce side: what an event asks of Salesforce (the records to upsert, or to
/// delete), and what to read from Salesforce to diff them and find their parents. It knows nothing of aggregates or
/// of how events get into the inbox.
/// <see cref="SalesforceBatchSyncService"/> does everything else (claiming, ordering, coalescing, rate limits,
/// batching, the read, the writes and deletes, retries, dead-lettering). The engine and the SQL never name an event
/// field: they go through the <paramref name="Reader"/>.
/// </summary>
/// <typeparam name="TEvent">The inbox payload (the whole event envelope).</typeparam>
/// <param name="Reader">How to read an inbox event: its entity id and meta (shared with ingest).</param>
/// <param name="Map">
/// The action delegate: inbox event (with its entity id) -> what to do with the entity's records. An inserted or
/// updated event upserts them (whether a record is created or updated is decided by what Salesforce has), a deleted
/// one deletes them (<see cref="SObjectRecord.Deleting{TSObject}"/>, by the entity's id alone). The engine runs
/// only what it is given.
/// </param>
/// <param name="Queries">The batch's units -> the reads (at most 5 after chunking) the diff and the foreign keys need.</param>
public sealed record SyncStrategy<TEvent>(
    EventReader<TEvent> Reader,
    Func<string, TEvent, SyncUnit> Map,
    Func<IReadOnlyCollection<SyncUnit>, IEnumerable<SoqlQuery>> Queries)
{
    /// <summary>The strategy's name, e.g. "account": how logs name it.</summary>
    public string EntityType => Reader.EntityType;

    /// <inheritdoc cref="EventReader{TEvent}.TryRead"/>
    public bool TryRead(string json, [NotNullWhen(true)] out ReadEvent<TEvent>? read, [NotNullWhen(false)] out string? problem)
    {
        return Reader.TryRead(json, out read, out problem);
    }
}

/// <summary>Records of one sObject to read from Salesforce, by a field (normally a unique external id).</summary>
public sealed record SoqlQuery(string SObject, string WhereField, IReadOnlyCollection<string> Values)
{
    /// <summary>Reads the units' records of <typeparamref name="TSObject"/> by external id (to diff them).</summary>
    public static SoqlQuery RecordsOf<TSObject>(IEnumerable<SyncUnit> units) where TSObject : class
    {
        var meta = SObjectMetadata<TSObject>.Instance;
        return new SoqlQuery(meta.SObject, meta.ExternalIdField, ExternalIdsOf(units, meta.SObject));
    }

    /// <summary>
    /// Reads the <typeparamref name="TSObject"/> records the units' foreign keys point at, to know that a parent exists
    /// before its child is sent.
    /// </summary>
    public static SoqlQuery ParentsOf<TSObject>(IEnumerable<SyncUnit> units) where TSObject : class
    {
        var meta = SObjectMetadata<TSObject>.Instance;
        return new SoqlQuery(meta.SObject, meta.ExternalIdField, ParentIdsOf(units, meta.SObject));
    }

    /// <summary>The external ids of every record of <paramref name="sobject"/> in the units.</summary>
    public static IReadOnlyCollection<string> ExternalIdsOf(IEnumerable<SyncUnit> units, string sobject)
    {
        return [.. units.SelectMany(u => u.Records)
                .Where(r => r.SObject.Equals(sobject, StringComparison.OrdinalIgnoreCase))
                .Select(r => r.ExternalId)
                .Distinct(StringComparer.Ordinal)];
    }

    /// <summary>The external ids every foreign key of the units points at in <paramref name="sobject"/>.</summary>
    public static IReadOnlyCollection<string> ParentIdsOf(IEnumerable<SyncUnit> units, string sobject)
    {
        return [.. units.SelectMany(u => u.Records)
                .SelectMany(r => r.Parents)
                .Where(l => l.SObject.Equals(sobject, StringComparison.OrdinalIgnoreCase))
                .Select(l => l.ExternalId)
                .Distinct(StringComparer.Ordinal)];
    }
}
