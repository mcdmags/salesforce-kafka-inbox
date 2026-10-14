using SalesforceSync.Salesforce;
using SalesforceSync.Salesforce.SObjects;

namespace SalesforceSync.Sync;

/// <summary>A Salesforce record to create or update, identified by an external id.</summary>
/// <param name="ReferenceId">Unique within its unit; other records of the unit can point at it before it exists.</param>
/// <param name="Fields">Desired values by API name. Fields not listed are left alone.</param>
/// <param name="Lookups">Reference fields to set to another record's Id.</param>
public sealed record SObjectRecord(
    string ReferenceId,
    string SObject,
    string ExternalIdField,
    string ExternalId,
    IReadOnlyDictionary<string, object?> Fields,
    IReadOnlyList<SObjectLookup>? Lookups = null)
{
    /// <summary>
    /// The record for an sObject instance: the object, external id field, field names and lookups come from the
    /// class's attributes (<see cref="SObjectMetadata{TSObject}"/>, cached per class). Every field is included, nulls
    /// too: the instance is the complete desired state, so a null field is cleared if Salesforce has a value.
    /// </summary>
    /// <exception cref="ArgumentException">The external id or a lookup's target id is missing.</exception>
    public static SObjectRecord From<TSObject>(string referenceId, TSObject sobject) where TSObject : class
    {
        var meta = SObjectMetadata<TSObject>.Instance;
        var externalId = meta.ExternalIdOf(sobject);
        if (string.IsNullOrWhiteSpace(externalId))
            throw new ArgumentException($"{meta.SObject}.{meta.ExternalIdField} is required");

        List<SObjectLookup>? lookups = null;
        foreach (var (lookup, targetId) in meta.LookupsOf(sobject))
        {
            if (string.IsNullOrWhiteSpace(targetId))
                throw new ArgumentException($"{meta.SObject}.{lookup.Field} needs the external id of the {lookup.TargetSObject} it points at");
            (lookups ??= []).Add(new SObjectLookup(lookup.Field, lookup.TargetSObject, lookup.TargetExternalIdField, targetId));
        }
        return new SObjectRecord(referenceId, meta.SObject, meta.ExternalIdField, externalId, meta.FieldsOf(sobject), lookups);
    }
}

/// <summary>
/// Sets <paramref name="Field"/> to the Id of the record of <paramref name="SObject"/> whose
/// <paramref name="ExternalIdField"/> is <paramref name="ExternalId"/>: an earlier record of the same unit (created in
/// the same graph if new) or a record already in Salesforce (read by the strategy's queries). If neither exists, the
/// unit is retried later (<see cref="SalesforceErrors.DependencyNotSynced"/>).
/// </summary>
public sealed record SObjectLookup(string Field, string SObject, string ExternalIdField, string ExternalId);

/// <summary>Records written as one Composite Graph: all succeed or none. A record must come after the records it looks up.</summary>
public sealed record UpsertUnit(IReadOnlyList<SObjectRecord> Records);

/// <summary>Records of one sObject to read from Salesforce, by a field (normally a unique external id).</summary>
public sealed record SoqlQuery(string SObject, string WhereField, IReadOnlyCollection<string> Values)
{
    /// <summary>Reads the units' records of <typeparamref name="TSObject"/> by external id (to diff them).</summary>
    public static SoqlQuery RecordsOf<TSObject>(IEnumerable<UpsertUnit> units) where TSObject : class
    {
        var meta = SObjectMetadata<TSObject>.Instance;
        return new SoqlQuery(meta.SObject, meta.ExternalIdField, ExternalIdsOf(units, meta.SObject));
    }

    /// <summary>Reads the <typeparamref name="TSObject"/> records the units' lookups point at (for their Ids).</summary>
    public static SoqlQuery LookupTargetsOf<TSObject>(IEnumerable<UpsertUnit> units) where TSObject : class
    {
        var meta = SObjectMetadata<TSObject>.Instance;
        return new SoqlQuery(meta.SObject, meta.ExternalIdField, LookupIdsOf(units, meta.SObject));
    }

    /// <summary>The external ids of every record of <paramref name="sobject"/> in the units.</summary>
    public static IReadOnlyCollection<string> ExternalIdsOf(IEnumerable<UpsertUnit> units, string sobject)
        => units.SelectMany(u => u.Records)
            .Where(r => r.SObject.Equals(sobject, StringComparison.OrdinalIgnoreCase))
            .Select(r => r.ExternalId)
            .Distinct(StringComparer.Ordinal)
            .ToList();

    /// <summary>The external ids every lookup of the units points at in <paramref name="sobject"/>.</summary>
    public static IReadOnlyCollection<string> LookupIdsOf(IEnumerable<UpsertUnit> units, string sobject)
        => units.SelectMany(u => u.Records)
            .SelectMany(r => r.Lookups ?? [])
            .Where(l => l.SObject.Equals(sobject, StringComparison.OrdinalIgnoreCase))
            .Select(l => l.ExternalId)
            .Distinct(StringComparer.Ordinal)
            .ToList();
}

/// <summary>
/// What varies per synced object: how an inbox event becomes Salesforce records, and what to read from Salesforce
/// to diff them and resolve their lookups. Everything else (claiming, ordering, coalescing, rate limits, batching,
/// retries, dead-lettering) is <see cref="SalesforceBatchUpsertService"/>'s.
/// </summary>
/// <typeparam name="TEvent">The inbox payload (the whole event envelope) of the entity's newest pending event.</typeparam>
/// <param name="EntityType">The entity type this strategy syncs: the name of the event's entity object (sf_event_type).</param>
/// <param name="Map">Inbox event (with its entity key) -> the records to upsert together.</param>
/// <param name="Queries">The batch's units -> the reads (at most 5 after chunking) the diff and lookups need.</param>
public sealed record UpsertStrategy<TEvent>(
    string EntityType,
    Func<string, TEvent, UpsertUnit> Map,
    Func<IReadOnlyCollection<UpsertUnit>, IEnumerable<SoqlQuery>> Queries);

public enum SyncOutcome
{
    /// <summary>Salesforce already matches the inbox state; nothing was written.</summary>
    Unchanged,

    /// <summary>The differing fields were written.</summary>
    Saved,

    /// <summary>The graph never ran (halted or rolled back because of other graphs): retry now, no retry spent.</summary>
    NotAttempted,

    /// <summary>May succeed later (lock contention, outage, a lookup target not synced yet): retry with backoff.</summary>
    TransientFailure,

    /// <summary>Retrying the same data fails the same way (validation, unknown or read-only field): dead-letter now.</summary>
    PermanentFailure
}

/// <summary>The outcome for one entity (one unit).</summary>
public sealed record UpsertResult(string Key, SyncOutcome Outcome, IReadOnlyList<SalesforceError> Errors)
{
    /// <summary>The error that decided the outcome (errors are ordered most significant first).</summary>
    public SalesforceError? PrimaryError =>
        Errors.FirstOrDefault(e => !SalesforceErrors.NotAttemptedCodes.Contains(e.Code)) ?? Errors.FirstOrDefault();
}
