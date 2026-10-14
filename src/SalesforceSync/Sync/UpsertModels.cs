using System.Text.Json;
using SalesforceSync.Salesforce;

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
    IReadOnlyList<SObjectLookup>? Lookups = null);

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
/// to diff them and resolve their lookups. Everything else (claiming, ordering, coalescing, batching,
/// retries, dead-lettering) is <see cref="SalesforceBatchUpsertService"/>'s.
/// </summary>
/// <typeparam name="TEvent">The inbox payload (the whole event envelope), deserialized after the entity's pending events are merged.</typeparam>
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

/// <summary>Event JSON field maps to the CLR values <see cref="FieldValues"/> normalizes.</summary>
public static class SObjectFields
{
    public static IReadOnlyDictionary<string, object?> From(IReadOnlyDictionary<string, JsonElement>? json)
    {
        var fields = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        if (json is null) return fields;
        foreach (var (name, value) in json)
            fields[name] = value.ValueKind switch
            {
                JsonValueKind.Null or JsonValueKind.Undefined => null,
                JsonValueKind.String => value.GetString(),
                JsonValueKind.Number => value.GetDecimal(),
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                _ => value.GetRawText()
            };
        return fields;
    }
}
