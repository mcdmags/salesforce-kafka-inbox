using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync;

/// <summary>
/// The records of one entity, upserted or deleted in the same request. Each record is saved or rejected on its own
/// (they are not atomic); the entity counts as synced only when all of them are. A record must come after the records
/// it looks up (by a lookup or a foreign key).
/// </summary>
public sealed record SyncUnit(IReadOnlyList<SObjectRecord> Records);

/// <summary>A Salesforce record to create or update, or to delete, identified by an external id.</summary>
/// <param name="ReferenceId">Unique within its unit; other records of the unit can point at it before it exists.</param>
/// <param name="Fields">Desired values by API name. Fields not listed are left alone.</param>
/// <param name="Lookups">Reference fields to set to another record's Id.</param>
/// <param name="RecordType">
/// Developer name of the record's record type: RecordTypeId is set to its Id (from Describe) like any other field.
/// Records of sObject classes always have one; null leaves RecordTypeId alone.
/// </param>
/// <param name="ForeignKeys">
/// Text fields of <paramref name="Fields"/> that hold a parent's external id: written as they are, but the parent goes
/// first and must exist (<see cref="SObjectForeignKeyAttribute"/>).
/// </param>
/// <param name="SObject">The object's API name, e.g. Account.</param>
/// <param name="ExternalIdField">The external id field the record is matched by.</param>
/// <param name="ExternalId">Its value: the record's identity in this sync.</param>
/// <param name="Delete">
/// Delete the record rather than upsert it (<see cref="Deleting{TSObject}"/>): only its object and external id count.
/// </param>
public sealed record SObjectRecord(
    string ReferenceId,
    string SObject,
    string ExternalIdField,
    string ExternalId,
    IReadOnlyDictionary<string, object?> Fields,
    IReadOnlyList<SObjectLookup>? Lookups = null,
    string? RecordType = null,
    IReadOnlyList<SObjectLookup>? ForeignKeys = null,
    bool Delete = false)
{
    /// <summary>
    /// The record for an sObject instance: the object, external id field, record type, field names, lookups and
    /// foreign keys come from the class's attributes (<see cref="SObjectMetadata{TSObject}"/>, cached per class).
    /// Every field is included, nulls too: the instance is the complete desired state, so a null field is cleared if
    /// Salesforce has a value.
    /// </summary>
    /// <exception cref="ArgumentException">The external id or a lookup's target id is missing.</exception>
    public static SObjectRecord From<TSObject>(string referenceId, TSObject sobject) where TSObject : class
    {
        var meta = SObjectMetadata<TSObject>.Instance;
        var externalId = meta.ExternalIdOf(sobject);
        if (string.IsNullOrWhiteSpace(externalId))
        {
            throw new ArgumentException($"{meta.SObject}.{meta.ExternalIdField} is required");
        }

        List<SObjectLookup>? lookups = null;
        foreach (var (lookup, targetId) in meta.LookupsOf(sobject))
        {
            if (string.IsNullOrWhiteSpace(targetId))
            {
                throw new ArgumentException($"{meta.SObject}.{lookup.Field} needs the external id of the {lookup.TargetSObject} it points at");
            }

            (lookups ??= []).Add(new SObjectLookup(lookup.Field, lookup.TargetSObject, lookup.TargetExternalIdField, targetId));
        }

        // An empty foreign key is an empty field like any other: no parent to wait for
        List<SObjectLookup>? foreignKeys = null;
        foreach (var (key, parentId) in meta.ForeignKeysOf(sobject))
        {
            if (!string.IsNullOrWhiteSpace(parentId))
            {
                (foreignKeys ??= []).Add(new SObjectLookup(key.Field, key.TargetSObject, key.TargetExternalIdField, parentId));
            }
        }
        return new SObjectRecord(referenceId, meta.SObject, meta.ExternalIdField, externalId, meta.FieldsOf(sobject), lookups,
            meta.RecordType, foreignKeys);
    }

    /// <summary>
    /// A delete of the <typeparamref name="TSObject"/> record with this external id. The id is all a delete needs: the
    /// read finds the record's Salesforce Id by it, and a record the read doesn't find is deleted already.
    /// </summary>
    /// <exception cref="ArgumentException">The external id is missing.</exception>
    public static SObjectRecord Deleting<TSObject>(string referenceId, string externalId) where TSObject : class
    {
        var meta = SObjectMetadata<TSObject>.Instance;
        return string.IsNullOrWhiteSpace(externalId)
            ? throw new ArgumentException($"{meta.SObject}.{meta.ExternalIdField} is required")
            : new SObjectRecord(referenceId, meta.SObject, meta.ExternalIdField, externalId, new Dictionary<string, object?>(),
                Delete: true);
    }

    /// <summary>The records this one points at: by its lookups, then by its foreign keys.</summary>
    public IEnumerable<SObjectLookup> Parents => (Lookups ?? []).Concat(ForeignKeys ?? []);
}

/// <summary>
/// The record of <paramref name="SObject"/> whose <paramref name="ExternalIdField"/> is <paramref name="ExternalId"/>,
/// which <paramref name="Field"/> points at: an earlier record of the same unit (written before this one) or a record
/// already in Salesforce (read by the strategy's queries). If neither exists, the unit is retried later
/// (<see cref="SalesforceErrors.DependencyNotSynced"/>). As a lookup, the field is set to that record's Id (or to a
/// reference to its create in the same request); as a foreign key, the field holds <paramref name="ExternalId"/> itself.
/// </summary>
public sealed record SObjectLookup(string Field, string SObject, string ExternalIdField, string ExternalId);

/// <summary>The outcome for one entity (one unit).</summary>
public sealed record SyncResult(string Key, SyncOutcome Outcome, IReadOnlyList<SalesforceError> Errors)
{
    /// <summary>The error that decided the outcome (errors are ordered most significant first).</summary>
    public SalesforceError? PrimaryError =>
        Errors.FirstOrDefault(e => e.Code != SalesforceErrors.ProcessingHalted) ?? Errors.FirstOrDefault();
}

public enum SyncOutcome
{
    /// <summary>Salesforce already matches the inbox state (a record to delete isn't there); nothing was written.</summary>
    Unchanged,

    /// <summary>The differing fields were written, and the records to delete deleted.</summary>
    Saved,

    /// <summary>Not saved because of another entity's record (skipped with it): retry now, no attempt spent.</summary>
    NotAttempted,

    /// <summary>May succeed later (lock contention, outage, a lookup target not synced yet): retry with backoff.</summary>
    TransientFailure,

    /// <summary>Retrying the same data fails the same way (validation, unknown or read-only field): dead-letter now.</summary>
    PermanentFailure
}