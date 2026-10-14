using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync.Upsert;

/// <summary>A unit whose values are in the form Salesforce stores them, ready to be compared and sent.</summary>
internal sealed record NormalizedUnit(string Key, IReadOnlyList<NormalizedRecord> Records);

/// <summary>A record with its object's Describe metadata and its values normalized per field.</summary>
internal sealed record NormalizedRecord(
    SObjectRecord Record,
    SObjectSchema Schema,
    IReadOnlyDictionary<SalesforceField, object?> Fields,
    IReadOnlyList<NormalizedLookup> Lookups)
{
    public string ReferenceId => Record.ReferenceId;
}

/// <summary>A lookup of a record: the reference field to set and the record it points at.</summary>
internal sealed record NormalizedLookup(SalesforceField Field, SObjectLookup Target);