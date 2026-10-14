namespace SalesforceSync.Sync.Mapping;

/// <summary>
/// Inbox payload (the source system's shape and names) -> Salesforce sObject (the org's shape and names). Written by
/// hand, one line per field, so a renamed or retyped field is a compile error rather than a silent mismatch. Values
/// pass through as they are, nulls included (null = empty); a mapper converts only where the two sides differ in type.
/// </summary>
public interface ISObjectMapper<in TPayload, out TSObject>
{
    TSObject Map(TPayload payload);
}
