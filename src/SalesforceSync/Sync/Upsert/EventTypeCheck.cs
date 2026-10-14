using SalesforceSync.Inbox.Payloads;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync.Upsert;

internal static class EventTypeCheck
{
    /// <summary>
    /// The event's type against what Salesforce has for the unit's main record: "inserted" for a record that exists
    /// (a replay, or it was created another way) or "updated" for one that doesn't (its insert was lost or
    /// dead-lettered, or the record was deleted in Salesforce).
    /// </summary>
    /// <returns>The mismatch, or null if the type agrees with Salesforce (or the unit has no type).</returns>
    public static SalesforceError? Mismatch(NormalizedUnit unit, CurrentRecords current)
    {
        if (unit.EventType is not { } type)
        {
            return null;
        }

        var main = unit.MainRecord;
        var exists = current.Find(main) is not null;
        return exists == (type == EventType.Updated)
            ? null
            : new SalesforceError(main.ReferenceId, SalesforceErrors.EventTypeMismatch, exists
            ? $"the event says inserted, but {main.SObject} {main.ExternalIdField}={main.ExternalId} is already in Salesforce"
            : $"the event says updated, but {main.SObject} {main.ExternalIdField}={main.ExternalId} is not in Salesforce");
    }
}