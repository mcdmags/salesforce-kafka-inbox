using SalesforceSync.Inbox.Payloads;

namespace SalesforceSync.Sync;

/// <summary>
/// The records of one entity, written in the same request. Each record is saved or rejected on its own (they are not
/// atomic); the entity counts as synced only when all of them are. The first record is the entity's main record; a
/// record must come after the records it looks up.
/// </summary>
/// <param name="EventType">What the event says happened to the main record (set by the engine), or null to go by Salesforce alone.</param>
public sealed record UpsertUnit(IReadOnlyList<SObjectRecord> Records, EventType? EventType = null);
