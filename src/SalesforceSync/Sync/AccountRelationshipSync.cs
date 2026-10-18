using System.Text.Json.Serialization;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.Sync;

/// <summary>
/// Account_Relationship__c between two Accounts, named by their external ids (Text foreign keys). If either Account
/// isn't in Salesforce yet, nothing is sent and the relationship is retried until the account sync has created it.
/// Like <see cref="AccountSync"/>, everything particular to relationships is in this file.
/// </summary>
public static class AccountRelationshipSync
{
    public const string Type = "accountRelationship";

    /// <summary>The strategy the engine runs.</summary>
    public static readonly SyncStrategy<AccountRelationshipEvent> Strategy =
        new(Type, e => e.AccountRelationship?.ExternalId, e => e.Meta, Map, Queries);

    /// <summary>
    /// A deleted event deletes the relationship, by its key alone; any other upserts it, which needs both Accounts' keys.
    /// </summary>
    public static SyncUnit Map(string relationshipKey, AccountRelationshipEvent e)
    {
        if (e.Meta?.IsDelete() == true)
        {
            return new SyncUnit([SObjectRecord.Deleting<AccountRelationshipSObject>("rel", relationshipKey)]);
        }

        var payload = e.AccountRelationship;
        return string.IsNullOrWhiteSpace(payload?.AccountExternalId) || string.IsNullOrWhiteSpace(payload.RelatedAccountExternalId)
            ? throw new ArgumentException("accountExternalId and relatedAccountExternalId are required")
            : new SyncUnit([SObjectRecord.From("rel", ToRelationship(payload))]);
    }

    // What to read: the relationships themselves (to diff) and the Accounts they point at (to know they exist)
    public static IEnumerable<SoqlQuery> Queries(IReadOnlyCollection<SyncUnit> units)
    {
        return [SoqlQuery.RecordsOf<AccountRelationshipSObject>(units), SoqlQuery.ParentsOf<AccountSObject>(units)];
    }

    /// <summary>Payload -> sObject, one line per field (see <see cref="AccountSync.ToAccount"/>).</summary>
    public static AccountRelationshipSObject ToRelationship(AccountRelationshipPayload payload)
    {
        return new()
        {
            ExternalId = payload.ExternalId,
            Name = payload.Name,
            AccountId = payload.AccountExternalId,
            RelatedAccountId = payload.RelatedAccountExternalId,
            Type = payload.Type,
            StartDate = payload.StartDate,
            Active = payload.IsActive
        };
    }
}

/// <summary>
/// Inbox event for an account-to-account relationship (entity type "accountRelationship", key =
/// accountRelationship.externalId), as <see cref="AccountAggregate"/> splits it out of its first Account's aggregate:
/// the relationship as the source system sends it (camelCase JSON, full record), or a delete of it (its key only).
/// <code>{ "meta": { "time": "2026-10-14T01:02:03.456Z", "type": "updated" },
///   "accountRelationship": { "externalId": "REL-1", "accountExternalId": "ACC-1", "relatedAccountExternalId": "ACC-2",
///                            "type": "Parent", "isActive": true } }
/// { "meta": { "time": "2026-10-15T01:02:03.456Z", "type": "deleted" }, "accountRelationship": { "externalId": "REL-1" } }</code>
/// </summary>
public sealed record AccountRelationshipEvent(EventMeta? Meta, AccountRelationshipPayload? AccountRelationship);

/// <summary>The <c>accountRelationship</c> object of an <see cref="AccountRelationshipEvent"/>.</summary>
public sealed class AccountRelationshipPayload
{
    public string? ExternalId { get; set; }

    /// <summary>External ids of the two accounts. Both are required; an event without them is dead-lettered (INVALID_EVENT).</summary>
    public string? AccountExternalId { get; set; }
    public string? RelatedAccountExternalId { get; set; }

    public string? Name { get; set; }
    public string? Type { get; set; }
    public DateOnly? StartDate { get; set; }
    public bool? IsActive { get; set; }
}

/// <summary>Account_Relationship__c between two Accounts (complete desired state, like <see cref="AccountSObject"/>).</summary>
[SObject("Account_Relationship__c", ExternalIdField = "ExternalId__c", RecordType = "Standard")]
public sealed class AccountRelationshipSObject
{
    [JsonPropertyName("ExternalId__c")] public string? ExternalId { get; set; }
    [JsonPropertyName("Name")] public string? Name { get; set; }

    /// <summary>The two Accounts, by their external ids (Text fields).</summary>
    [JsonPropertyName("AccountId__c"), SObjectForeignKey(typeof(AccountSObject))]
    public string? AccountId { get; set; }

    [JsonPropertyName("RelatedAccountId__c"), SObjectForeignKey(typeof(AccountSObject))]
    public string? RelatedAccountId { get; set; }

    [JsonPropertyName("Type__c")] public string? Type { get; set; }
    [JsonPropertyName("Start_Date__c")] public DateOnly? StartDate { get; set; }
    [JsonPropertyName("Active__c")] public bool? Active { get; set; }
}
