using System.Text.Json.Serialization;
using SalesforceSync.Inbox;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync;

/// <summary>
/// Account_Relationship__c between two Accounts, looked up by their external ids. If either Account isn't in
/// Salesforce yet, the relationship is retried until the account sync has created it. Like <see cref="AccountSync"/>,
/// everything particular to relationships is in this file.
/// </summary>
public static class AccountRelationshipSync
{
    public const string Type = "accountRelationship";
    public const string DefaultTopic = "account-relationship-changes";

    /// <summary>The strategy the engine runs (the relationship's key is in the event itself, so Map doesn't take it).</summary>
    public static readonly UpsertStrategy<AccountRelationshipEvent> Strategy =
        new(Type, e => e.AccountRelationship?.ExternalId, e => e.Meta, (_, e) => Map(e), Queries);

    public static UpsertUnit Map(AccountRelationshipEvent e)
    {
        var payload = e.AccountRelationship;
        return string.IsNullOrWhiteSpace(payload?.AccountExternalId) || string.IsNullOrWhiteSpace(payload.RelatedAccountExternalId)
            ? throw new ArgumentException("accountExternalId and relatedAccountExternalId are required")
            : new UpsertUnit([SObjectRecord.From("rel", ToRelationship(payload))]);
    }

    // What to read: the relationships themselves (to diff) and the Accounts they point at (for their Ids)
    public static IEnumerable<SoqlQuery> Queries(IReadOnlyCollection<UpsertUnit> units)
    {
        return [
        SoqlQuery.RecordsOf<AccountRelationshipSObject>(units),
        SoqlQuery.LookupTargetsOf<AccountSObject>(units)
    ];
    }

    /// <summary>Payload -> sObject, one line per field (see <see cref="AccountSync.ToAccount"/>).</summary>
    public static AccountRelationshipSObject ToRelationship(AccountRelationshipPayload payload)
    {
        return new()
        {
            ExternalId = payload.ExternalId,
            Name = payload.Name,
            AccountExternalId = payload.AccountExternalId,
            RelatedAccountExternalId = payload.RelatedAccountExternalId,
            Type = payload.Type,
            StartDate = payload.StartDate,
            Active = payload.IsActive
        };
    }
}

/// <summary>
/// Inbox event for an account-to-account relationship (entity type "accountRelationship", key =
/// accountRelationship.externalId), as the source system sends it (camelCase JSON, full record).
/// <code>{ "meta": { "time": "2026-10-14T01:02:03.456Z" },
///   "accountRelationship": { "externalId": "REL-1", "accountExternalId": "ACC-1", "relatedAccountExternalId": "ACC-2",
///                            "type": "Parent", "isActive": true } }</code>
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
[SObject("Account_Relationship__c", ExternalIdField = "External_Id__c", RecordType = "Standard")]
public sealed class AccountRelationshipSObject
{
    [JsonPropertyName("External_Id__c")] public string? ExternalId { get; set; }
    [JsonPropertyName("Name")] public string? Name { get; set; }

    [JsonPropertyName("Account__c"), SObjectLookup(typeof(AccountSObject))]
    public string? AccountExternalId { get; set; }

    [JsonPropertyName("Related_Account__c"), SObjectLookup(typeof(AccountSObject))]
    public string? RelatedAccountExternalId { get; set; }

    [JsonPropertyName("Type__c")] public string? Type { get; set; }
    [JsonPropertyName("Start_Date__c")] public DateOnly? StartDate { get; set; }
    [JsonPropertyName("Active__c")] public bool? Active { get; set; }
}

/// <summary>A row of <c>sf_account_relationship_inbox</c>: events of the account relationship type.</summary>
public sealed class AccountRelationshipInboxRow : InboxRow;