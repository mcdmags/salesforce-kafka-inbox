namespace SalesforceSync.Inbox.Payloads;

/// <summary>
/// Inbox event for an account-to-account relationship (entity type "accountRelationship", key =
/// accountRelationship.externalId), as the source system sends it (camelCase JSON, full record).
/// <code>{ "meta": { "time": "2026-10-14T01:02:03.456Z" },
///   "accountRelationship": { "externalId": "REL-1", "accountExternalId": "ACC-1", "relatedAccountExternalId": "ACC-2",
///                            "type": "Parent", "isActive": true } }</code>
/// </summary>
public sealed record AccountRelationshipEvent(AccountRelationshipPayload? AccountRelationship);

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
