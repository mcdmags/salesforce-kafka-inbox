namespace SalesforceSync.Sync;

/// <summary>
/// Inbox event for account-to-account relationship (entity type "accountRelationship", key =
/// accountRelationship.externalId), split by <see cref="AccountAggregate"/> from its first Account's aggregate:
/// relationship as source sends it (camelCase JSON, full record), or delete (key only).
/// <code>{ "meta": { "time": "2026-10-14T01:02:03.456Z", "type": "updated" },
///   "accountRelationship": { "externalId": "REL-1", "accountExternalId": "ACC-1", "relatedAccountExternalId": "ACC-2",
///                            "type": "Parent", "isActive": true } }
/// { "meta": { "time": "2026-10-15T01:02:03.456Z", "type": "deleted" }, "accountRelationship": { "externalId": "REL-1" } }</code>
/// </summary>
public sealed record AccountRelationshipEvent(EventMeta? Meta, AccountRelationshipPayload? AccountRelationship)
{
    public const string Type = "accountRelationship";

    public static readonly EventReader<AccountRelationshipEvent> Reader = new(Type, e => e.AccountRelationship?.ExternalId, e => e.Meta);
}

/// <summary>The <c>accountRelationship</c> object of an <see cref="AccountRelationshipEvent"/>.</summary>
public sealed class AccountRelationshipPayload
{
    public string? ExternalId { get; set; }

    /// <summary>External ids of the two accounts. Both required; missing → dead-lettered (INVALID_EVENT).</summary>
    public string? AccountExternalId { get; set; }
    public string? RelatedAccountExternalId { get; set; }

    public string? Name { get; set; }
    public string? Type { get; set; }
    public DateOnly? StartDate { get; set; }
    public bool? IsActive { get; set; }
}
