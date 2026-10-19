namespace SalesforceSync.Sync;

/// <summary>
/// Inbox event for an account-to-account relationship (entity type "accountRelationship", key =
/// accountRelationship.externalId), as <see cref="AccountAggregate"/> splits it out of its first Account's aggregate:
/// the relationship as the source system sends it (camelCase JSON, full record), or a delete (its key only).
/// <code>{ "meta": { "time": "2026-10-14T01:02:03.456Z", "type": "updated" },
///   "accountRelationship": { "externalId": "REL-1", "accountExternalId": "ACC-1", "relatedAccountExternalId": "ACC-2",
///                            "type": "Parent", "isActive": true } }
/// { "meta": { "time": "2026-10-15T01:02:03.456Z", "type": "deleted" }, "accountRelationship": { "externalId": "REL-1" } }</code>
/// </summary>
public sealed record AccountRelationshipEvent(EventMeta? Meta, AccountRelationshipPayload? AccountRelationship)
{
    public const string Type = "accountRelationship";

    /// <summary>How an inbox event of this type is read: the entity's id and the meta.</summary>
    public static readonly EventReader<AccountRelationshipEvent> Reader = new(Type, e => e.AccountRelationship?.ExternalId, e => e.Meta);
}

/// <summary>The <c>accountRelationship</c> object of an <see cref="AccountRelationshipEvent"/>.</summary>
public sealed class AccountRelationshipPayload
{
    public string? ExternalId { get; set; }

    /// <summary>External ids of the two accounts. Both required. An event without them is dead-lettered (INVALID_EVENT).</summary>
    public string? AccountExternalId { get; set; }
    public string? RelatedAccountExternalId { get; set; }

    public string? Name { get; set; }
    public string? Type { get; set; }
    public DateOnly? StartDate { get; set; }
    public bool? IsActive { get; set; }
}
