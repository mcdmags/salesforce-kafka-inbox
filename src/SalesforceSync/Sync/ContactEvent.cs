namespace SalesforceSync.Sync;

/// <summary>
/// Inbox event for a contact (entity type "contact", key = contact.externalId), split by <see cref="AccountAggregate"/>
/// from its account's aggregate: contact as source sends it (camelCase JSON, full record), named by its Account, or
/// delete (key only).
/// <code>{ "meta": { "time": "2026-10-14T01:02:03.456Z", "type": "inserted" },
///   "contact": { "externalId": "CON-1", "accountExternalId": "ACC-1", "firstName": "Ada", "lastName": "Lovelace",
///                "email": "ada@example.com", "phone": "+1 555 0101" } }
/// { "meta": { "time": "2026-10-15T01:02:03.456Z", "type": "deleted" }, "contact": { "externalId": "CON-1" } }</code>
/// </summary>
public sealed record ContactEvent(EventMeta? Meta, ContactPayload? Contact)
{
    public const string Type = "contact";

    public static readonly EventReader<ContactEvent> Reader = new(Type, e => e.Contact?.ExternalId, e => e.Meta);
}

/// <summary>The <c>contact</c> object of a <see cref="ContactEvent"/>.</summary>
public sealed class ContactPayload
{
    public string? ExternalId { get; set; }

    /// <summary>External id of contact's account. Required; missing → dead-lettered (INVALID_EVENT).</summary>
    public string? AccountExternalId { get; set; }

    public string? FirstName { get; set; }

    /// <summary>Required in Salesforce; missing → refused (REQUIRED_FIELD_MISSING), dead-lettered.</summary>
    public string? LastName { get; set; }

    public string? Email { get; set; }
    public string? Phone { get; set; }
}
