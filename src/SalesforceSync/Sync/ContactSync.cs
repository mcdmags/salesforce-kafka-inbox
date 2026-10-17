using System.Text.Json.Serialization;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.Sync;

/// <summary>
/// Contact of an Account, named by the Account's external id (a Text foreign key). If the Account isn't in Salesforce
/// yet, nothing is sent and the contact is retried until the account sync has created it. Like
/// <see cref="AccountSync"/>, everything particular to contacts is in this file.
/// </summary>
public static class ContactSync
{
    public const string Type = "contact";

    /// <summary>The strategy the engine runs.</summary>
    public static readonly SyncStrategy<ContactEvent> Strategy = new(Type, e => e.Contact?.ExternalId, e => e.Meta, Map, Queries);

    /// <summary>
    /// A deleted event deletes the contact, by its key alone; any other upserts it, which needs its Account's key.
    /// </summary>
    public static SyncUnit Map(string contactKey, ContactEvent e)
    {
        if (e.Meta?.IsDelete() == true)
        {
            return new SyncUnit([SObjectRecord.Deleting<ContactSObject>("contact", contactKey)]);
        }

        var payload = e.Contact;
        return string.IsNullOrWhiteSpace(payload?.AccountExternalId)
            ? throw new ArgumentException("accountExternalId is required")
            : new SyncUnit([SObjectRecord.From("contact", ToContact(payload))]);
    }

    // What to read: the contacts themselves (to diff) and the Accounts they point at (to know they exist)
    public static IEnumerable<SoqlQuery> Queries(IReadOnlyCollection<SyncUnit> units)
    {
        return [SoqlQuery.RecordsOf<ContactSObject>(units), SoqlQuery.LookupTargetsOf<AccountSObject>(units)];
    }

    /// <summary>Payload -> sObject, one line per field (see <see cref="AccountSync.ToAccount"/>).</summary>
    public static ContactSObject ToContact(ContactPayload payload)
    {
        return new()
        {
            ExternalId = payload.ExternalId,
            AccountId = payload.AccountExternalId,
            FirstName = payload.FirstName,
            LastName = payload.LastName,
            Email = payload.Email,
            Phone = payload.Phone
        };
    }
}

/// <summary>
/// Inbox event for a contact (entity type "contact", key = contact.externalId), as <see cref="AccountAggregate"/> splits
/// it out of its account's aggregate: the contact as the source system sends it (camelCase JSON, full record), named
/// by its Account, or a delete of it, which carries its key only.
/// <code>{ "meta": { "time": "2026-10-14T01:02:03.456Z", "type": "inserted" },
///   "contact": { "externalId": "CON-1", "accountExternalId": "ACC-1", "firstName": "Ada", "lastName": "Lovelace",
///                "email": "ada@example.com", "phone": "+1 555 0101" } }
/// { "meta": { "time": "2026-10-15T01:02:03.456Z", "type": "deleted" }, "contact": { "externalId": "CON-1" } }</code>
/// </summary>
public sealed record ContactEvent(EventMeta? Meta, ContactPayload? Contact);

/// <summary>The <c>contact</c> object of a <see cref="ContactEvent"/>.</summary>
public sealed class ContactPayload
{
    public string? ExternalId { get; set; }

    /// <summary>External id of the contact's account. Required; an event without it is dead-lettered (INVALID_EVENT).</summary>
    public string? AccountExternalId { get; set; }

    public string? FirstName { get; set; }

    /// <summary>Required in Salesforce: an event without it is dead-lettered (REQUIRED_FIELD_MISSING) before any call.</summary>
    public string? LastName { get; set; }

    public string? Email { get; set; }
    public string? Phone { get; set; }
}

/// <summary>The Salesforce Contact as this sync writes it (complete desired state, like <see cref="AccountSObject"/>).</summary>
[SObject("Contact", ExternalIdField = "ExternalId__c", RecordType = "Standard")]
public sealed class ContactSObject
{
    [JsonPropertyName("ExternalId__c")] public string? ExternalId { get; set; }

    /// <summary>The Account, by its external id (a Text field; the standard AccountId lookup is left alone).</summary>
    [JsonPropertyName("AccountId__c"), SObjectForeignKey(typeof(AccountSObject))]
    public string? AccountId { get; set; }

    [JsonPropertyName("FirstName")] public string? FirstName { get; set; }
    [JsonPropertyName("LastName")] public string? LastName { get; set; }
    [JsonPropertyName("Email")] public string? Email { get; set; }
    [JsonPropertyName("Phone")] public string? Phone { get; set; }
}
