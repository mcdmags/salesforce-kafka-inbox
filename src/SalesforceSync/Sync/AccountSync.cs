using System.Text.Json.Serialization;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.Sync;

/// <summary>
/// Account + its Profile__c, written in the same batch (Accounts first). The Profile names its Account by the Account's
/// external id (a Text foreign key).
/// Typed event -> mapping -> sObjects -> records. An event without a <c>profile</c> writes only the Account.
/// Everything particular to accounts is in this file: the event as the source sends it, the sObjects as the org
/// has them, the mapping between the two, and the inbox row type.
/// </summary>
public sealed class AccountSync
{
    public const string Type = AccountEvent.Type;

    /// <summary>The strategy the engine runs.</summary>
    public SyncStrategy<AccountEvent> Strategy { get; }

    public AccountSync()
    {
        Strategy = new(AccountEvent.Reader, Map, Queries);
    }

    /// <summary>
    /// A deleted event deletes the Profile, then the Account (children first): an org whose automation refuses to
    /// delete an Account that records still name accepts that order. Salesforce itself refuses nothing for Text
    /// keys; the contacts' and relationships' own deletes come through their inboxes.
    /// Any other event upserts the Account and, if the event has one, its Profile.
    /// </summary>
    public SyncUnit Map(string accountKey, AccountEvent e)
    {
        if (e.Meta?.IsDelete() == true)
        {
            return new SyncUnit([SObjectRecord.Deleting<ProfileSObject>("prof", e.Account?.Profile?.ExternalId ?? accountKey),
                SObjectRecord.Deleting<AccountSObject>("acct", accountKey)]);
        }

        var payload = e.Account ?? throw new ArgumentException("account is required");
        var records = new List<SObjectRecord> { SObjectRecord.From("acct", ToAccount(payload)) };

        if (payload.Profile is not null)
        {
            var profile = ToProfile(payload.Profile);
            // What the profile takes from its account: its id (unless it has its own) and the Account it belongs to
            profile.ExternalId ??= accountKey;
            profile.AccountId = accountKey;
            records.Add(SObjectRecord.From("prof", profile));
        }
        return new SyncUnit(records);
    }

    public IEnumerable<SoqlQuery> Queries(IReadOnlyCollection<SyncUnit> units)
    {
        return [SoqlQuery.RecordsOf<AccountSObject>(units), SoqlQuery.RecordsOf<ProfileSObject>(units)];
    }

    /// <summary>
    /// Inbox payload (the source system's shape and names) -> Salesforce sObject (the org's shape and names). Written by
    /// hand, one line per field, so a renamed or retyped field is a compile error, not a silent mismatch. Values
    /// pass through as they are, nulls included (null = empty). The mapping converts only where the two sides differ in type.
    /// </summary>
    public AccountSObject ToAccount(AccountPayload payload)
    {
        return new()
        {
            ExternalId = payload.ExternalId,
            Name = payload.Name,
            Rating = payload.Rating,
            Industry = payload.Industry,
            Description = payload.Description,
            AnnualRevenue = payload.AnnualRevenue,
            NumberOfEmployees = payload.NumberOfEmployees,
            Phone = payload.Phone,
            Website = payload.Website,
            Active = payload.IsActive,
            // A list in the event, a ';'-separated multi-select picklist in Salesforce
            Tags = payload.Tags is null ? null : string.Join(';', payload.Tags),
            CustomerSince = payload.CustomerSince,
            LastContacted = payload.LastContactedAt
        };
    }

    /// <summary>
    /// The profile on its own. <see cref="Map"/>, which has both, fills in what it takes from its account (the default
    /// external id and the Account's key).
    /// </summary>
    public ProfileSObject ToProfile(ProfilePayload payload)
    {
        return new()
        {
            ExternalId = payload.ExternalId,
            Name = payload.Name,
            Tier = payload.Tier,
            Score = payload.Score,
            Discount = payload.DiscountPercent,
            Notes = payload.Notes,
            OptIn = payload.OptIn,
            StartDate = payload.StartDate
        };
    }
}

/// <summary>
/// Inbox event for an account and its profile (entity type "account", key = account.externalId): the account part of
/// its aggregate (<see cref="AccountAggregate"/>) as the source system sends it, or a delete of the Account (its key,
/// and its Profile's if that has its own). The JSON is camelCase. Nothing here is a Salesforce name.
/// <code>{ "meta": { "time": "2026-10-14T01:02:03.456Z", "type": "updated" },
///   "account": { "externalId": "ACC-1", "name": "Acme", "rating": "Hot", "isActive": true,
///                "profile": { "tier": "Gold", "score": 87.5 } } }
/// { "meta": { "time": "2026-10-15T01:02:03.456Z", "type": "deleted" }, "account": { "externalId": "ACC-1" } }</code>
/// An event always carries the full record: a null or absent property means "empty in the source", and the
/// field is cleared in Salesforce. The one exception is <see cref="AccountPayload.Profile"/> as a whole.
/// </summary>
public sealed record AccountEvent(EventMeta? Meta, AccountPayload? Account)
{
    public const string Type = "account";

    /// <summary>How an inbox event of this type is read: the entity's id and the meta.</summary>
    public static readonly EventReader<AccountEvent> Reader = new(Type, e => e.Account?.ExternalId, e => e.Meta);
}

/// <summary>The <c>account</c> object of an <see cref="AccountEvent"/>.</summary>
public class AccountPayload
{
    public string? ExternalId { get; set; }
    public string? Name { get; set; }
    public string? Rating { get; set; }
    public string? Industry { get; set; }
    public string? Description { get; set; }
    public decimal? AnnualRevenue { get; set; }
    public int? NumberOfEmployees { get; set; }
    public string? Phone { get; set; }
    public string? Website { get; set; }
    public bool? IsActive { get; set; }
    public IReadOnlyList<string>? Tags { get; set; }
    public DateOnly? CustomerSince { get; set; }
    public DateTimeOffset? LastContactedAt { get; set; }

    /// <summary>Optional. Without it only the Account is written; the Profile in Salesforce is left as it is.</summary>
    public ProfilePayload? Profile { get; set; }
}

/// <summary>The optional <c>account.profile</c> object of an <see cref="AccountEvent"/>.</summary>
public sealed class ProfilePayload
{
    /// <summary>Defaults to the account's externalId.</summary>
    public string? ExternalId { get; set; }

    public string? Name { get; set; }
    public string? Tier { get; set; }
    public decimal? Score { get; set; }
    public decimal? DiscountPercent { get; set; }
    public string? Notes { get; set; }
    public bool? OptIn { get; set; }
    public DateOnly? StartDate { get; set; }
}

/// <summary>
/// The Salesforce Account as this sync writes it. An instance is the record's complete desired state: a null
/// property means "empty", and the diff clears the field if Salesforce has a value.
/// </summary>
[SObject("Account", ExternalIdField = "ExternalId__c", RecordType = "Business")]
public sealed class AccountSObject
{
    [JsonPropertyName("ExternalId__c")] public string? ExternalId { get; set; }
    [JsonPropertyName("Name")] public string? Name { get; set; }
    [JsonPropertyName("Rating")] public string? Rating { get; set; }
    [JsonPropertyName("Industry")] public string? Industry { get; set; }
    [JsonPropertyName("Description")] public string? Description { get; set; }
    [JsonPropertyName("AnnualRevenue")] public decimal? AnnualRevenue { get; set; }
    [JsonPropertyName("NumberOfEmployees")] public int? NumberOfEmployees { get; set; }
    [JsonPropertyName("Phone")] public string? Phone { get; set; }
    [JsonPropertyName("Website")] public string? Website { get; set; }
    [JsonPropertyName("Active__c")] public bool? Active { get; set; }

    /// <summary>Multi-select picklist: the selected values separated by ';'.</summary>
    [JsonPropertyName("Tags__c")] public string? Tags { get; set; }

    [JsonPropertyName("Customer_Since__c")] public DateOnly? CustomerSince { get; set; }
    [JsonPropertyName("Last_Contacted__c")] public DateTimeOffset? LastContacted { get; set; }
}

/// <summary>Profile__c, the detail record of an Account (complete desired state, like <see cref="AccountSObject"/>).</summary>
[SObject("Profile__c", ExternalIdField = "ExternalId__c", RecordType = "Standard")]
public sealed class ProfileSObject
{
    [JsonPropertyName("ExternalId__c")] public string? ExternalId { get; set; }
    [JsonPropertyName("Name")] public string? Name { get; set; }

    /// <summary>
    /// The Account this Profile belongs to: a Text field holding the Account's external id. The Account is in the same
    /// unit, so its write goes first and no query checks it.
    /// </summary>
    [JsonPropertyName("AccountId__c"), SObjectForeignKey(typeof(AccountSObject))]
    public string? AccountId { get; set; }

    [JsonPropertyName("Tier__c")] public string? Tier { get; set; }
    [JsonPropertyName("Score__c")] public decimal? Score { get; set; }
    [JsonPropertyName("Discount__c")] public decimal? Discount { get; set; }
    [JsonPropertyName("Notes__c")] public string? Notes { get; set; }
    [JsonPropertyName("Opt_In__c")] public bool? OptIn { get; set; }
    [JsonPropertyName("Start_Date__c")] public DateOnly? StartDate { get; set; }
}
