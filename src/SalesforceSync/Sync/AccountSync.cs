using System.Text.Json.Serialization;
using SalesforceSync.Inbox;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync;

/// <summary>
/// Account + its Profile__c, written in the same request (Accounts first); the Profile looks up its Account.
/// Typed event -> mapping -> sObjects -> records. An event without a <c>profile</c> writes only the Account.
/// Everything that is particular to accounts is in this file: the event as the source sends it, the sObjects as the org
/// has them, the mapping between the two, and the inbox row type.
/// </summary>
public static class AccountSync
{
    public const string Type = "account";
    public const string DefaultTopic = "account-changes";

    /// <summary>The strategy the engine runs.</summary>
    public static readonly UpsertStrategy<AccountEvent> Strategy = new(Type, e => e.Account?.ExternalId, e => e.Meta, Map, Queries);

    public static UpsertUnit Map(string accountKey, AccountEvent e)
    {
        var payload = e.Account ?? throw new ArgumentException("account is required");
        var records = new List<SObjectRecord> { SObjectRecord.From("acct", ToAccount(payload)) };

        if (payload.Profile is not null)
        {
            var profile = ToProfile(payload.Profile);
            // What the profile takes from its account: its id unless it has its own, and the Account it belongs to
            profile.ExternalId ??= accountKey;
            profile.AccountExternalId = accountKey;
            records.Add(SObjectRecord.From("prof", profile));
        }
        return new UpsertUnit(records);
    }

    public static IEnumerable<SoqlQuery> Queries(IReadOnlyCollection<UpsertUnit> units)
    {
        return [
        SoqlQuery.RecordsOf<AccountSObject>(units),
        SoqlQuery.RecordsOf<ProfileSObject>(units)
    ];
    }

    /// <summary>
    /// Inbox payload (the source system's shape and names) -> Salesforce sObject (the org's shape and names). Written by
    /// hand, one line per field, so a renamed or retyped field is a compile error rather than a silent mismatch. Values
    /// pass through as they are, nulls included (null = empty); the mapping converts only where the two sides differ in type.
    /// </summary>
    public static AccountSObject ToAccount(AccountPayload payload)
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
    /// The profile on its own. What it takes from its account (the default external id and the Account lookup) is filled
    /// in by <see cref="Map"/>, which has both.
    /// </summary>
    public static ProfileSObject ToProfile(ProfilePayload payload)
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
/// Inbox event for an account and its profile (entity type "account", key = account.externalId), as the source
/// system sends it. The JSON is camelCase; nothing here is a Salesforce name.
/// <code>{ "meta": { "time": "2026-10-14T01:02:03.456Z" },
///   "account": { "externalId": "ACC-1", "name": "Acme", "rating": "Hot", "isActive": true,
///                "profile": { "tier": "Gold", "score": 87.5 } } }</code>
/// An event always carries the full record: a property that is null or absent means "empty in the source", and the
/// field is cleared in Salesforce. The one exception is <see cref="AccountPayload.Profile"/> as a whole.
/// </summary>
public sealed record AccountEvent(EventMeta? Meta, AccountPayload? Account);

/// <summary>The <c>account</c> object of an <see cref="AccountEvent"/>.</summary>
public sealed class AccountPayload
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
[SObject("Account", ExternalIdField = "External_Id__c", RecordType = "Business")]
public sealed class AccountSObject
{
    [JsonPropertyName("External_Id__c")] public string? ExternalId { get; set; }
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
[SObject("Profile__c", ExternalIdField = "External_Id__c", RecordType = "Standard")]
public sealed class ProfileSObject
{
    [JsonPropertyName("External_Id__c")] public string? ExternalId { get; set; }
    [JsonPropertyName("Name")] public string? Name { get; set; }

    /// <summary>The Account this Profile belongs to, by the Account's external id.</summary>
    [JsonPropertyName("Account__c"), SObjectLookup(typeof(AccountSObject))]
    public string? AccountExternalId { get; set; }

    [JsonPropertyName("Tier__c")] public string? Tier { get; set; }
    [JsonPropertyName("Score__c")] public decimal? Score { get; set; }
    [JsonPropertyName("Discount__c")] public decimal? Discount { get; set; }
    [JsonPropertyName("Notes__c")] public string? Notes { get; set; }
    [JsonPropertyName("Opt_In__c")] public bool? OptIn { get; set; }
    [JsonPropertyName("Start_Date__c")] public DateOnly? StartDate { get; set; }
}

/// <summary>A row of <c>sf_account_inbox</c>: events of the account type.</summary>
public sealed class AccountInboxRow : InboxRow;