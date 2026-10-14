namespace SalesforceSync.Inbox.Payloads;

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