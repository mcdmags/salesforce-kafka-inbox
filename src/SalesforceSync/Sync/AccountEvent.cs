namespace SalesforceSync.Sync;

/// <summary>
/// Inbox event for account + profile (entity type "account", key = account.externalId): account part of its aggregate
/// (<see cref="AccountAggregate"/>) as source sends it, or Account delete (its key, plus Profile's if own). camelCase
/// JSON. No Salesforce names.
/// <code>{ "meta": { "time": "2026-10-14T01:02:03.456Z", "type": "updated" },
///   "account": { "externalId": "ACC-1", "name": "Acme", "rating": "Hot", "isActive": true,
///                "profile": { "tier": "Gold", "score": 87.5 } } }
/// { "meta": { "time": "2026-10-15T01:02:03.456Z", "type": "deleted" }, "account": { "externalId": "ACC-1" } }</code>
/// Event always carries full record: null/absent property = "empty in source" → field cleared in Salesforce. Only
/// exception: <see cref="AccountPayload.Profile"/> as a whole.
/// </summary>
public sealed record AccountEvent(EventMeta? Meta, AccountPayload? Account)
{
    public const string Type = "account";

    /// <summary>How inbox events of this type are read: entity id + meta.</summary>
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

    /// <summary>Optional. Absent → only Account written; Salesforce's Profile untouched.</summary>
    public ProfilePayload? Profile { get; set; }
}

/// <summary>The optional <c>account.profile</c> object of an <see cref="AccountEvent"/>.</summary>
public sealed class ProfilePayload
{
    /// <summary>Defaults to account's externalId.</summary>
    public string? ExternalId { get; set; }

    public string? Name { get; set; }
    public string? Tier { get; set; }
    public decimal? Score { get; set; }
    public decimal? DiscountPercent { get; set; }
    public string? Notes { get; set; }
    public bool? OptIn { get; set; }
    public DateOnly? StartDate { get; set; }
}
