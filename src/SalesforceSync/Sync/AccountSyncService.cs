using System.Text.Json;
using System.Text.Json.Serialization;

namespace SalesforceSync.Sync;

/// <summary>
/// Inbox event for an Account and its Profile__c (entity type "account", key = account.externalId).
/// <code>{ "meta": { "time": "2026-10-14T01:02:03.456Z" },
///   "account": { "externalId": "ACC-1", "Name": "Acme", "profile": { "externalId": "PRF-1", "Tier__c": "Gold" } } }</code>
/// Mapping assumptions (change them here and in <see cref="AccountSyncService.Map"/>):
/// <list type="bullet">
/// <item>Every property of <c>account</c> except <c>externalId</c> and <c>profile</c> is an Account field, named by its
/// Salesforce API name. Absent = unchanged, <c>null</c> = clear.</item>
/// <item><c>account.profile</c> is optional. Its properties except <c>externalId</c> are Profile__c fields;
/// <c>profile.externalId</c> defaults to the account's externalId. Without a profile, only the Account is written.</item>
/// </list>
/// </summary>
public sealed record AccountEvent(AccountData? Account);

/// <summary>The <c>account</c> object of an <see cref="AccountEvent"/>.</summary>
public sealed record AccountData
{
    public string? ExternalId { get; init; }

    public ProfileData? Profile { get; init; }

    /// <summary>Every other property: Account fields by API name.</summary>
    [JsonExtensionData]
    public Dictionary<string, JsonElement>? Fields { get; init; }
}

/// <summary>The optional <c>account.profile</c> object of an <see cref="AccountEvent"/>.</summary>
public sealed record ProfileData
{
    public string? ExternalId { get; init; }

    /// <summary>Every other property: Profile__c fields by API name.</summary>
    [JsonExtensionData]
    public Dictionary<string, JsonElement>? Fields { get; init; }
}

/// <summary>Account + its Profile__c, written together (one graph); the Profile looks up its Account.</summary>
public sealed class AccountSyncService(SalesforceBatchUpsertService upsert) : ISalesforceEntitySync
{
    public const string Type = "account";
    public const string AccountObject = "Account";
    public const string ProfileObject = "Profile__c";
    public const string ExternalId = "External_Id__c";

    public static readonly UpsertStrategy<AccountEvent> Strategy = new(Type, Map, Queries);

    public string EntityType => Type;

    public Task<int> SyncBatchAsync(CancellationToken ct) => upsert.UpsertBatchAsync(Strategy, ct);

    public static UpsertUnit Map(string accountKey, AccountEvent e)
    {
        var account = e.Account ?? throw new ArgumentException("account is required");
        var records = new List<SObjectRecord> { new("acct", AccountObject, ExternalId, accountKey, SObjectFields.From(account.Fields)) };
        if (account.Profile is { } profile)
            records.Add(new SObjectRecord("prof", ProfileObject, ExternalId, profile.ExternalId ?? accountKey,
                SObjectFields.From(profile.Fields), [new SObjectLookup("Account__c", AccountObject, ExternalId, accountKey)]));
        return new UpsertUnit(records);
    }

    public static IEnumerable<SoqlQuery> Queries(IReadOnlyCollection<UpsertUnit> units) =>
    [
        new SoqlQuery(AccountObject, ExternalId, SoqlQuery.ExternalIdsOf(units, AccountObject)),
        new SoqlQuery(ProfileObject, ExternalId, SoqlQuery.ExternalIdsOf(units, ProfileObject))
    ];
}
