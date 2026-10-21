using System.Text.Json;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// Account + its Profile__c, same batch (Accounts first). Profile names its Account by external id (Text foreign key).
/// Event without <c>profile</c> → only Account written.
/// Flow (claim, parse, record, ...) = <see cref="SalesforceBatchSyncService"/>; event handling = <see cref="Send"/>.
/// </summary>
public sealed class AccountSync(SalesforceBatchSyncService batch, SalesforceCalls salesforce, IRepository<AccountInboxRow> inbox) : IEntitySync
{
    public const string Type = AccountEvent.Type;

    /// <summary>Also the parent the relationship and contact syncs read.</summary>
    public static readonly SalesforceObject AccountObject = SalesforceObject.Of<AccountRecord>();
    private static readonly SalesforceObject ProfileObject = SalesforceObject.Of<ProfileRecord>();

    public string EntityType => Type;

    public Task<int> SyncBatchAsync(CancellationToken ct)
    {
        return batch.SyncBatchAsync(AccountEvent.Reader, inbox, Send, ct);
    }

    private async Task<Dictionary<string, List<Failure>>> Send(IReadOnlyList<ReadEvent<AccountEvent>> events, CancellationToken ct)
    {
        var failures = new Dictionary<string, List<Failure>>();
        var deletes = events.Where(e => e.Event.Meta?.IsDelete() == true).ToList();
        var upserts = UpsertsWithAccount(events.Except(deletes), failures);

        var (accountType, profileType) = await RecordTypeIdsAsync(ct);
        if (accountType is null || profileType is null)
        {
            FailAll(upserts, failures, SalesforceErrors.InvalidRecordType, "the Account or Profile record type is not in the org");
            upserts = [];
        }

        var wantedAccounts = upserts.Select(e => WantedAccount(e, accountType)).ToList();
        var wantedProfiles = upserts.Where(HasProfile).Select(e => WantedProfile(e, profileType)).ToList();

        var accountsNow = await ReadCurrentAsync(AccountObject, wantedAccounts, events.Select(e => e.EntityId), ct);
        var profilesNow = await ReadCurrentAsync(ProfileObject, wantedProfiles, events.Select(ProfileKey), ct);

        await salesforce.UpsertAsync(AccountObject, Changed(wantedAccounts, accountsNow), failures, ct);
        await salesforce.UpsertAsync(ProfileObject, ProfilesOfSavedAccounts(Changed(wantedProfiles, profilesNow), failures), failures, ct);

        await salesforce.DeleteAsync(Existing(deletes, profilesNow, ProfileKey), failures, ct);
        await salesforce.DeleteAsync(Existing(AccountsWithProfileDeleted(deletes, failures), accountsNow, e => e.EntityId), failures, ct);

        return failures;
    }

    /// <summary>A record the event wants in Salesforce: its entity, the external id it is matched by, every field it sends.</summary>
    private sealed record Wanted(string EntityId, string ExternalId, Dictionary<string, object?> Fields);

    /// <summary>The upserts that have an account; one without is a bad event, dead-lettered at once.</summary>
    private static List<ReadEvent<AccountEvent>> UpsertsWithAccount(IEnumerable<ReadEvent<AccountEvent>> upserts, Dictionary<string, List<Failure>> failures)
    {
        var all = upserts.ToList();
        foreach (var e in all.Where(e => e.Event.Account is null))
        {
            SalesforceCalls.Fail(failures, e.EntityId, SalesforceErrors.InvalidEvent, "account is required", permanent: true);
        }

        return [.. all.Where(e => e.Event.Account is not null)];
    }

    private async Task<(string? Account, string? Profile)> RecordTypeIdsAsync(CancellationToken ct)
    {
        return (await salesforce.RecordTypeIdAsync(AccountObject, "Business", ct), await salesforce.RecordTypeIdAsync(ProfileObject, "Standard", ct));
    }

    private static void FailAll(IEnumerable<ReadEvent<AccountEvent>> events, Dictionary<string, List<Failure>> failures, string code, string message)
    {
        foreach (var e in events)
        {
            SalesforceCalls.Fail(failures, e.EntityId, code, message);
        }
    }

    private static Wanted WantedAccount(ReadEvent<AccountEvent> e, string? recordTypeId)
    {
        return new Wanted(e.EntityId, e.EntityId, AccountFields(e.Event.Account!, recordTypeId));
    }

    /// <summary>An event without a profile leaves Salesforce's profile as it is.</summary>
    private static bool HasProfile(ReadEvent<AccountEvent> e)
    {
        return e.Event.Account!.Profile is not null;
    }

    private static Wanted WantedProfile(ReadEvent<AccountEvent> e, string? recordTypeId)
    {
        return new Wanted(e.EntityId, ProfileKey(e), ProfileFields(e, recordTypeId));
    }

    /// <summary>
    /// What Salesforce has for these external ids: the values to compare, and the Ids deletes need. Reads every field
    /// the wanted records send, so a field the org lacks shows here, before anything is written.
    /// </summary>
    private Task<Dictionary<string, JsonElement>> ReadCurrentAsync(SalesforceObject sobject, List<Wanted> wanted, IEnumerable<string> externalIds,
        CancellationToken ct)
    {
        return salesforce.ReadAsync(sobject, wanted.SelectMany(w => w.Fields.Keys), externalIds, ct);
    }

    /// <summary>What to write: a missing record with its fields, an existing one with the fields that differ; nothing when all are the same.</summary>
    private static List<(string Key, string ExternalId, Dictionary<string, object?> Fields)> Changed(List<Wanted> wanted, Dictionary<string, JsonElement> current)
    {
        return [.. wanted
            .Select(w => (w.EntityId, w.ExternalId, Fields: SalesforceCalls.Changes(w.Fields, current.TryGetValue(w.ExternalId, out var row) ? row : null)))
            .Where(w => w.Fields.Count > 0)];
    }

    /// <summary>A profile names its account, so it is written only if the account was saved.</summary>
    private static IEnumerable<(string Key, string ExternalId, Dictionary<string, object?> Fields)> ProfilesOfSavedAccounts(
        IEnumerable<(string Key, string ExternalId, Dictionary<string, object?> Fields)> profiles, Dictionary<string, List<Failure>> failures)
    {
        return profiles.Where(p => !failures.ContainsKey(p.Key));
    }

    /// <summary>Children first: an account is deleted only if deleting its profile didn't fail.</summary>
    private static IEnumerable<ReadEvent<AccountEvent>> AccountsWithProfileDeleted(IEnumerable<ReadEvent<AccountEvent>> deletes, Dictionary<string, List<Failure>> failures)
    {
        return deletes.Where(e => !failures.ContainsKey(e.EntityId));
    }

    /// <summary>The deletes whose record Salesforce has, by its Id. One Salesforce lacks is already deleted.</summary>
    private static IEnumerable<(string Key, string Id)> Existing(IEnumerable<ReadEvent<AccountEvent>> deletes, Dictionary<string, JsonElement> current,
        Func<ReadEvent<AccountEvent>, string> externalId)
    {
        return deletes.Where(e => current.ContainsKey(externalId(e))).Select(e => (e.EntityId, current[externalId(e)].GetProperty("Id").GetString()!));
    }

    private static string ProfileKey(ReadEvent<AccountEvent> e)
    {
        return e.Event.Account?.Profile?.ExternalId ?? e.EntityId;
    }

    private static Dictionary<string, object?> AccountFields(AccountPayload a, string? recordTypeId)
    {
        return SalesforceFields.Of(new AccountRecord
        {
            Name = a.Name,
            Rating = a.Rating,
            Industry = a.Industry,
            Description = a.Description,
            AnnualRevenue = a.AnnualRevenue,
            NumberOfEmployees = a.NumberOfEmployees,
            Phone = a.Phone,
            Website = a.Website,
            Active = a.IsActive,
            // list in event, ';'-separated multi-select picklist in Salesforce
            Tags = a.Tags is null ? null : string.Join(';', a.Tags),
            CustomerSince = a.CustomerSince,
            LastContacted = a.LastContactedAt,
            RecordTypeId = recordTypeId
        });
    }

    private static Dictionary<string, object?> ProfileFields(ReadEvent<AccountEvent> e, string? recordTypeId)
    {
        var p = e.Event.Account!.Profile!;
        return SalesforceFields.Of(new ProfileRecord
        {
            Name = p.Name,
            AccountId = e.EntityId,
            Tier = p.Tier,
            Score = p.Score,
            Discount = p.DiscountPercent,
            Notes = p.Notes,
            OptIn = p.OptIn,
            StartDate = p.StartDate,
            RecordTypeId = recordTypeId
        });
    }
}
