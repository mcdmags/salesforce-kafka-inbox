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
        var upserts = events.Except(deletes).ToList();

        foreach (var e in upserts.Where(e => e.Event.Account is null))
        {
            SalesforceCalls.Fail(failures, e.EntityId, SalesforceErrors.InvalidEvent, "account is required", permanent: true);
        }

        upserts = [.. upserts.Where(e => e.Event.Account is not null)];
        var accountType = await salesforce.RecordTypeIdAsync(AccountObject, "Business", ct);
        var profileType = await salesforce.RecordTypeIdAsync(ProfileObject, "Standard", ct);
        if (accountType is null || profileType is null)
        {
            foreach (var e in upserts)
            {
                SalesforceCalls.Fail(failures, e.EntityId, SalesforceErrors.InvalidRecordType, "the Account or Profile record type is not in the org");
            }

            upserts = [];
        }

        var wantedAccounts = upserts.Select(e => (e.EntityId, Fields: AccountFields(e.Event.Account!, accountType!))).ToList();
        var wantedProfiles = upserts.Where(e => e.Event.Account!.Profile is not null) // no profile in event: leave Salesforce's
            .Select(e => (e.EntityId, ExternalId: ProfileKey(e), Fields: ProfileFields(e, profileType!))).ToList();

        // READ Salesforce state: Ids for deletes, values to compare
        var accountsNow = await salesforce.ReadAsync(AccountObject, wantedAccounts.SelectMany(w => w.Fields.Keys), events.Select(e => e.EntityId), ct);
        var profilesNow = await salesforce.ReadAsync(ProfileObject, wantedProfiles.SelectMany(w => w.Fields.Keys), events.Select(ProfileKey), ct);

        // DECIDE: missing → create, differs → update, same → skip
        var accounts = wantedAccounts
            .Select(w => (w.EntityId, ExternalId: w.EntityId, Fields: SalesforceCalls.Changes(w.Fields, Now(accountsNow, w.EntityId))))
            .Where(x => x.Fields.Count > 0).ToList();
        var profiles = wantedProfiles
            .Select(w => (w.EntityId, w.ExternalId, Fields: SalesforceCalls.Changes(w.Fields, Now(profilesNow, w.ExternalId))))
            .Where(x => x.Fields.Count > 0).ToList();

        // WRITE. Accounts first; profile only if its account saved
        await salesforce.UpsertAsync(AccountObject, accounts, failures, ct);
        await salesforce.UpsertAsync(ProfileObject, profiles.Where(p => !failures.ContainsKey(p.EntityId)), failures, ct);

        // Delete Profile, then Account (children first). Record Salesforce lacks = already deleted
        await salesforce.DeleteAsync(deletes.Where(e => profilesNow.ContainsKey(ProfileKey(e)))
            .Select(e => (e.EntityId, profilesNow[ProfileKey(e)].GetProperty("Id").GetString()!)), failures, ct);
        await salesforce.DeleteAsync(deletes.Where(e => accountsNow.ContainsKey(e.EntityId) && !failures.ContainsKey(e.EntityId))
            .Select(e => (e.EntityId, accountsNow[e.EntityId].GetProperty("Id").GetString()!)), failures, ct);

        return failures;
    }

    private static string ProfileKey(ReadEvent<AccountEvent> e)
    {
        return e.Event.Account?.Profile?.ExternalId ?? e.EntityId;
    }

    private static JsonElement? Now(Dictionary<string, JsonElement> found, string key)
    {
        return found.TryGetValue(key, out var row) ? row : null;
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
