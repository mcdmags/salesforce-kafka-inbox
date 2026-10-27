using System.Text.Json;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// Account + its Profile__c, same batch (Accounts first). Profile names its Account by external id (Text foreign key).
/// Event without <c>profile</c> → only Account written. An Account is a person account or a business account
/// (<see cref="AccountPayload.IsPerson"/>): its record type at create decides which, and each kind has its own name fields.
/// Flow (claim, parse, record, ...) = <see cref="SalesforceBatchSyncService"/>; event handling = <see cref="Send"/>.
/// </summary>
public sealed class AccountSync(SalesforceBatchSyncService batch, SalesforceCalls salesforce, IRepository<AccountInboxRow> inbox) : IEntitySync
{
    public const string Type = AccountEvent.Type;

    /// <summary>Also the parent the relationship and contact syncs read.</summary>
    public static readonly SalesforceObject AccountObject = SalesforceObject.Of<AccountRecord>();
    private static readonly SalesforceObject ProfileObject = SalesforceObject.Of<ProfileRecord>();

    /// <summary>Developer names of the record types new Accounts get: a business's, a person's (a person account one).</summary>
    private const string BusinessRecordType = "Business";
    private const string PersonRecordType = "PersonAccount";

    private static readonly string RecordTypeIdField = SalesforceFields.Name<AccountRecord>(r => r.RecordTypeId);
    private static readonly string IsPersonAccountField = SalesforceFields.Name<AccountReadFields>(r => r.IsPersonAccount);

    public string EntityType => Type;

    public Task<int> SyncBatchAsync(CancellationToken ct)
    {
        return batch.SyncBatchAsync(AccountEvent.Reader, inbox, Send, ct);
    }

    private async Task<Failures> Send(IReadOnlyList<ReadEvent<AccountEvent>> events, CancellationToken ct)
    {
        var failures = new Failures();
        var deletes = events.Where(e => e.Event.Meta?.IsDelete() == true).ToList();
        var upserts = SalesforceCalls.KeepValid(events.Except(deletes), e => e.Event.Account is not null, "account is required", failures);

        var people = upserts.Where(IsPerson).Select(e => e.EntityId).ToHashSet(StringComparer.Ordinal);
        var desiredAccounts = upserts.Select(DesiredAccount).ToList();
        var desiredProfiles = upserts.Where(HasProfile).Select(DesiredProfile).ToList();

        var currentAccounts = await salesforce.ReadAsync(AccountObject, desiredAccounts.FieldNames().Append(IsPersonAccountField),
            events.Select(e => e.EntityId), ct);
        var currentProfiles = await salesforce.ReadAsync(ProfileObject, desiredProfiles.FieldNames(), events.Select(ProfileKey), ct);
        desiredAccounts = SameKindAsInSalesforce(desiredAccounts, people, currentAccounts, failures);
        desiredAccounts = await WithRecordTypeIfNewAsync(desiredAccounts, people, currentAccounts, failures, ct);

        await salesforce.UpsertChangedAsync(AccountObject, desiredAccounts, currentAccounts, failures, ct);
        // A profile names its account, so it is written only if the account was saved
        var profilesOfSavedAccounts = desiredProfiles.Where(p => !failures.Contains(p.EntityId));
        await salesforce.UpsertChangedAsync(ProfileObject, profilesOfSavedAccounts, currentProfiles, failures, ct);

        // Children first: an account is deleted only if deleting its profile didn't fail
        await salesforce.DeleteAsync(SalesforceCalls.RecordsToDelete(deletes, currentProfiles, ProfileKey), failures, ct);
        var accountsWithProfileDeleted = deletes.Where(e => !failures.Contains(e.EntityId)).ToList();
        await salesforce.DeleteAsync(SalesforceCalls.RecordsToDelete(accountsWithProfileDeleted, currentAccounts), failures, ct);

        return failures;
    }

    private static bool IsPerson(ReadEvent<AccountEvent> e)
    {
        return e.Event.Account!.IsPerson == true;
    }

    private static DesiredRecord DesiredAccount(ReadEvent<AccountEvent> e)
    {
        return new DesiredRecord(e.EntityId, e.EntityId, AccountFieldsOf(e.Event.Account!));
    }

    /// <summary>
    /// An Account Salesforce has as the other kind (the event a person, Salesforce a business account, or the other
    /// way round) can't take the event's name fields, and an update can't change its kind: dead-lettered, to replay
    /// once the Account is converted or deleted in Salesforce.
    /// </summary>
    private static List<DesiredRecord> SameKindAsInSalesforce(List<DesiredRecord> accounts, HashSet<string> people,
        Dictionary<string, JsonElement> currentAccounts, Failures failures)
    {
        var same = new List<DesiredRecord>();
        foreach (var a in accounts)
        {
            var person = people.Contains(a.EntityId);
            if (currentAccounts.TryGetValue(a.ExternalId, out var current) && current.TryGetProperty(IsPersonAccountField, out var isPerson)
                && isPerson.ValueKind is JsonValueKind.True or JsonValueKind.False && isPerson.GetBoolean() != person)
            {
                failures.Add(a.EntityId, SalesforceErrors.AccountKindMismatch,
                    $"the event is a {Kind(person)} and Salesforce has the Account as a {Kind(!person)} account: convert or delete it there, then replay",
                    permanent: true);
            }
            else
            {
                same.Add(a);
            }
        }

        return same;
    }

    private static string Kind(bool person)
    {
        return person ? "person" : "business";
    }

    /// <summary>
    /// Accounts Salesforce doesn't have yet get their kind's record type (needed only to create one). If the org lacks
    /// it, they fail (retried) and are left out; their profiles then wait for them.
    /// </summary>
    private async Task<List<DesiredRecord>> WithRecordTypeIfNewAsync(List<DesiredRecord> accounts, HashSet<string> people,
        Dictionary<string, JsonElement> currentAccounts, Failures failures, CancellationToken ct)
    {
        var ready = new List<DesiredRecord>();
        var ids = new Dictionary<string, string?>(); // developer name → Id, looked up once a batch
        foreach (var a in accounts)
        {
            if (currentAccounts.ContainsKey(a.ExternalId))
            {
                ready.Add(a);
                continue;
            }

            var recordType = people.Contains(a.EntityId) ? PersonRecordType : BusinessRecordType;
            if (!ids.TryGetValue(recordType, out var id))
            {
                ids[recordType] = id = await salesforce.RecordTypeIdAsync(AccountObject, recordType, ct);
            }

            if (id is null)
            {
                failures.Add(a.EntityId, SalesforceErrors.InvalidRecordType, $"the Account record type {recordType} is not in the org");
            }
            else
            {
                ready.Add(a with { Fields = new(a.Fields) { [RecordTypeIdField] = id } });
            }
        }

        return ready;
    }

    /// <summary>An event without a profile leaves Salesforce's profile as it is.</summary>
    private static bool HasProfile(ReadEvent<AccountEvent> e)
    {
        return e.Event.Account!.Profile is not null;
    }

    private static DesiredRecord DesiredProfile(ReadEvent<AccountEvent> e)
    {
        return new DesiredRecord(e.EntityId, ProfileKey(e), ProfileFields(e));
    }

    private static string ProfileKey(ReadEvent<AccountEvent> e)
    {
        return e.Event.Account?.Profile?.ExternalId ?? e.EntityId;
    }

    /// <summary>A person's fields (first and last name) or a business's (Name), and the fields both have.</summary>
    private static Dictionary<string, object?> AccountFieldsOf(AccountPayload a)
    {
        return a.IsPerson == true
            ? SalesforceFields.Of(WithSharedFields(new PersonAccountRecord { FirstName = a.FirstName, LastName = a.LastName }, a))
            : SalesforceFields.Of(WithSharedFields(new AccountRecord { Name = a.Name }, a));
    }

    private static T WithSharedFields<T>(T record, AccountPayload a)
        where T : AccountFields
    {
        record.Rating = a.Rating;
        record.Industry = a.Industry;
        record.Description = a.Description;
        record.AnnualRevenue = a.AnnualRevenue;
        record.NumberOfEmployees = a.NumberOfEmployees;
        record.Phone = a.Phone;
        record.Website = a.Website;
        record.Active = a.IsActive;
        // list in event, ';'-separated multi-select picklist in Salesforce
        record.Tags = a.Tags is null ? null : string.Join(';', a.Tags);
        record.CustomerSince = a.CustomerSince;
        record.LastContacted = a.LastContactedAt;
        return record;
    }

    private static Dictionary<string, object?> ProfileFields(ReadEvent<AccountEvent> e)
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
            StartDate = p.StartDate
        });
    }
}
