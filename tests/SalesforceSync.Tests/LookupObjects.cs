using System.Text.Json.Serialization;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.Tests;

/// <summary>
/// A real lookup, for the engine's lookup machinery (Id resolution, <c>@{reference}</c> in Composite), which the
/// example objects (Text foreign keys) no longer use: the Contact's standard <c>AccountId</c> lookup, which the mock org
/// has like every org.
/// </summary>
[SObject("Contact", ExternalIdField = "ExternalId__c", RecordType = "Standard")]
internal sealed class LookupContactSObject
{
    [JsonPropertyName("ExternalId__c")] public string? ExternalId { get; set; }

    [JsonPropertyName("AccountId"), SObjectLookup(typeof(AccountSObject))]
    public string? AccountExternalId { get; set; }

    [JsonPropertyName("LastName")] public string? LastName { get; set; }
}

/// <summary>Strategies of <see cref="LookupContactSObject"/>, in the shape of the real ones.</summary>
internal static class LookupStrategies
{
    /// <summary>
    /// An Account and its Contact "C-{key}" (named after the account) that looks it up: a new pair goes in one
    /// Composite request, the Contact pointing at <c>@{Account[i].id}</c>.
    /// </summary>
    public static readonly SyncStrategy<AccountEvent> AccountWithContact = new("accountWithContact", e => e.Account?.ExternalId,
        e => e.Meta,
        (key, e) => new SyncUnit(
        [
            SObjectRecord.From("acct", AccountSync.ToAccount(e.Account!)),
            SObjectRecord.From("contact", new LookupContactSObject { ExternalId = $"C-{key}", AccountExternalId = key, LastName = e.Account!.Name })
        ]),
        units => [SoqlQuery.RecordsOf<AccountSObject>(units), SoqlQuery.RecordsOf<LookupContactSObject>(units)]);

    /// <summary>A Contact alone, looking up an Account of another inbox: the read finds the Account's Id.</summary>
    public static readonly SyncStrategy<ContactEvent> Contact = new("lookupContact", e => e.Contact?.ExternalId, e => e.Meta,
        (_, e) => new SyncUnit([SObjectRecord.From("contact", new LookupContactSObject
        {
            ExternalId = e.Contact!.ExternalId,
            AccountExternalId = e.Contact.AccountExternalId,
            LastName = e.Contact.LastName
        })]),
        units => [SoqlQuery.RecordsOf<LookupContactSObject>(units), SoqlQuery.LookupTargetsOf<AccountSObject>(units)]);
}