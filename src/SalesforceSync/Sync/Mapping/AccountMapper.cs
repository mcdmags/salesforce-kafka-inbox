using SalesforceSync.Inbox.Payloads;
using SalesforceSync.Salesforce.SObjects;

namespace SalesforceSync.Sync.Mapping;

public sealed class AccountMapper : ISObjectMapper<AccountPayload, AccountSObject>
{
    public AccountSObject Map(AccountPayload payload) => new()
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
