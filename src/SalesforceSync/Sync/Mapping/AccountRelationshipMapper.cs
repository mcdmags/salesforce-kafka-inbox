using SalesforceSync.Inbox.Payloads;
using SalesforceSync.Salesforce.SObjects;

namespace SalesforceSync.Sync.Mapping;

public sealed class AccountRelationshipMapper : ISObjectMapper<AccountRelationshipPayload, AccountRelationshipSObject>
{
    public AccountRelationshipSObject Map(AccountRelationshipPayload payload) => new()
    {
        ExternalId = payload.ExternalId,
        Name = payload.Name,
        AccountExternalId = payload.AccountExternalId,
        RelatedAccountExternalId = payload.RelatedAccountExternalId,
        Type = payload.Type,
        StartDate = payload.StartDate,
        Active = payload.IsActive
    };
}
