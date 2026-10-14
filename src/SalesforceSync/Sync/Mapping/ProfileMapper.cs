using SalesforceSync.Inbox.Payloads;
using SalesforceSync.Salesforce.SObjects;

namespace SalesforceSync.Sync.Mapping;

/// <summary>
/// The profile on its own. What it takes from its account (the default external id and the Account lookup) is filled
/// in by <see cref="AccountSyncService"/>, which has both.
/// </summary>
public sealed class ProfileMapper : ISObjectMapper<ProfilePayload, ProfileSObject>
{
    public ProfileSObject Map(ProfilePayload payload) => new()
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
