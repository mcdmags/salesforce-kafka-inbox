using SalesforceSync.Sync;

namespace SalesforceSync.Tests;

/// <summary>
/// The hand-written mappings: an anonymous payload in, the expected sObject out. Each expectation lists every
/// property, so a field added to an sObject but forgotten in its mapping fails here.
/// </summary>
public sealed class MapperTests
{
    [Theory, TypedAutoData]
    public void Account_payload_maps_field_by_field(AccountPayload payload)
    {
        AccountSync.ToAccount(payload).Should().BeEquivalentTo(new AccountSObject
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
            Tags = string.Join(';', payload.Tags!),
            CustomerSince = payload.CustomerSince,
            LastContacted = payload.LastContactedAt
        });
    }

    [Theory, TypedAutoData]
    public void Profile_payload_maps_field_by_field(ProfilePayload payload)
    {
        AccountSync.ToProfile(payload).Should().BeEquivalentTo(new ProfileSObject
        {
            ExternalId = payload.ExternalId,
            Name = payload.Name,
            AccountExternalId = null, // the account's: filled in by AccountSync.Map
            Tier = payload.Tier,
            Score = payload.Score,
            Discount = payload.DiscountPercent,
            Notes = payload.Notes,
            OptIn = payload.OptIn,
            StartDate = payload.StartDate
        });
    }

    [Theory, TypedAutoData]
    public void Relationship_payload_maps_field_by_field(AccountRelationshipPayload payload)
    {
        AccountRelationshipSync.ToRelationship(payload).Should().BeEquivalentTo(new AccountRelationshipSObject
        {
            ExternalId = payload.ExternalId,
            Name = payload.Name,
            AccountExternalId = payload.AccountExternalId,
            RelatedAccountExternalId = payload.RelatedAccountExternalId,
            Type = payload.Type,
            StartDate = payload.StartDate,
            Active = payload.IsActive
        });
    }

    [Fact]
    public void Empty_payloads_map_to_empty_sObjects()
    {
        // null = empty in the source; a mapper passes it on and never invents a value
        AccountSync.ToAccount(new AccountPayload()).Should().BeEquivalentTo(new AccountSObject());
        AccountSync.ToProfile(new ProfilePayload()).Should().BeEquivalentTo(new ProfileSObject());
        AccountRelationshipSync.ToRelationship(new AccountRelationshipPayload()).Should().BeEquivalentTo(new AccountRelationshipSObject());
    }

    [Fact]
    public void Tags_list_becomes_a_multi_select_value()
    {
        AccountSync.ToAccount(new AccountPayload { Tags = ["B2B", "Retail"] }).Tags.Should().Be("B2B;Retail");
        AccountSync.ToAccount(new AccountPayload { Tags = [] }).Tags.Should().BeEmpty("Salesforce stores an empty value as null");
        AccountSync.ToAccount(new AccountPayload { Tags = null }).Tags.Should().BeNull();
    }

    [Theory, TypedAutoData]
    public void Every_sObject_property_is_set_by_its_mapper(AccountPayload account, ProfilePayload profile, AccountRelationshipPayload relationship)
    {
        // Guards the tests above too: with every payload property filled, nothing mapped may be left empty
        NullProperties(AccountSync.ToAccount(account)).Should().BeEmpty();
        NullProperties(AccountSync.ToProfile(profile)).Should().Equal(nameof(ProfileSObject.AccountExternalId));
        NullProperties(AccountRelationshipSync.ToRelationship(relationship)).Should().BeEmpty();
    }

    private static IEnumerable<string> NullProperties(object sobject)
    {
        return sobject.GetType().GetProperties().Where(p => p.GetValue(sobject) is null).Select(p => p.Name);
    }
}