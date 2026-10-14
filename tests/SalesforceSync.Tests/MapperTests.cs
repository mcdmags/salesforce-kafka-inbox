using SalesforceSync.Inbox.Payloads;
using SalesforceSync.Salesforce.SObjects;
using SalesforceSync.Sync.Mapping;

namespace SalesforceSync.Tests;

/// <summary>
/// The hand-written mappers: an anonymous payload in, the expected sObject out. Each expectation lists every
/// property, so a field added to an sObject but forgotten in its mapper fails here.
/// </summary>
public sealed class MapperTests
{
    [Theory, TypedAutoData]
    public void Account_payload_maps_field_by_field(AccountPayload payload)
    {
        new AccountMapper().Map(payload).Should().BeEquivalentTo(new AccountSObject
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
        new ProfileMapper().Map(payload).Should().BeEquivalentTo(new ProfileSObject
        {
            ExternalId = payload.ExternalId,
            Name = payload.Name,
            AccountExternalId = null, // the account's: filled in by AccountSyncService
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
        new AccountRelationshipMapper().Map(payload).Should().BeEquivalentTo(new AccountRelationshipSObject
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
        new AccountMapper().Map(new AccountPayload()).Should().BeEquivalentTo(new AccountSObject());
        new ProfileMapper().Map(new ProfilePayload()).Should().BeEquivalentTo(new ProfileSObject());
        new AccountRelationshipMapper().Map(new AccountRelationshipPayload()).Should().BeEquivalentTo(new AccountRelationshipSObject());
    }

    [Fact]
    public void Tags_list_becomes_a_multi_select_value()
    {
        var mapper = new AccountMapper();

        mapper.Map(new AccountPayload { Tags = ["B2B", "Retail"] }).Tags.Should().Be("B2B;Retail");
        mapper.Map(new AccountPayload { Tags = [] }).Tags.Should().BeEmpty("Salesforce stores an empty value as null");
        mapper.Map(new AccountPayload { Tags = null }).Tags.Should().BeNull();
    }

    [Theory, TypedAutoData]
    public void Every_sObject_property_is_set_by_its_mapper(AccountPayload account, ProfilePayload profile, AccountRelationshipPayload relationship)
    {
        // Guards the tests above too: with every payload property filled, nothing mapped may be left empty
        NullProperties(new AccountMapper().Map(account)).Should().BeEmpty();
        NullProperties(new ProfileMapper().Map(profile)).Should().Equal(nameof(ProfileSObject.AccountExternalId));
        NullProperties(new AccountRelationshipMapper().Map(relationship)).Should().BeEmpty();
    }

    private static IEnumerable<string> NullProperties(object sobject)
    {
        return sobject.GetType().GetProperties().Where(p => p.GetValue(sobject) is null).Select(p => p.Name);
    }
}