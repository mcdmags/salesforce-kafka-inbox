using SalesforceSync.Sync;
using SalesforceSync.ToSalesforce;

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
        Syncs.Account.ToAccount(payload).Should().BeEquivalentTo(new AccountSObject
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
        Syncs.Account.ToProfile(payload).Should().BeEquivalentTo(new ProfileSObject
        {
            ExternalId = payload.ExternalId,
            Name = payload.Name,
            AccountId = null, // the account's: filled in by Syncs.Account.Map
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
        Syncs.Relationship.ToRelationship(payload).Should().BeEquivalentTo(new AccountRelationshipSObject
        {
            ExternalId = payload.ExternalId,
            Name = payload.Name,
            AccountId = payload.AccountExternalId,
            RelatedAccountId = payload.RelatedAccountExternalId,
            Type = payload.Type,
            StartDate = payload.StartDate,
            Active = payload.IsActive
        });
    }

    [Theory, TypedAutoData]
    public void Contact_payload_maps_field_by_field(ContactPayload payload)
    {
        Syncs.Contact.ToContact(payload).Should().BeEquivalentTo(new ContactSObject
        {
            ExternalId = payload.ExternalId,
            AccountId = payload.AccountExternalId,
            FirstName = payload.FirstName,
            LastName = payload.LastName,
            Email = payload.Email,
            Phone = payload.Phone
        });
    }

    [Fact]
    public void Empty_payloads_map_to_empty_sObjects()
    {
        // null = empty in the source; a mapper passes it on and never invents a value
        Syncs.Account.ToAccount(new AccountPayload()).Should().BeEquivalentTo(new AccountSObject());
        Syncs.Account.ToProfile(new ProfilePayload()).Should().BeEquivalentTo(new ProfileSObject());
        Syncs.Relationship.ToRelationship(new AccountRelationshipPayload()).Should().BeEquivalentTo(new AccountRelationshipSObject());
        Syncs.Contact.ToContact(new ContactPayload()).Should().BeEquivalentTo(new ContactSObject());
    }

    [Fact]
    public void Tags_list_becomes_a_multi_select_value()
    {
        Syncs.Account.ToAccount(new AccountPayload { Tags = ["B2B", "Retail"] }).Tags.Should().Be("B2B;Retail");
        Syncs.Account.ToAccount(new AccountPayload { Tags = [] }).Tags.Should().BeEmpty("Salesforce stores an empty value as null");
        Syncs.Account.ToAccount(new AccountPayload { Tags = null }).Tags.Should().BeNull();
    }

    [Theory, TypedAutoData]
    public void Every_sObject_property_is_set_by_its_mapper(AccountPayload account, ProfilePayload profile, AccountRelationshipPayload relationship,
        ContactPayload contact)
    {
        // Guards the tests above too: with every payload property filled, nothing mapped may be left empty
        NullProperties(Syncs.Account.ToAccount(account)).Should().BeEmpty();
        NullProperties(Syncs.Account.ToProfile(profile)).Should().Equal(nameof(ProfileSObject.AccountId));
        NullProperties(Syncs.Relationship.ToRelationship(relationship)).Should().BeEmpty();
        NullProperties(Syncs.Contact.ToContact(contact)).Should().BeEmpty();
    }

    private static IEnumerable<string> NullProperties(object sobject)
    {
        return sobject.GetType().GetProperties().Where(p => p.GetValue(sobject) is null).Select(p => p.Name);
    }
}