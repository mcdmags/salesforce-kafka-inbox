using System.Reflection;
using System.Text.Json.Serialization;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.Salesforce.SObjects;
using SalesforceSync.Sync;

namespace SalesforceSync.Tests;

/// <summary>sObject classes -> the engine's records, and the classes' Salesforce names against the mock org.</summary>
public sealed class SObjectRecordTests
{
    [Theory, TypedAutoData]
    public void Object_external_id_and_field_names_come_from_the_attributes(AccountSObject account)
    {
        var record = SObjectRecord.From("acct", account);

        (record.ReferenceId, record.SObject, record.ExternalIdField, record.ExternalId)
            .Should().Be(("acct", "Account", "External_Id__c", account.ExternalId!));
        record.Lookups.Should().BeNull();
        record.Fields.Should().BeEquivalentTo(new Dictionary<string, object?>
        {
            ["Name"] = account.Name,
            ["Rating"] = account.Rating,
            ["Industry"] = account.Industry,
            ["Description"] = account.Description,
            ["AnnualRevenue"] = account.AnnualRevenue,
            ["NumberOfEmployees"] = account.NumberOfEmployees,
            ["Phone"] = account.Phone,
            ["Website"] = account.Website,
            ["Active__c"] = account.Active,
            ["Tags__c"] = account.Tags,
            ["Customer_Since__c"] = account.CustomerSince,
            ["Last_Contacted__c"] = account.LastContacted
        }, "the external id is the record's key, not one of its fields");
    }

    [Fact]
    public void Null_properties_are_included_so_the_diff_can_clear_them()
    {
        var record = SObjectRecord.From("acct", new AccountSObject { ExternalId = "ACC-1", Name = "Acme" });

        record.Fields.Should().HaveCount(12).And.Contain("Name", "Acme");
        record.Fields.Where(f => f.Key != "Name").Should().OnlyContain(f => f.Value == null);
    }

    [Fact]
    public void Values_keep_their_CLR_types()
    {
        var record = SObjectRecord.From("acct", new AccountSObject
        {
            ExternalId = "ACC-1", AnnualRevenue = 10.5m, NumberOfEmployees = 3, Active = true,
            CustomerSince = new DateOnly(2020, 1, 15), LastContacted = DateTimeOffset.UnixEpoch
        });

        record.Fields["AnnualRevenue"].Should().BeOfType<decimal>();
        record.Fields["NumberOfEmployees"].Should().BeOfType<int>();
        record.Fields["Active__c"].Should().BeOfType<bool>();
        record.Fields["Customer_Since__c"].Should().BeOfType<DateOnly>();
        record.Fields["Last_Contacted__c"].Should().BeOfType<DateTimeOffset>();
    }

    [Theory, TypedAutoData]
    public void Lookups_carry_the_target_object_and_its_external_id_field(AccountRelationshipSObject relationship)
    {
        var record = SObjectRecord.From("rel", relationship);

        (record.SObject, record.ExternalId).Should().Be(("Account_Relationship__c", relationship.ExternalId!));
        record.Lookups.Should().Equal(
            new SObjectLookup("Account__c", "Account", "External_Id__c", relationship.AccountExternalId!),
            new SObjectLookup("Related_Account__c", "Account", "External_Id__c", relationship.RelatedAccountExternalId!));
        record.Fields.Keys.Should().BeEquivalentTo(["Name", "Type__c", "Start_Date__c", "Active__c"], "a lookup is not a value field");
    }

    [Fact]
    public void Missing_external_id_or_lookup_target_is_rejected()
    {
        var noId = () => SObjectRecord.From("prof", new ProfileSObject { AccountExternalId = "ACC-1" });
        var noTarget = () => SObjectRecord.From("prof", new ProfileSObject { ExternalId = "PRF-1" });

        noId.Should().ThrowExactly<ArgumentException>().WithMessage("*Profile__c.External_Id__c*");
        noTarget.Should().ThrowExactly<ArgumentException>().WithMessage("*Profile__c.Account__c*");
    }

    [Theory, TypedAutoData]
    public void Queries_are_derived_from_the_sObject_classes(AccountRelationshipSObject relationship)
    {
        UpsertUnit[] units = [new([SObjectRecord.From("rel", relationship)])];

        SoqlQuery.RecordsOf<AccountRelationshipSObject>(units).Should().BeEquivalentTo(
            new SoqlQuery("Account_Relationship__c", "External_Id__c", [relationship.ExternalId!]));
        SoqlQuery.LookupTargetsOf<AccountSObject>(units).Should().BeEquivalentTo(
            new SoqlQuery("Account", "External_Id__c", [relationship.AccountExternalId!, relationship.RelatedAccountExternalId!]));
    }

    // ---------- Guard: Salesforce names exist in the (mock) org ----------

    public static TheoryData<Type> SObjectTypes()
    {
        var data = new TheoryData<Type>();
        foreach (var type in SObjectMetadata.TypesIn(typeof(AccountSObject).Assembly)) data.Add(type);
        return data;
    }

    [Fact]
    public void Every_synced_object_has_an_sObject_class()
        => SObjectMetadata.TypesIn(typeof(AccountSObject).Assembly).Select(t => SObjectMetadata.For(t).SObject)
            .Should().BeEquivalentTo(FakeSchema.Objects.Keys);

    [Theory, MemberData(nameof(SObjectTypes))]
    public void Every_Salesforce_name_on_an_sObject_class_exists_in_the_org(Type sobjectType)
    {
        var meta = SObjectMetadata.For(sobjectType);
        FakeSchema.Objects.Should().ContainKey(meta.SObject);

        // Straight from the attributes, so a property the metadata would skip can't hide a typo
        var names = sobjectType.GetProperties().Select(p => p.GetCustomAttribute<JsonPropertyNameAttribute>()?.Name).ToList();
        names.Should().NotContainNulls("every property names its Salesforce field");
        names.Should().OnlyHaveUniqueItems();
        foreach (var name in names)
            FakeSchema.Find(meta.SObject, name!).Should().NotBeNull("{0}.{1} must be a field of the org", meta.SObject, name)
                .And.Match<FakeField>(f => f.Name == name, "API names are written exactly as the org has them");

        FakeSchema.Find(meta.SObject, meta.ExternalIdField).Should().NotBeNull();
        foreach (var lookup in meta.Lookups)
        {
            FakeSchema.Find(meta.SObject, lookup.Field)!.Type.Should().Be("reference", "{0} is declared as a lookup", lookup.Field);
            FakeSchema.Find(lookup.TargetSObject, lookup.TargetExternalIdField)
                .Should().NotBeNull("{0} looks up {1} by {2}", lookup.Field, lookup.TargetSObject, lookup.TargetExternalIdField);
        }
    }

    [Theory, MemberData(nameof(SObjectTypes))]
    public void Every_writable_field_of_the_org_is_on_its_sObject_class(Type sobjectType)
    {
        var meta = SObjectMetadata.For(sobjectType);

        meta.Fields.Concat(meta.Lookups.Select(l => l.Field)).Append(meta.ExternalIdField).Should().BeEquivalentTo(
            FakeSchema.Objects[meta.SObject].Where(f => f.Createable || f.Updateable).Select(f => f.Name));
    }

    [Fact]
    public void A_class_declared_wrongly_fails_when_it_is_first_used()
    {
        var noName = () => SObjectRecord.From("x", new NoFieldName { ExternalId = "1" });
        var noExternalId = () => SObjectRecord.From("x", new NoExternalId());
        var notMarked = () => SObjectRecord.From("x", new NotMarked());

        noName.Should().Throw<TypeInitializationException>().WithInnerException<InvalidOperationException>().WithMessage("*Oops*JsonPropertyName*");
        noExternalId.Should().Throw<TypeInitializationException>().WithInnerException<InvalidOperationException>().WithMessage("*Id__c*");
        notMarked.Should().Throw<TypeInitializationException>().WithInnerException<InvalidOperationException>().WithMessage("*[SObject*");
    }

    [SObject("X__c", ExternalIdField = "Id__c")]
    private sealed class NoFieldName
    {
        [JsonPropertyName("Id__c")] public string? ExternalId { get; set; }
        public string? Oops { get; set; }
    }

    [SObject("X__c", ExternalIdField = "Id__c")]
    private sealed class NoExternalId
    {
        [JsonPropertyName("Name")] public string? Name { get; set; }
    }

    private sealed class NotMarked
    {
        [JsonPropertyName("Name")] public string? Name { get; set; }
    }
}
