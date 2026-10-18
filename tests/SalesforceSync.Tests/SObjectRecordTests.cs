using System.Reflection;
using System.Text.Json.Serialization;
using SalesforceSync.FakeSalesforce;
using SalesforceSync.Infrastructure.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.Tests;

/// <summary>sObject classes -> the engine's records, and the classes' Salesforce names against the mock org.</summary>
public sealed class SObjectRecordTests
{
    [Theory, TypedAutoData]
    public void Object_external_id_and_field_names_come_from_the_attributes(AccountSObject account)
    {
        var record = SObjectRecord.From("acct", account);

        (record.ReferenceId, record.SObject, record.ExternalIdField, record.ExternalId, record.RecordType)
            .Should().Be(("acct", "Account", "ExternalId__c", account.ExternalId!, "Business"));
        record.ForeignKeys.Should().BeNull();
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
            ExternalId = "ACC-1",
            AnnualRevenue = 10.5m,
            NumberOfEmployees = 3,
            Active = true,
            CustomerSince = new DateOnly(2020, 1, 15),
            LastContacted = DateTimeOffset.UnixEpoch
        });

        record.Fields["AnnualRevenue"].Should().BeOfType<decimal>();
        record.Fields["NumberOfEmployees"].Should().BeOfType<int>();
        record.Fields["Active__c"].Should().BeOfType<bool>();
        record.Fields["Customer_Since__c"].Should().BeOfType<DateOnly>();
        record.Fields["Last_Contacted__c"].Should().BeOfType<DateTimeOffset>();
    }

    [Theory, TypedAutoData]
    public void Foreign_keys_are_value_fields_that_also_name_their_parent(AccountRelationshipSObject relationship)
    {
        var record = SObjectRecord.From("rel", relationship);

        (record.SObject, record.ExternalId).Should().Be(("Account_Relationship__c", relationship.ExternalId!));
        record.ForeignKeys.Should().Equal(
            new ForeignKey("AccountId__c", "Account", "ExternalId__c", relationship.AccountId!),
            new ForeignKey("RelatedAccountId__c", "Account", "ExternalId__c", relationship.RelatedAccountId!));
        record.Fields.Should().Contain("AccountId__c", relationship.AccountId).And.Contain("RelatedAccountId__c", relationship.RelatedAccountId,
            "the text is written as it is");
    }

    [Fact]
    public void Missing_external_id_is_rejected_and_an_empty_foreign_key_is_just_an_empty_field()
    {
        var noId = () => SObjectRecord.From("prof", new ProfileSObject { AccountId = "ACC-1" });

        noId.Should().ThrowExactly<ArgumentException>().WithMessage("*Profile__c.ExternalId__c*");
        var noParent = SObjectRecord.From("prof", new ProfileSObject { ExternalId = "PRF-1" });
        noParent.ForeignKeys.Should().BeNull("no parent to wait for");
        noParent.Fields.Should().Contain("AccountId__c", null);
    }

    [Theory, TypedAutoData]
    public void Queries_are_derived_from_the_sObject_classes(AccountRelationshipSObject relationship)
    {
        SyncUnit[] units = [new([SObjectRecord.From("rel", relationship)])];

        SoqlQuery.RecordsOf<AccountRelationshipSObject>(units).Should().BeEquivalentTo(
            new SoqlQuery("Account_Relationship__c", "ExternalId__c", [relationship.ExternalId!]));
        SoqlQuery.ParentsOf<AccountSObject>(units).Should().BeEquivalentTo(
            new SoqlQuery("Account", "ExternalId__c", [relationship.AccountId!, relationship.RelatedAccountId!]),
            "a foreign key's parent is read to know it exists");
    }

    // ---------- Guard: Salesforce names exist in the (mock) org ----------

    public static TheoryData<Type> SObjectTypes()
    {
        var data = new TheoryData<Type>();
        foreach (var type in SObjectMetadata.TypesIn(typeof(AccountSObject).Assembly))
        {
            data.Add(type);
        }

        return data;
    }

    [Fact]
    public void Every_synced_object_has_an_sObject_class()
    {
        SObjectMetadata.TypesIn(typeof(AccountSObject).Assembly).Select(t => SObjectMetadata.For(t).SObject)
                .Should().BeEquivalentTo(FakeSchema.Objects.Keys);
    }

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
        {
            FakeSchema.Find(meta.SObject, name!).Should().NotBeNull("{0}.{1} must be a field of the org", meta.SObject, name)
                .And.Match<FakeField>(f => f.Name == name, "API names are written exactly as the org has them");
        }

        FakeSchema.Find(meta.SObject, meta.ExternalIdField).Should().NotBeNull();
        FakeSchema.Find(meta.SObject, SObjectSchema.RecordTypeIdField).Should().NotBeNull("every synced object has record types");
        FakeSchema.Describe(meta.SObject)["recordTypeInfos"]!.AsArray()
            .Should().Contain(r => (string)r!["developerName"]! == meta.RecordType && (bool)r["active"]! && (bool)r["available"]!,
                "{0}'s record type {1} must exist in the org, active and available", meta.SObject, meta.RecordType);
        foreach (var key in meta.ForeignKeys)
        {
            FakeSchema.Find(meta.SObject, key.Field)!.KeyOf.Should().Be(key.TargetSObject, "{0} is declared as a foreign key", key.Field);
            FakeSchema.Find(key.TargetSObject, key.TargetExternalIdField).Should().NotBeNull();
        }
    }

    [Theory, MemberData(nameof(SObjectTypes))]
    public void Every_writable_field_of_the_org_is_on_its_sObject_class(Type sobjectType)
    {
        var meta = SObjectMetadata.For(sobjectType);

        // RecordTypeId is set from the class's record type; a standard lookup the class doesn't declare (Contact.AccountId)
        // is left alone
        meta.Fields.Append(meta.ExternalIdField).Append(SObjectSchema.RecordTypeIdField)
            .Should().BeEquivalentTo(FakeSchema.Objects[meta.SObject]
                .Where(f => (f.Createable || f.Updateable) && (f.Type != "reference" || f.Name == SObjectSchema.RecordTypeIdField))
                .Select(f => f.Name));
    }

    [Fact]
    public void A_class_declared_wrongly_fails_when_it_is_first_used()
    {
        var noName = () => SObjectRecord.From("x", new NoFieldName { ExternalId = "1" });
        var noExternalId = () => SObjectRecord.From("x", new NoExternalId());
        var notMarked = () => SObjectRecord.From("x", new NotMarked());
        var noRecordType = () => SObjectRecord.From("x", new NoRecordType { ExternalId = "1" });

        noName.Should().Throw<TypeInitializationException>().WithInnerException<InvalidOperationException>().WithMessage("*Oops*JsonPropertyName*");
        noExternalId.Should().Throw<TypeInitializationException>().WithInnerException<InvalidOperationException>().WithMessage("*Id__c*");
        notMarked.Should().Throw<TypeInitializationException>().WithInnerException<InvalidOperationException>().WithMessage("*[SObject*");
        noRecordType.Should().Throw<TypeInitializationException>().WithInnerException<InvalidOperationException>()
            .WithMessage("*NoRecordType*RecordType = *");
    }

    [SObject("X__c", ExternalIdField = "Id__c", RecordType = "X")]
    private sealed class NoFieldName
    {
        [JsonPropertyName("Id__c")] public string? ExternalId { get; set; }
        public string? Oops { get; set; }
    }

    [SObject("X__c", ExternalIdField = "Id__c", RecordType = "X")]
    private sealed class NoExternalId
    {
        [JsonPropertyName("Name")] public string? Name { get; set; }
    }

    /// <summary>RecordType is a required member, so only a blank one compiles.</summary>
    [SObject("X__c", ExternalIdField = "Id__c", RecordType = " ")]
    private sealed class NoRecordType
    {
        [JsonPropertyName("Id__c")] public string? ExternalId { get; set; }
    }

    private sealed class NotMarked
    {
        [JsonPropertyName("Name")] public string? Name { get; set; }
    }
}