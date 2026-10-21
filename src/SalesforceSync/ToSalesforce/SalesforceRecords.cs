using System.Text.Json.Serialization;

namespace SalesforceSync.ToSalesforce;

// The Salesforce side of each sync: one class per object, one property per field the sync writes. A property's
// Salesforce API name is its own name, or [JsonPropertyName] where they differ (custom fields, __c). The syncs fill one
// from the event and send SalesforceFields.Of(record); no field name is written out anywhere else.

/// <summary>The Account fields the sync writes.</summary>
[SObject("Account", "ExternalId__c")]
public sealed class AccountRecord
{
    public string? Name { get; init; }
    public string? Rating { get; init; }
    public string? Industry { get; init; }
    public string? Description { get; init; }
    public decimal? AnnualRevenue { get; init; }
    public int? NumberOfEmployees { get; init; }
    public string? Phone { get; init; }
    public string? Website { get; init; }

    [JsonPropertyName("Active__c")]
    public bool? Active { get; init; }

    /// <summary>Multi-select picklist: the values separated by ';'.</summary>
    [JsonPropertyName("Tags__c")]
    public string? Tags { get; init; }

    [JsonPropertyName("Customer_Since__c")]
    public DateOnly? CustomerSince { get; init; }

    [JsonPropertyName("Last_Contacted__c")]
    public DateTimeOffset? LastContacted { get; init; }

    public string? RecordTypeId { get; init; }
}

/// <summary>The account's Profile__c fields the sync writes. Names its Account by external id (Text foreign key).</summary>
[SObject("Profile__c", "ExternalId__c")]
public sealed class ProfileRecord
{
    public string? Name { get; init; }

    [JsonPropertyName("AccountId__c")]
    public string? AccountId { get; init; }

    [JsonPropertyName("Tier__c")]
    public string? Tier { get; init; }

    [JsonPropertyName("Score__c")]
    public decimal? Score { get; init; }

    [JsonPropertyName("Discount__c")]
    public decimal? Discount { get; init; }

    [JsonPropertyName("Notes__c")]
    public string? Notes { get; init; }

    [JsonPropertyName("Opt_In__c")]
    public bool? OptIn { get; init; }

    [JsonPropertyName("Start_Date__c")]
    public DateOnly? StartDate { get; init; }

    public string? RecordTypeId { get; init; }
}

/// <summary>The Account_Relationship__c fields the sync writes. Names both Accounts by external id (Text foreign keys).</summary>
[SObject("Account_Relationship__c", "ExternalId__c")]
public sealed class AccountRelationshipRecord
{
    public string? Name { get; init; }

    [JsonPropertyName("AccountId__c")]
    public string? AccountId { get; init; }

    [JsonPropertyName("RelatedAccountId__c")]
    public string? RelatedAccountId { get; init; }

    [JsonPropertyName("Type__c")]
    public string? Type { get; init; }

    [JsonPropertyName("Start_Date__c")]
    public DateOnly? StartDate { get; init; }

    [JsonPropertyName("Active__c")]
    public bool? Active { get; init; }

    public string? RecordTypeId { get; init; }
}

/// <summary>The Contact fields the sync writes. Names its Account by external id (Text foreign key).</summary>
[SObject("Contact", "ExternalId__c")]
public sealed class ContactRecord
{
    [JsonPropertyName("AccountId__c")]
    public string? AccountId { get; init; }

    public string? FirstName { get; init; }
    public string? LastName { get; init; }
    public string? Email { get; init; }
    public string? Phone { get; init; }
    public string? RecordTypeId { get; init; }
}

/// <summary>The Salesforce object a record class is, and the external-id field its records are matched by.</summary>
/// <param name="name">Object API name, e.g. <c>Account</c>.</param>
/// <param name="externalIdField">API name of its external-id field, e.g. <c>ExternalId__c</c>.</param>
[AttributeUsage(AttributeTargets.Class)]
public sealed class SObjectAttribute(string name, string externalIdField) : Attribute
{
    public string Name { get; } = name;
    public string ExternalIdField { get; } = externalIdField;
}
