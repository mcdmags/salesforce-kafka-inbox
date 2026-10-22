using System.Text.Json.Serialization;

namespace SalesforceSync.ToSalesforce;

// The Salesforce side of each sync: one class per object, one property per field the sync writes. A property's
// Salesforce API name is its own name, or [JsonPropertyName] where they differ (custom fields, __c). The syncs fill one
// from the event and send SalesforceFields.Of(record); no field name is written out anywhere else.

/// <summary>
/// The Account fields the sync writes for a person and a business alike. Its record type, set at create, makes the
/// Account a person account or a business account, and each takes its own name fields (<see cref="AccountRecord"/>,
/// <see cref="PersonAccountRecord"/>).
/// </summary>
public abstract class AccountFields
{
    public string? Rating { get; set; }
    public string? Industry { get; set; }
    public string? Description { get; set; }
    public decimal? AnnualRevenue { get; set; }
    public int? NumberOfEmployees { get; set; }
    public string? Phone { get; set; }
    public string? Website { get; set; }

    [JsonPropertyName("Active__c")]
    public bool? Active { get; set; }

    /// <summary>Multi-select picklist: the values separated by ';'.</summary>
    [JsonPropertyName("Tags__c")]
    public string? Tags { get; set; }

    [JsonPropertyName("Customer_Since__c")]
    public DateOnly? CustomerSince { get; set; }

    [JsonPropertyName("Last_Contacted__c")]
    public DateTimeOffset? LastContacted { get; set; }

    /// <summary>Set when the Account is created; Salesforce requires it then, and an update never changes it.</summary>
    [CreateOnly]
    public string? RecordTypeId { get; set; }
}

/// <summary>A business Account: its name is <c>Name</c>.</summary>
[SObject("Account", "ExternalId__c")]
public sealed class AccountRecord : AccountFields
{
    public string? Name { get; set; }
}

/// <summary>A person Account: first and last name, from which Salesforce sets <c>Name</c> (sending Name is refused).</summary>
[SObject("Account", "ExternalId__c")]
public sealed class PersonAccountRecord : AccountFields
{
    public string? FirstName { get; set; }
    public string? LastName { get; set; }
}

/// <summary>Account fields the sync reads but never writes.</summary>
public sealed class AccountReadFields
{
    /// <summary>Set by Salesforce from the record type: a person account, or a business account.</summary>
    public bool? IsPersonAccount { get; init; }
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
}

/// <summary>
/// A field sent only when the record is created: <see cref="SalesforceFields.Of{T}"/> leaves it out while it is null,
/// so it is neither read nor compared, and an update never clears it.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class CreateOnlyAttribute : Attribute;

/// <summary>The Salesforce object a record class is, and the external-id field its records are matched by.</summary>
/// <param name="name">Object API name, e.g. <c>Account</c>.</param>
/// <param name="externalIdField">API name of its external-id field, e.g. <c>ExternalId__c</c>.</param>
[AttributeUsage(AttributeTargets.Class)]
public sealed class SObjectAttribute(string name, string externalIdField) : Attribute
{
    public string Name { get; } = name;
    public string ExternalIdField { get; } = externalIdField;
}
