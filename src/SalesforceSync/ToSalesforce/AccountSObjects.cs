using System.Text.Json.Serialization;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// The Salesforce Account as this sync writes it. An instance is the record's complete desired state: a null
/// property means "empty", and the diff clears the field if Salesforce has a value.
/// </summary>
[SObject("Account", ExternalIdField = "ExternalId__c", RecordType = "Business")]
public sealed class AccountSObject
{
    [JsonPropertyName("ExternalId__c")] public string? ExternalId { get; set; }
    [JsonPropertyName("Name")] public string? Name { get; set; }
    [JsonPropertyName("Rating")] public string? Rating { get; set; }
    [JsonPropertyName("Industry")] public string? Industry { get; set; }
    [JsonPropertyName("Description")] public string? Description { get; set; }
    [JsonPropertyName("AnnualRevenue")] public decimal? AnnualRevenue { get; set; }
    [JsonPropertyName("NumberOfEmployees")] public int? NumberOfEmployees { get; set; }
    [JsonPropertyName("Phone")] public string? Phone { get; set; }
    [JsonPropertyName("Website")] public string? Website { get; set; }
    [JsonPropertyName("Active__c")] public bool? Active { get; set; }

    /// <summary>Multi-select picklist: the selected values separated by ';'.</summary>
    [JsonPropertyName("Tags__c")] public string? Tags { get; set; }

    [JsonPropertyName("Customer_Since__c")] public DateOnly? CustomerSince { get; set; }
    [JsonPropertyName("Last_Contacted__c")] public DateTimeOffset? LastContacted { get; set; }
}

/// <summary>Profile__c, the detail record of an Account (complete desired state, like <see cref="AccountSObject"/>).</summary>
[SObject("Profile__c", ExternalIdField = "ExternalId__c", RecordType = "Standard")]
public sealed class ProfileSObject
{
    [JsonPropertyName("ExternalId__c")] public string? ExternalId { get; set; }
    [JsonPropertyName("Name")] public string? Name { get; set; }

    /// <summary>
    /// The Account this Profile belongs to: a Text field holding the Account's external id. The Account is in the same
    /// unit, so its write goes first and no query checks it.
    /// </summary>
    [JsonPropertyName("AccountId__c"), SObjectForeignKey(typeof(AccountSObject))]
    public string? AccountId { get; set; }

    [JsonPropertyName("Tier__c")] public string? Tier { get; set; }
    [JsonPropertyName("Score__c")] public decimal? Score { get; set; }
    [JsonPropertyName("Discount__c")] public decimal? Discount { get; set; }
    [JsonPropertyName("Notes__c")] public string? Notes { get; set; }
    [JsonPropertyName("Opt_In__c")] public bool? OptIn { get; set; }
    [JsonPropertyName("Start_Date__c")] public DateOnly? StartDate { get; set; }
}
