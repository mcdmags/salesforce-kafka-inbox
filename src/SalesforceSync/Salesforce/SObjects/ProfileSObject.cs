using System.Text.Json.Serialization;

namespace SalesforceSync.Salesforce.SObjects;

/// <summary>Profile__c, the detail record of an Account (complete desired state, like <see cref="AccountSObject"/>).</summary>
[SObject("Profile__c", ExternalIdField = "External_Id__c")]
public sealed class ProfileSObject
{
    [JsonPropertyName("External_Id__c")] public string? ExternalId { get; set; }
    [JsonPropertyName("Name")] public string? Name { get; set; }

    /// <summary>The Account this Profile belongs to, by the Account's external id.</summary>
    [JsonPropertyName("Account__c"), SObjectLookup(typeof(AccountSObject))]
    public string? AccountExternalId { get; set; }

    [JsonPropertyName("Tier__c")] public string? Tier { get; set; }
    [JsonPropertyName("Score__c")] public decimal? Score { get; set; }
    [JsonPropertyName("Discount__c")] public decimal? Discount { get; set; }
    [JsonPropertyName("Notes__c")] public string? Notes { get; set; }
    [JsonPropertyName("Opt_In__c")] public bool? OptIn { get; set; }
    [JsonPropertyName("Start_Date__c")] public DateOnly? StartDate { get; set; }
}