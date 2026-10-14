using System.Text.Json.Serialization;

namespace SalesforceSync.Salesforce.SObjects;

/// <summary>Account_Relationship__c between two Accounts (complete desired state, like <see cref="AccountSObject"/>).</summary>
[SObject("Account_Relationship__c", ExternalIdField = "External_Id__c")]
public sealed class AccountRelationshipSObject
{
    [JsonPropertyName("External_Id__c")] public string? ExternalId { get; set; }
    [JsonPropertyName("Name")] public string? Name { get; set; }

    [JsonPropertyName("Account__c"), SObjectLookup(typeof(AccountSObject))]
    public string? AccountExternalId { get; set; }

    [JsonPropertyName("Related_Account__c"), SObjectLookup(typeof(AccountSObject))]
    public string? RelatedAccountExternalId { get; set; }

    [JsonPropertyName("Type__c")] public string? Type { get; set; }
    [JsonPropertyName("Start_Date__c")] public DateOnly? StartDate { get; set; }
    [JsonPropertyName("Active__c")] public bool? Active { get; set; }
}
