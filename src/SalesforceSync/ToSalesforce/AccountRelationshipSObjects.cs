using System.Text.Json.Serialization;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.ToSalesforce;

/// <summary>Account_Relationship__c between two Accounts (complete desired state, like <see cref="AccountSObject"/>).</summary>
[SObject("Account_Relationship__c", ExternalIdField = "ExternalId__c", RecordType = "Standard")]
public sealed class AccountRelationshipSObject
{
    [JsonPropertyName("ExternalId__c")] public string? ExternalId { get; set; }
    [JsonPropertyName("Name")] public string? Name { get; set; }

    /// <summary>The two Accounts, by their external ids (Text fields).</summary>
    [JsonPropertyName("AccountId__c"), SObjectForeignKey(typeof(AccountSObject))]
    public string? AccountId { get; set; }

    [JsonPropertyName("RelatedAccountId__c"), SObjectForeignKey(typeof(AccountSObject))]
    public string? RelatedAccountId { get; set; }

    [JsonPropertyName("Type__c")] public string? Type { get; set; }
    [JsonPropertyName("Start_Date__c")] public DateOnly? StartDate { get; set; }
    [JsonPropertyName("Active__c")] public bool? Active { get; set; }
}
