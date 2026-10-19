using System.Text.Json.Serialization;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.ToSalesforce;

/// <summary>The Salesforce Contact as this sync writes it (complete desired state, like <see cref="AccountSObject"/>).</summary>
[SObject("Contact", ExternalIdField = "ExternalId__c", RecordType = "Standard")]
public sealed class ContactSObject
{
    [JsonPropertyName("ExternalId__c")] public string? ExternalId { get; set; }

    /// <summary>The Account, by its external id (a Text field; the standard AccountId lookup is left alone).</summary>
    [JsonPropertyName("AccountId__c"), SObjectForeignKey(typeof(AccountSObject))]
    public string? AccountId { get; set; }

    [JsonPropertyName("FirstName")] public string? FirstName { get; set; }
    [JsonPropertyName("LastName")] public string? LastName { get; set; }
    [JsonPropertyName("Email")] public string? Email { get; set; }
    [JsonPropertyName("Phone")] public string? Phone { get; set; }
}
