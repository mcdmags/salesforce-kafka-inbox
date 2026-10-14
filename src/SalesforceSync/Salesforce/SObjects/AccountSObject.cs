using System.Text.Json.Serialization;

namespace SalesforceSync.Salesforce.SObjects;

/// <summary>
/// The Salesforce Account as this sync writes it. An instance is the record's complete desired state: a null
/// property means "empty", and the diff clears the field if Salesforce has a value.
/// </summary>
[SObject("Account", ExternalIdField = "External_Id__c")]
public sealed class AccountSObject
{
    [JsonPropertyName("External_Id__c")] public string? ExternalId { get; set; }
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
