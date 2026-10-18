namespace SalesforceSync.Infrastructure.Salesforce;

/// <summary>
/// A record as an upsert takes it: <c>{ "attributes": { "type": ... }, field: value, ... }</c>. A dictionary of the fields
/// by API name, so it serializes as the JSON object Salesforce expects.
/// </summary>
public sealed class SObjectBody : Dictionary<string, object?>
{
    public SObjectBody(string type)
    {
        this["attributes"] = new Dictionary<string, object?> { ["type"] = type };
    }
}
