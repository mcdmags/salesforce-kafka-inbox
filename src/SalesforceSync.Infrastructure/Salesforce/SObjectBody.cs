namespace SalesforceSync.Infrastructure.Salesforce;

/// <summary>A record as an upsert takes it: <c>{ "attributes": { "type": ... }, field: value, ... }</c>, keyed by field API name.</summary>
public sealed class SObjectBody : Dictionary<string, object?>
{
    public SObjectBody(string type)
    {
        this["attributes"] = new Dictionary<string, object?> { ["type"] = type };
    }
}
