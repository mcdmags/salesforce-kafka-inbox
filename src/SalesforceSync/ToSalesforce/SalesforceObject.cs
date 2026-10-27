using System.Reflection;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// A Salesforce object and the external-id field its records are matched by. Each object has its own: Salesforce sets
/// no common name, so an org may use e.g. <c>Legacy_Id__c</c> on Account and <c>Source_Id__c</c> on Contact. Declared
/// on each record class (<see cref="SObjectAttribute"/>) and read with <see cref="Of{T}"/>.
/// </summary>
/// <param name="Name">Object API name, e.g. <c>Account</c>.</param>
/// <param name="ExternalIdField">API name of its external-id field, e.g. <c>ExternalId__c</c>.</param>
public sealed record SalesforceObject(string Name, string ExternalIdField)
{
    /// <summary>The object the record class <typeparamref name="T"/> declares.</summary>
    public static SalesforceObject Of<T>()
    {
        var sobject = typeof(T).GetCustomAttribute<SObjectAttribute>()
                      ?? throw new InvalidOperationException($"{typeof(T).Name} has no [SObject]");
        return new SalesforceObject(sobject.Name, sobject.ExternalIdField);
    }
}
