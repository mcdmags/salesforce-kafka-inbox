using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json.Serialization;

namespace SalesforceSync.ToSalesforce;

/// <summary>
/// A record class (<see cref="AccountRecord"/>, ...) as the fields a sync sends: API name to value. A dictionary, not
/// JSON, because <see cref="SalesforceCalls.ReadAsync"/> selects its keys and <see cref="SalesforceCalls.Changes"/>
/// compares its CLR values (decimal, DateOnly, ...) with what Salesforce returned.
/// </summary>
public static class SalesforceFields
{
    /// <summary>Every property of the record by its API name, null values included: null clears the field.</summary>
    public static Dictionary<string, object?> Of<T>(T record)
        where T : class
    {
        return Properties<T>.All.ToDictionary(p => p.Name, p => p.Get(record));
    }

    /// <summary>One property's API name: <c>Name&lt;ContactRecord&gt;(c => c.AccountId)</c> is <c>AccountId__c</c>.</summary>
    public static string Name<T>(Expression<Func<T, object?>> property)
    {
        var body = property.Body is UnaryExpression { NodeType: ExpressionType.Convert } convert ? convert.Operand : property.Body;
        return ApiName((PropertyInfo)((MemberExpression)body).Member);
    }

    private static string ApiName(PropertyInfo property)
    {
        return property.GetCustomAttribute<JsonPropertyNameAttribute>()?.Name ?? property.Name;
    }

    // Per record class, once: its properties with compiled getters (a sync builds one record per event)
    private static class Properties<T>
    {
        public static readonly (string Name, Func<T, object?> Get)[] All = [.. typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Select(p => (ApiName(p), Getter(p)))];

        private static Func<T, object?> Getter(PropertyInfo property)
        {
            var record = Expression.Parameter(typeof(T));
            return Expression.Lambda<Func<T, object?>>(Expression.Convert(Expression.Property(record, property), typeof(object)), record).Compile();
        }
    }
}
