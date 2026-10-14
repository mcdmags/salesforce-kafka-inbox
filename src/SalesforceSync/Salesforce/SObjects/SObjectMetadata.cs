using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json.Serialization;

namespace SalesforceSync.Salesforce.SObjects;

/// <summary>
/// What the attributes of an sObject class say: the object, its external id field, its record type, its fields and
/// its lookups.
/// Read by reflection once per class (see <see cref="SObjectMetadata{TSObject}"/>).
/// </summary>
public abstract class SObjectMetadata
{
    /// <summary>The object's API name.</summary>
    public required string SObject { get; init; }

    public required string ExternalIdField { get; init; }

    /// <summary>Developer name of the record type of the class's records.</summary>
    public required string RecordType { get; init; }

    /// <summary>API names of the value fields (everything except the external id and the lookups).</summary>
    public required IReadOnlyList<string> Fields { get; init; }

    public required IReadOnlyList<SObjectLookupField> Lookups { get; init; }

    /// <summary>The metadata of an sObject class known only at run time (tests, diagnostics); not for the hot path.</summary>
    public static SObjectMetadata For(Type sobjectType)
    {
        return (SObjectMetadata)typeof(SObjectMetadata<>).MakeGenericType(sobjectType)
                .GetProperty(nameof(SObjectMetadata<object>.Instance))!.GetValue(null)!;
    }

    /// <summary>Every sObject class (marked <see cref="SObjectAttribute"/>) in <paramref name="assembly"/>.</summary>
    public static IReadOnlyList<Type> TypesIn(Assembly assembly)
    {
        return [.. assembly.GetTypes().Where(t => t.IsDefined(typeof(SObjectAttribute), false))];
    }

    internal static SObjectAttribute ObjectOf(Type type)
    {
        return type.GetCustomAttribute<SObjectAttribute>()
               ?? throw new InvalidOperationException($"{type.Name} is not marked [SObject(...)]");
    }
}

/// <summary>
/// <see cref="SObjectMetadata"/> of <typeparamref name="TSObject"/> plus compiled property getters. Built once per
/// class (the static <see cref="Instance"/>), so turning an instance into a record costs no reflection. A class that
/// is declared wrongly (a property without <c>[JsonPropertyName]</c>, no external id property, no record type, a lookup
/// that isn't a string) fails here, the first time the class is used, rather than per event.
/// </summary>
public sealed class SObjectMetadata<TSObject> : SObjectMetadata where TSObject : class
{
    public static SObjectMetadata<TSObject> Instance { get; } = Build();

    private readonly Func<TSObject, object?> _externalId;
    private readonly (string Name, Func<TSObject, object?> Get)[] _fields;
    private readonly (SObjectLookupField Lookup, Func<TSObject, object?> Get)[] _lookups;

    private SObjectMetadata(Func<TSObject, object?> externalId, (string, Func<TSObject, object?>)[] fields,
        (SObjectLookupField, Func<TSObject, object?>)[] lookups)
    {
        _externalId = externalId;
        _fields = fields;
        _lookups = lookups;
    }

    public string? ExternalIdOf(TSObject sobject)
    {
        return (string?)_externalId(sobject);
    }

    /// <summary>Every value field by API name, nulls included: the instance is the record's complete desired state.</summary>
    public Dictionary<string, object?> FieldsOf(TSObject sobject)
    {
        var values = new Dictionary<string, object?>(_fields.Length, StringComparer.OrdinalIgnoreCase);
        foreach (var (name, get) in _fields)
        {
            values[name] = get(sobject);
        }

        return values;
    }

    /// <summary>Every lookup with the external id it holds (null if not set).</summary>
    public IEnumerable<(SObjectLookupField Lookup, string? ExternalId)> LookupsOf(TSObject sobject)
    {
        foreach (var (lookup, get) in _lookups)
        {
            yield return (lookup, (string?)get(sobject));
        }
    }

    private static SObjectMetadata<TSObject> Build()
    {
        var type = typeof(TSObject);
        var sobject = ObjectOf(type);
        if (string.IsNullOrWhiteSpace(sobject.RecordType))
        {
            throw new InvalidOperationException($"{type.Name} needs [SObject(..., RecordType = \"<record type developer name>\")]");
        }

        Func<TSObject, object?>? externalId = null;
        var fields = new List<(string, Func<TSObject, object?>)>();
        var lookups = new List<(SObjectLookupField, Func<TSObject, object?>)>();

        foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            var name = property.GetCustomAttribute<JsonPropertyNameAttribute>()?.Name
                       ?? throw new InvalidOperationException(
                           $"{type.Name}.{property.Name} needs [JsonPropertyName(\"<Salesforce API name>\")]");
            var getter = Getter(property);
            var isExternalId = name.Equals(sobject.ExternalIdField, StringComparison.OrdinalIgnoreCase);

            if (property.GetCustomAttribute<SObjectLookupAttribute>() is { } lookup)
            {
                RequireString(property, "a lookup holds the target's external id");
                var target = ObjectOf(lookup.Target);
                lookups.Add((new SObjectLookupField(name, target.Name, target.ExternalIdField), getter));
            }
            else if (isExternalId)
            {
                RequireString(property, "the external id");
                externalId = getter;
            }
            else
            {
                fields.Add((name, getter));
            }
        }

        return externalId is null
            ? throw new InvalidOperationException(
                $"{type.Name} has no property named [JsonPropertyName(\"{sobject.ExternalIdField}\")] (its external id field)")
            : new SObjectMetadata<TSObject>(externalId, [.. fields], [.. lookups])
            {
                SObject = sobject.Name,
                ExternalIdField = sobject.ExternalIdField,
                RecordType = sobject.RecordType,
                Fields = [.. fields.Select(f => f.Item1)],
                Lookups = [.. lookups.Select(l => l.Item1)]
            };
    }

    /// <summary>sobject => (object?)sobject.Property, compiled.</summary>
    private static Func<TSObject, object?> Getter(PropertyInfo property)
    {
        var instance = Expression.Parameter(typeof(TSObject), "sobject");
        var value = Expression.Convert(Expression.Property(instance, property), typeof(object));
        return Expression.Lambda<Func<TSObject, object?>>(value, instance).Compile();
    }

    private static void RequireString(PropertyInfo property, string why)
    {
        if (property.PropertyType != typeof(string))
        {
            throw new InvalidOperationException($"{typeof(TSObject).Name}.{property.Name} must be a string: {why}");
        }
    }
}