using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json.Serialization;

namespace SalesforceSync.Salesforce;

/// <summary>
/// What the attributes of an sObject class say: the object, its external id field, its record type, its fields, its
/// lookups and its foreign keys.
/// Read by reflection once per class (see <see cref="SObjectMetadata{TSObject}"/>).
/// </summary>
public abstract class SObjectMetadata
{
    /// <summary>The object's API name.</summary>
    public required string SObject { get; init; }

    public required string ExternalIdField { get; init; }

    /// <summary>Developer name of the record type of the class's records.</summary>
    public required string RecordType { get; init; }

    /// <summary>API names of the value fields (everything except the external id and the lookups; foreign keys included).</summary>
    public required IReadOnlyList<string> Fields { get; init; }

    public required IReadOnlyList<SObjectLookupField> Lookups { get; init; }

    /// <summary>The Text fields that hold a parent's external id (<see cref="SObjectForeignKeyAttribute"/>), also in <see cref="Fields"/>.</summary>
    public required IReadOnlyList<SObjectLookupField> ForeignKeys { get; init; }

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

    private protected static SObjectAttribute ObjectOf(Type type)
    {
        return type.GetCustomAttribute<SObjectAttribute>()
               ?? throw new InvalidOperationException($"{type.Name} is not marked [SObject(...)]");
    }

    /// <summary><paramref name="field"/> pointing at the object of the sObject class <paramref name="target"/>, by its external id field.</summary>
    private protected static SObjectLookupField PointerTo(string field, Type target)
    {
        var sobject = ObjectOf(target);
        return new SObjectLookupField(field, sobject.Name, sobject.ExternalIdField);
    }
}

/// <summary>
/// <see cref="SObjectMetadata"/> of <typeparamref name="TSObject"/> plus compiled property getters. Built once per
/// class (the static <see cref="Instance"/>), so turning an instance into a record costs no reflection. A class that
/// is declared wrongly (a property without <c>[JsonPropertyName]</c>, no external id property, no record type, a lookup
/// or foreign key that isn't a string) fails here, the first time the class is used, rather than per event.
/// </summary>
public sealed class SObjectMetadata<TSObject> : SObjectMetadata where TSObject : class
{
    public static SObjectMetadata<TSObject> Instance { get; } = Build();

    private readonly Func<TSObject, object?> _externalId;
    private readonly (string Name, Func<TSObject, object?> Get)[] _fields;
    private readonly (SObjectLookupField Lookup, Func<TSObject, object?> Get)[] _lookups;
    private readonly (SObjectLookupField Key, Func<TSObject, object?> Get)[] _foreignKeys;

    private SObjectMetadata(Func<TSObject, object?> externalId, (string, Func<TSObject, object?>)[] fields,
        (SObjectLookupField, Func<TSObject, object?>)[] lookups, (SObjectLookupField, Func<TSObject, object?>)[] foreignKeys)
    {
        _externalId = externalId;
        _fields = fields;
        _lookups = lookups;
        _foreignKeys = foreignKeys;
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
        return _lookups.Select(l => (l.Lookup, (string?)l.Get(sobject)));
    }

    /// <summary>Every foreign key with the external id it holds (null if not set).</summary>
    public IEnumerable<(SObjectLookupField Key, string? ExternalId)> ForeignKeysOf(TSObject sobject)
    {
        return _foreignKeys.Select(k => (k.Key, (string?)k.Get(sobject)));
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
        var foreignKeys = new List<(SObjectLookupField, Func<TSObject, object?>)>();

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
                lookups.Add((PointerTo(name, lookup.Target), getter));
            }
            else if (isExternalId)
            {
                RequireString(property, "the external id");
                externalId = getter;
            }
            else
            {
                if (property.GetCustomAttribute<SObjectForeignKeyAttribute>() is { } key)
                {
                    RequireString(property, "a foreign key holds the parent's external id");
                    foreignKeys.Add((PointerTo(name, key.Parent), getter));
                }

                fields.Add((name, getter));
            }
        }

        return externalId is null
            ? throw new InvalidOperationException(
                $"{type.Name} has no property named [JsonPropertyName(\"{sobject.ExternalIdField}\")] (its external id field)")
            : new SObjectMetadata<TSObject>(externalId, [.. fields], [.. lookups], [.. foreignKeys])
            {
                SObject = sobject.Name,
                ExternalIdField = sobject.ExternalIdField,
                RecordType = sobject.RecordType,
                Fields = [.. fields.Select(f => f.Item1)],
                Lookups = [.. lookups.Select(l => l.Item1)],
                ForeignKeys = [.. foreignKeys.Select(k => k.Item1)]
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

/// <summary>
/// Marks a class as a Salesforce object: its API name, the unique external id field records are matched by, and the
/// record type every record is written with. Every property of the class names its field with
/// <c>[JsonPropertyName]</c>, so Salesforce API names (also namespaced ones such as <c>LLC_BI__Active__c</c>) appear
/// only on these classes.
/// <code>[SObject("Account", ExternalIdField = "ExternalId__c", RecordType = "Business")]</code>
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class SObjectAttribute(string name) : Attribute
{
    /// <summary>The object's API name, e.g. <c>Account</c> or <c>Profile__c</c>.</summary>
    public string Name { get; } = name;

    /// <summary>API name of the external id field; the class must have a string property with that name.</summary>
    public required string ExternalIdField { get; init; }

    /// <summary>
    /// Developer name of the record type every record of the class has (the engine sets <c>RecordTypeId</c> to its Id,
    /// from Describe). It must exist in the org, be active and be assigned to the user the sync runs as.
    /// </summary>
    public required string RecordType { get; init; }
}

/// <summary>
/// On a string property: the field is a lookup (a reference field, holding a Salesforce Id), and the property holds
/// the external id of the record it points at (not a Salesforce Id; the engine resolves it: to the Id the read found,
/// or to <c>@{reference}</c> to a parent created in the same Composite request). The target is named by its sObject
/// class, which already knows its own API name and external id field. For a Text field that holds the parent's
/// external id itself, see <see cref="SObjectForeignKeyAttribute"/>.
/// <code>[JsonPropertyName("Parent__c"), SObjectLookup(typeof(ParentSObject))]
/// public string? ParentExternalId { get; set; }</code>
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class SObjectLookupAttribute(Type target) : Attribute
{
    /// <summary>The sObject class (marked <see cref="SObjectAttribute"/>) of the record the lookup points at.</summary>
    public Type Target { get; } = target;
}

/// <summary>
/// On a string property of a Text field (not a lookup): a foreign key, holding the external id of its parent record,
/// e.g. the Account's <c>ExternalId__c</c>. To normalization, the read, the diff and the write it is an ordinary
/// field: compared with the event's value and written as the text, never resolved to an Id, never a
/// <c>@{reference}</c>. What the attribute adds: the parent's write is sent before the child's (parents first: the
/// org rejects a child whose parent doesn't exist), and a parent that isn't written with the child must already be in
/// Salesforce, or the child waits for it (<see cref="SalesforceErrors.DependencyNotSynced"/>, nothing sent). The
/// strategy's queries must then read the parent (<c>SoqlQuery.LookupTargetsOf</c>). An empty value has no parent to
/// wait for.
/// <code>[JsonPropertyName("AccountId__c"), SObjectForeignKey(typeof(AccountSObject))]
/// public string? AccountId { get; set; }</code>
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class SObjectForeignKeyAttribute(Type parent) : Attribute
{
    /// <summary>The sObject class (marked <see cref="SObjectAttribute"/>) of the parent, whose external id the field holds.</summary>
    public Type Parent { get; } = parent;
}

/// <summary>A lookup or foreign key field of an sObject class and the object it points at.</summary>
public sealed record SObjectLookupField(string Field, string TargetSObject, string TargetExternalIdField);