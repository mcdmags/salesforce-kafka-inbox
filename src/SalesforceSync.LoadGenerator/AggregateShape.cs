using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using SalesforceSync.Sync;

namespace SalesforceSync.LoadGenerator;

/// <summary>
/// The one place the load generator and the benchmarks know the aggregate's structure: which property of the sync's
/// event classes is each entity's id, each foreign key, each child list, and the version marker the benchmarks' check
/// reads back. Everything else reaches the payloads through these, so a property renamed, removed or retyped in the
/// event classes breaks the build here, and nothing else spells a JSON field name.
/// <para>
/// For another structure: change the event classes, then fix what no longer compiles here (and in
/// <c>AggregateFactory</c>, which builds a snapshot from these).
/// </para>
/// </summary>
public static class AggregateShape
{
    // ---------- Ids and links ----------

    public static readonly Field<AccountPayload, string?> AccountId = new(a => a.ExternalId);

    /// <summary>The account's own object (the profile): written with the account, its marker carries the account's version.</summary>
    public static readonly Field<AccountPayload, ProfilePayload?> Profile = new(a => a.Profile);

    public static readonly Field<ProfilePayload, string?> ProfileId = new(p => p.ExternalId);

    public static readonly Field<AccountSnapshot, List<AccountRelationshipPayload?>?> Relationships = new(s => s.AccountRelationships);
    public static readonly Field<AccountRelationshipPayload, string?> RelationshipId = new(r => r.ExternalId);

    /// <summary>The relationship's own account: left empty in the aggregate, the sync fills it in when it splits it.</summary>
    public static readonly Field<AccountRelationshipPayload, string?> RelationshipAccount = new(r => r.AccountExternalId);

    /// <summary>The other account a relationship names.</summary>
    public static readonly Field<AccountRelationshipPayload, string?> RelatedAccount = new(r => r.RelatedAccountExternalId);

    public static readonly Field<AccountSnapshot, List<ContactPayload?>?> Contacts = new(s => s.Contacts);
    public static readonly Field<ContactPayload, string?> ContactId = new(c => c.ExternalId);

    /// <summary>The contact's account: left empty in the aggregate, the sync fills it in.</summary>
    public static readonly Field<ContactPayload, string?> ContactAccount = new(c => c.AccountExternalId);

    // ---------- Version markers ----------
    // Each entity's version, set on every change (text: "vN", number: N). The benchmarks' check reads them back from
    // the org, so each must be a property the sync sends to Salesforce.

    public static readonly Field<AccountPayload, string?> AccountVersion = new(a => a.Description);
    public static readonly Field<ProfilePayload, decimal?> ProfileVersion = new(p => p.Score);
    public static readonly Field<AccountRelationshipPayload, string?> RelationshipVersion = new(r => r.Name);
    public static readonly Field<ContactPayload, string?> ContactVersion = new(c => c.LastName);

    // ---------- Kept from the template ----------

    /// <summary>
    /// Properties every event keeps as the template has them ({key} replaced by the entity's id), instead of a new
    /// AutoFixture value: picklists, which a real org restricts to its own values, and the contact's email, which
    /// Salesforce checks is an address.
    /// </summary>
    public static readonly PropertyInfo[] Kept =
    [
        Field<AccountPayload, string?>.Of(a => a.Rating),
        Field<AccountPayload, string?>.Of(a => a.Industry),
        Field<ProfilePayload, string?>.Of(p => p.Tier),
        Field<AccountRelationshipPayload, string?>.Of(r => r.Type),
        Field<ContactPayload, string?>.Of(c => c.Email)
    ];

    /// <summary>Properties AutoFixture never fills: the ids, links and child lists above (set by the factory).</summary>
    public static readonly PropertyInfo[] Structure =
    [
        AccountId.Property, ProfileId.Property, Relationships.Property, RelationshipId.Property, RelationshipAccount.Property,
        RelatedAccount.Property, Contacts.Property, ContactId.Property, ContactAccount.Property
    ];

    public static bool IsKept(PropertyInfo property)
    {
        return Kept.Any(p => Same(p, property));
    }

    public static bool IsStructure(PropertyInfo property)
    {
        return Structure.Any(p => Same(p, property));
    }

    // Same property whichever class it was found through (AccountSnapshot's ExternalId is AccountPayload's)
    private static bool Same(PropertyInfo a, PropertyInfo b)
    {
        return a.MetadataToken == b.MetadataToken && a.Module == b.Module;
    }
}

/// <summary>A property of an event class, read and written through compiled accessors.</summary>
public sealed class Field<T, TValue>
{
    private readonly Func<T, TValue> _get;
    private readonly Action<T, TValue> _set;

    public Field(Expression<Func<T, TValue>> property)
    {
        Property = Of(property);
        _get = property.Compile();
        var value = Expression.Parameter(typeof(TValue));
        _set = Expression.Lambda<Action<T, TValue>>(Expression.Assign(Strip(property.Body), value), property.Parameters[0], value).Compile();
    }

    public PropertyInfo Property { get; }

    public TValue Get(T item)
    {
        return _get(item);
    }

    public void Set(T item, TValue value)
    {
        _set(item, value);
    }

    /// <summary>Writes <paramref name="version"/> as this marker's type has it: "vN" in text, N in a number.</summary>
    public void SetVersion(T item, long version)
    {
        var type = Nullable.GetUnderlyingType(typeof(TValue)) ?? typeof(TValue);
        Set(item, (TValue)(type == typeof(string) ? $"v{version}" : Convert.ChangeType(version, type, CultureInfo.InvariantCulture)));
    }

    /// <summary>The version <see cref="SetVersion"/> wrote; null if there is none.</summary>
    public long? Version(T item)
    {
        return Get(item) switch
        {
            null => null,
            string text => text.StartsWith('v') && long.TryParse(text[1..], out var v) ? v : null,
            var number => Convert.ToInt64(number, CultureInfo.InvariantCulture)
        };
    }

    public static PropertyInfo Of(Expression<Func<T, TValue>> property)
    {
        return Strip(property.Body).Member as PropertyInfo
               ?? throw new ArgumentException($"{property} is not a property", nameof(property));
    }

    // A value-type property arrives wrapped in a conversion
    private static MemberExpression Strip(Expression body)
    {
        return (body is UnaryExpression { NodeType: ExpressionType.Convert } convert ? convert.Operand : body) as MemberExpression
               ?? throw new ArgumentException($"{body} is not a property");
    }
}
