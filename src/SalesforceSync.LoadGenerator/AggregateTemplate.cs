using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using SalesforceSync.Sync;

namespace SalesforceSync.LoadGenerator;

/// <summary>
/// A real event of yours, as the base of every aggregate the load generator and the benchmarks publish
/// (<see cref="AggregateFactory"/>). Required: templates/account-event.json, copied next to each program
/// (<see cref="DefaultPath"/>); the load generator's <c>LoadGenerator:Template</c> or the benchmarks'
/// <c>--template my.json</c> (env var <see cref="Variable"/>) names another file.
/// <para>
/// The file is a whole aggregate message, <c>{ "meta", "before", "after" }</c>; its <c>after</c> (or <c>before</c>) is
/// the base. Its first relationship and first contact are the base of every child; a list without one gets children
/// built by AutoFixture. It is read into the sync's event classes, so a file that doesn't fit them is refused at
/// startup, every problem listed: a property the classes lack (a typo, or a field your classes don't have yet), a
/// value of the wrong type, no profile.
/// </para>
/// </summary>
public sealed class AggregateTemplate
{
    /// <summary>Env var naming the template file (child benchmark processes inherit it).</summary>
    public const string Variable = "SFSYNC_BENCH_TEMPLATE";

    public const string FileName = "account-event.json";

    /// <summary>templates/account-event.json as the build copies it next to the program.</summary>
    public static string DefaultPath => Path.Combine(AppContext.BaseDirectory, FileName);

    private static readonly Lazy<AggregateTemplate> FromEnvironment = new(() =>
        Load(Environment.GetEnvironmentVariable(Variable) is { Length: > 0 } path ? path : DefaultPath));

    // Kept as JSON: every use deserializes its own copy
    private readonly string _account;
    private readonly string? _relationship;
    private readonly string? _contact;

    private AggregateTemplate(AccountSnapshot account)
    {
        _relationship = Child(AggregateShape.Relationships.Get(account));
        _contact = Child(AggregateShape.Contacts.Get(account));
        AggregateShape.Relationships.Set(account, []);
        AggregateShape.Contacts.Set(account, []);
        _account = JsonSerializer.Serialize(account, EventJson.Options);
    }

    /// <summary>The run's template: the file named by <see cref="Variable"/>, else <see cref="DefaultPath"/>.</summary>
    public static AggregateTemplate Current => FromEnvironment.Value;

    /// <exception cref="InvalidDataException">The file is missing or doesn't fit the event classes.</exception>
    public static AggregateTemplate Load(string path)
    {
        return File.Exists(path)
            ? Parse(File.ReadAllText(path), path)
            : throw new InvalidDataException($"The aggregate template {path} doesn't exist: start from templates/{FileName}, a real event of yours");
    }

    /// <exception cref="InvalidDataException">The JSON doesn't fit the event classes; one problem per line.</exception>
    public static AggregateTemplate Parse(string json, string source)
    {
        JsonObject? root;
        try
        {
            root = JsonNode.Parse(json) as JsonObject;
        }
        catch (JsonException ex)
        {
            throw Refused(source, [$"not valid JSON: {ex.Message}"]);
        }

        if (root is null)
        {
            throw Refused(source, ["the JSON is not an object"]);
        }

        var problems = new List<string>();
        CheckShape(root, typeof(AccountAggregateEvent), "", problems);
        AccountAggregateEvent? aggregate = null;
        try
        {
            aggregate = root.Deserialize<AccountAggregateEvent>(EventJson.Options);
        }
        catch (JsonException ex)
        {
            problems.Add($"it is not an aggregate the sync can read: {ex.Message}");
        }

        var account = aggregate?.After ?? aggregate?.Before;
        if (aggregate is not null && account is null)
        {
            problems.Add($"there is no account: the message needs an \"{JsonName(Property<AccountAggregateEvent>(nameof(AccountAggregateEvent.After)))}\" object");
        }
        else if (account is not null && AggregateShape.Profile.Get(account) is null)
        {
            problems.Add($"{JsonName(AggregateShape.Profile.Property)}: needs an object: its {JsonName(AggregateShape.ProfileVersion.Property)} carries the account's version");
        }

        if (problems.Count > 0)
        {
            throw Refused(source, problems);
        }

        var template = new AggregateTemplate(account!);
        // What the consumer makes of an aggregate built on it: one the sync reads
        var factory = new AggregateFactory(template, seed: 0);
        var sample = new AccountAggregateEvent(EventMeta.Of(EventType.Inserted, new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)), null,
            factory.Account("A1", "P1", 1, [factory.Relationship("R1", "A2", 1)], [factory.Contact("C1", 1)]));
        return new AccountAggregate().TryRead(JsonSerializer.Serialize(sample, EventJson.Options), out _, out var problem)
            ? template
            : throw Refused(source, [$"it is not an aggregate the sync can read: {problem}"]);
    }

    /// <summary>A new copy of the account (with its profile), without children.</summary>
    public AccountSnapshot Account()
    {
        return JsonSerializer.Deserialize<AccountSnapshot>(_account, EventJson.Options)!;
    }

    /// <summary>A new copy of the first relationship; null if the template has none.</summary>
    public AccountRelationshipPayload? Relationship()
    {
        return _relationship is null ? null : JsonSerializer.Deserialize<AccountRelationshipPayload>(_relationship, EventJson.Options);
    }

    /// <summary>A new copy of the first contact; null if the template has none.</summary>
    public ContactPayload? Contact()
    {
        return _contact is null ? null : JsonSerializer.Deserialize<ContactPayload>(_contact, EventJson.Options);
    }

    /// <summary>The property's name in the event JSON.</summary>
    public static string JsonName(PropertyInfo property)
    {
        return property.GetCustomAttribute<JsonPropertyNameAttribute>()?.Name
               ?? EventJson.Options.PropertyNamingPolicy?.ConvertName(property.Name)
               ?? property.Name;
    }

    private static string? Child<T>(List<T?>? children)
        where T : class
    {
        return children?.FirstOrDefault() is { } first ? JsonSerializer.Serialize(first, EventJson.Options) : null;
    }

    private static PropertyInfo Property<T>(string name)
    {
        return typeof(T).GetProperty(name)!;
    }

    /// <summary>Every property of the file exists on the event class (case-insensitive, as the reader matches), at every level.</summary>
    private static void CheckShape(JsonNode? node, Type type, string path, List<string> problems)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        if (node is JsonArray items)
        {
            var element = type.IsGenericType ? type.GetGenericArguments()[0] : type;
            for (var i = 0; i < items.Count; i++)
            {
                CheckShape(items[i], element, $"{path}[{i}]", problems);
            }
        }
        else if (node is JsonObject obj)
        {
            foreach (var (name, value) in obj)
            {
                var at = path.Length == 0 ? name : $"{path}.{name}";
                var property = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .FirstOrDefault(p => JsonName(p).Equals(name, StringComparison.OrdinalIgnoreCase));
                if (property is null)
                {
                    problems.Add($"{at}: {type.Name} has no such property");
                }
                else
                {
                    CheckShape(value, property.PropertyType, at, problems);
                }
            }
        }
    }

    private static InvalidDataException Refused(string source, IEnumerable<string> problems)
    {
        return new InvalidDataException($"The aggregate template {source} doesn't conform:{Environment.NewLine}  - " +
                                        string.Join($"{Environment.NewLine}  - ", problems));
    }
}
