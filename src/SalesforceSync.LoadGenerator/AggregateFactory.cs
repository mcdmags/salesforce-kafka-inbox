using System.Collections;
using System.Reflection;
using AutoFixture.Kernel;
using SalesforceSync.Sync;

namespace SalesforceSync.LoadGenerator;

/// <summary>
/// Builds the records of each published aggregate: a copy of the template's (<see cref="AggregateTemplate"/>), where
/// AutoFixture gives every filled-in property a new value of its type, so every change changes every field. Kept as in
/// the template: properties it leaves empty (null stays null), lists, and <see cref="AggregateShape.Kept"/>. Then the
/// ids, links and version markers are set through <see cref="AggregateShape"/>.
/// <para>
/// Values come from one seeded sequence, so the same seed and calls give the same records (every benchmark iteration
/// gets the same workload). Texts are 8 characters: AutoFixture's default (property name + GUID, 40+ characters) is
/// longer than Salesforce allows for some fields (Phone and FirstName: 40).
/// </para>
/// </summary>
public sealed class AggregateFactory
{
    private static readonly DateTime FirstDay = new(2015, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private readonly AggregateTemplate _template;
    private readonly Fixture _fixture = new();
    private readonly SpecimenContext _values;
    private readonly Random _random;

    public AggregateFactory(AggregateTemplate template, int seed)
    {
        _template = template;
        _random = new Random(seed);
        _fixture.Register(() => Text(8));
        _fixture.Register(() => _random.Next(1, 10_000));
        _fixture.Register(() => (long)_random.Next(1, 10_000));
        _fixture.Register(() => _random.Next(1, 10_000_000) / 100m);
        _fixture.Register(() => Math.Round(_random.NextDouble() * 1_000, 2));
        _fixture.Register(() => _random.Next(2) == 1);
        _fixture.Register(() => FirstDay.AddSeconds(_random.Next(0, 10 * 365 * 24 * 3600)));
        _fixture.Register(() => new DateTimeOffset(_fixture.Create<DateTime>()));
        _fixture.Register(() => DateOnly.FromDateTime(_fixture.Create<DateTime>()));
        _values = new SpecimenContext(_fixture);
    }

    /// <summary>The account at <paramref name="version"/>, with its profile and these children.</summary>
    public AccountSnapshot Account(string key, string profileKey, long version, List<AccountRelationshipPayload?> relationships, List<ContactPayload?> contacts)
    {
        var account = Vary(_template.Account(), key);
        AggregateShape.AccountId.Set(account, key);
        AggregateShape.AccountVersion.SetVersion(account, version);
        var profile = AggregateShape.Profile.Get(account)!;
        AggregateShape.ProfileId.Set(profile, profileKey);
        AggregateShape.ProfileVersion.SetVersion(profile, version);
        AggregateShape.Relationships.Set(account, relationships);
        AggregateShape.Contacts.Set(account, contacts);
        return account;
    }

    /// <summary>A relationship at <paramref name="version"/> naming <paramref name="related"/>.</summary>
    public AccountRelationshipPayload Relationship(string id, string related, long version)
    {
        var relationship = Vary(_template.Relationship() ?? _fixture.Create<AccountRelationshipPayload>(), id);
        AggregateShape.RelationshipId.Set(relationship, id);
        AggregateShape.RelatedAccount.Set(relationship, related);
        // The aggregate's account is the relationship's; the sync fills it in when it splits the aggregate
        AggregateShape.RelationshipAccount.Set(relationship, null);
        AggregateShape.RelationshipVersion.SetVersion(relationship, version);
        return relationship;
    }

    /// <summary>A contact at <paramref name="version"/>.</summary>
    public ContactPayload Contact(string id, long version)
    {
        var contact = Vary(_template.Contact() ?? _fixture.Create<ContactPayload>(), id);
        AggregateShape.ContactId.Set(contact, id);
        AggregateShape.ContactAccount.Set(contact, null);
        AggregateShape.ContactVersion.SetVersion(contact, version);
        return contact;
    }

    /// <summary>New values for the template's filled-in properties, at every level; see the class summary for what is kept.</summary>
    private T Vary<T>(T item, string key)
        where T : notnull
    {
        foreach (var property in item.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.CanWrite))
        {
            var value = property.GetValue(item);
            if (value is null || AggregateShape.IsStructure(property))
            {
                continue;
            }

            if (AggregateShape.IsKept(property))
            {
                if (value is string text)
                {
                    property.SetValue(item, text.Replace("{key}", key, StringComparison.Ordinal));
                }
            }
            else if (value is IEnumerable and not string)
            {
                // A list keeps the template's items
            }
            else if (value.GetType() is { IsClass: true } nested && nested != typeof(string))
            {
                Vary(value, key);
            }
            else
            {
                property.SetValue(item, _values.Resolve(Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType));
            }
        }

        return item;
    }

    private string Text(int length)
    {
        const string Letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        return string.Create(length, _random, (chars, random) =>
        {
            for (var i = 0; i < chars.Length; i++)
            {
                chars[i] = Letters[random.Next(Letters.Length)];
            }
        });
    }
}
