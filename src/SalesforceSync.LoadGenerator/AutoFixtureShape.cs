using System.Text.Json;
using System.Text.Json.Nodes;
using SalesforceSync.Sync;

namespace SalesforceSync.LoadGenerator;

/// <summary>
/// Records AutoFixture builds from the sync's event classes (<see cref="AccountPayload"/>, <see cref="ProfilePayload"/>,
/// <see cref="AccountRelationshipPayload"/>, <see cref="ContactPayload"/>): every property gets a new random value of
/// its type on every event, so the shape always matches the classes and every field changes. Only what must stay
/// consistent is set here: the ids, a relationship's related account, a contact's email.
/// <para>
/// Texts are 8 random characters: AutoFixture's default (property name + GUID, 40+ characters) is longer than
/// Salesforce allows for some fields (Phone and FirstName: 40). Picklist fields get random text too, which a real org
/// with restricted picklists would reject.
/// </para>
/// </summary>
public sealed class AutoFixtureShape
{
    private readonly Fixture _fixture = new();

    public AutoFixtureShape()
    {
        _fixture.Register(() => Guid.NewGuid().ToString("N")[..8]);
        // AutoFixture 4 can't build these on its own
        _fixture.Register(() => DateOnly.FromDateTime(_fixture.Create<DateTime>()));
        _fixture.Register<IReadOnlyList<string>>(() => [.. _fixture.CreateMany<string>(2)]);
    }

    public JsonObject Account(string key, string profileKey, JsonArray relationships, JsonArray contacts)
    {
        var account = _fixture.Create<AccountPayload>();
        account.ExternalId = key;
        account.Profile!.ExternalId = profileKey;
        var json = JsonSerializer.SerializeToNode(account, EventJson.Options)!.AsObject();
        json["accountRelationships"] = relationships;
        json["contacts"] = contacts;
        return json;
    }

    public JsonObject Relationship(string id, string related)
    {
        var relationship = _fixture.Create<AccountRelationshipPayload>();
        relationship.ExternalId = id;
        relationship.RelatedAccountExternalId = related;
        // The aggregate's account is the relationship's; the sync fills it in when it splits the aggregate
        relationship.AccountExternalId = null;
        return JsonSerializer.SerializeToNode(relationship, EventJson.Options)!.AsObject();
    }

    public JsonObject Contact(string id)
    {
        var contact = _fixture.Create<ContactPayload>();
        contact.ExternalId = id;
        contact.AccountExternalId = null;
        contact.Email = $"{id.ToLowerInvariant()}@example.com";
        return JsonSerializer.SerializeToNode(contact, EventJson.Options)!.AsObject();
    }
}
