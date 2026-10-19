using System.Globalization;
using Microsoft.EntityFrameworkCore;
using SalesforceSync.Data;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.Tests;

/// <summary>
/// The raw event format: what the Kafka consumer admits into the inbox, and the inbox's own rules.
/// </summary>
public sealed class InboxEventTests
{
    private static readonly EventReader<AccountEvent> Accounts = AccountEvent.Reader;

    private static readonly EventReader<AccountRelationshipEvent> Relationships = AccountRelationshipEvent.Reader;

    // ---------- What the consumer admits (the same typed read the engine uses) ----------

    [Theory]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03.456Z", "type": "updated" }, "account": { "externalId": "ACC-1" } }""", "ACC-1", 456)]
    [InlineData(/*lang=json,strict*/ """{ "account": { "externalId": "ACC-2" }, "meta": { "time": "2026-10-14T03:02:03+02:00", "type": "updated" } }""", "ACC-2", 0)]
    [InlineData(/*lang=json,strict*/ """{ "Meta": { "Time": "2026-10-14T01:02:03Z", "Type": "INSERTED" }, "Account": { "ExternalId": "ACC-3" }, "somethingElse": 1 }""", "ACC-3", 0)]
    public void Valid_event_yields_its_entity_id_and_time(string json, string entityId, int millisecond)
    {
        Accounts.TryRead(json, out var read, out var problem).Should().BeTrue("the event is valid, but: {0}", problem);
        read!.EntityId.Should().Be(entityId);
        read.Time.Should().Be(new DateTimeOffset(2026, 10, 14, 1, 2, 3, millisecond, TimeSpan.Zero), "it is the same instant");
    }

    [Theory]
    [InlineData("2026-10-14T01:02:03.1234567Z", "2026-10-14T01:02:03.123456Z")]
    [InlineData("2026-10-14T03:02:03.4569999+02:00", "2026-10-14T01:02:03.456999Z")]
    [InlineData("2026-10-14T01:02:03.123456951Z", "2026-10-14T01:02:03.123457Z")]
    public void An_event_time_is_stored_cut_to_the_microsecond_in_UTC(string time, string stored)
    {
        var read = DateTimeOffset.Parse(time, CultureInfo.InvariantCulture);

        var cut = InboxRow.ToWholeMicroseconds(read);

        cut.Should().Be(DateTimeOffset.Parse(stored, CultureInfo.InvariantCulture), "not rounded up as Postgres would");
        cut.Offset.Should().Be(TimeSpan.Zero, "Npgsql writes only UTC times to timestamptz");
    }

    [Theory]
    [InlineData("inserted")]
    [InlineData("updated")]
    [InlineData("Inserted")]
    [InlineData("UPDATED")]
    [InlineData("deleted")]
    [InlineData("Deleted")]
    public void Event_type_is_inserted_updated_or_deleted_in_any_case(string type)
    {
        var json = $$"""{ "meta": { "time": "2026-10-14T01:00:00Z", "type": "{{type}}" }, "account": { "externalId": "ACC-1" } }""";

        Accounts.TryRead(json, out _, out var problem).Should().BeTrue("the type is valid, but: {0}", problem);
    }

    [Theory]
    [InlineData("not json", "Not a valid account event")]
    [InlineData("[1, 2]", "Not a valid account event")]
    [InlineData("null", "null")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": "updated" } }""", "entity object")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": "updated" }, "accountRelationship": { "externalId": "R" } }""", "entity object")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": "updated" }, "account": "ACC-1" }""", "Not a valid account event")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": "updated" }, "account": { "name": "Acme" } }""", "non-empty id")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": "updated" }, "account": { "externalId": " " } }""", "non-empty id")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": "updated" }, "account": { "externalId": 17 } }""", "Not a valid account event")]
    [InlineData(/*lang=json,strict*/ """{ "account": { "externalId": "ACC-1" } }""", "event time")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03", "type": "updated" }, "account": { "externalId": "ACC-1" } }""", "event time")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14 01:02:03Z", "type": "updated" }, "account": { "externalId": "ACC-1" } }""", "event time")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "now", "type": "updated" }, "account": { "externalId": "ACC-1" } }""", "event time")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-13-14T01:02:03Z", "type": "updated" }, "account": { "externalId": "ACC-1" } }""", "event time")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": 1791939723456, "type": "updated" }, "account": { "externalId": "ACC-1" } }""", "Not a valid account event")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z" }, "account": { "externalId": "ACC-1" } }""", "event type")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": null }, "account": { "externalId": "ACC-1" } }""", "event type")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": "upsert" }, "account": { "externalId": "ACC-1" } }""", "event type")]
    [InlineData(/*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": 1 }, "account": { "externalId": "ACC-1" } }""", "Not a valid account event")]
    public void Invalid_event_is_rejected_with_the_reason(string json, string reason)
    {
        Accounts.TryRead(json, out var read, out var problem).Should().BeFalse();
        read.Should().BeNull();
        problem.Should().Contain(reason);
    }

    [Fact]
    public void Each_entity_type_reads_only_its_own_events()
    {
        const string account = /*lang=json,strict*/ """{ "meta": { "time": "2026-10-14T01:02:03Z", "type": "updated" }, "account": { "externalId": "ACC-1" } }""";

        Relationships.TryRead(account, out _, out var problem).Should().BeFalse("an account event on the relationship topic is skipped at ingest");
        problem.Should().Contain("accountRelationship");
    }

    [Fact]
    public void Every_change_to_the_model_has_its_migration()
    {
        // Only the model and the migrations' snapshot are compared
        using var db = SyncDbContext.ForModel();

        db.Database.HasPendingModelChanges().Should().BeFalse(
            "a change to SyncDbContext's model needs a migration: dotnet ef migrations add <Name> --project src/SalesforceSync --output-dir Data/Migrations");
    }
}
