using SalesforceSync.Persistence;
using SalesforceSync.Sync;
using SalesforceSync.ToSalesforce;

namespace SalesforceSync.Tests;

/// <summary>
/// The three synced objects as the app builds them (it registers each once in DI): the ingests, the syncs and the
/// strategies the engine runs for them. The tests that use only the mapping or the projection need neither inbox nor
/// engine.
/// </summary>
internal static class Syncs
{
    public static readonly AccountIngest AccountIngest = new(null!);
    public static readonly AccountRelationshipIngest RelationshipIngest = new(null!);
    public static readonly ContactIngest ContactIngest = new(null!);

    public static readonly AccountSync Account = new(null!, null!);
    public static readonly AccountRelationshipSync Relationship = new(null!, null!);
    public static readonly ContactSync Contact = new(null!, null!);

    public static readonly SyncStrategy<AccountEvent> AccountStrategy = new(AccountEvent.Reader, Account.Map, Account.Queries);
    public static readonly SyncStrategy<AccountRelationshipEvent> RelationshipStrategy = new(AccountRelationshipEvent.Reader, Relationship.Map, Relationship.Queries);
    public static readonly SyncStrategy<ContactEvent> ContactStrategy = new(ContactEvent.Reader, Contact.Map, Contact.Queries);

    public static readonly AccountAggregate Aggregate = new();

    /// <summary>One entity's event out of an aggregate: its type (which inbox stores it) and the event as stored.</summary>
    public sealed record EntityEvent(string EntityType, string Json);

    /// <summary>The per-entity events of an aggregate, in the order of the types (the Account, its relationships, its contacts).</summary>
    public static bool TrySplit(string json, out List<EntityEvent>? events, out string? problem)
    {
        events = null;
        if (!Aggregate.TryRead(json, out var change, out problem))
        {
            return false;
        }

        events =
        [
            .. AccountIngest.Project(change).Select(e => new EntityEvent(AccountSync.Type, AggregateProjection.Serialize(e))),
            .. RelationshipIngest.Project(change).Select(e => new EntityEvent(AccountRelationshipSync.Type, AggregateProjection.Serialize(e))),
            .. ContactIngest.Project(change).Select(e => new EntityEvent(ContactSync.Type, AggregateProjection.Serialize(e)))
        ];
        return true;
    }
}
