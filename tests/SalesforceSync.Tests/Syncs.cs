using SalesforceSync.Persistence;
using SalesforceSync.Sync;

namespace SalesforceSync.Tests;

/// <summary>
/// The three ingests as the app builds them (it registers each once in DI), and the aggregate. The tests that use only
/// the projection need no inbox.
/// </summary>
internal static class Syncs
{
    public static readonly AccountIngest AccountIngest = new(null!);
    public static readonly AccountRelationshipIngest RelationshipIngest = new(null!);
    public static readonly ContactIngest ContactIngest = new(null!);

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
            .. AccountIngest.Project(change).Select(e => new EntityEvent(AccountEvent.Type, AggregateProjection.Serialize(e))),
            .. RelationshipIngest.Project(change).Select(e => new EntityEvent(AccountRelationshipEvent.Type, AggregateProjection.Serialize(e))),
            .. ContactIngest.Project(change).Select(e => new EntityEvent(ContactEvent.Type, AggregateProjection.Serialize(e)))
        ];
        return true;
    }
}
