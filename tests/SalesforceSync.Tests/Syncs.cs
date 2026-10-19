using SalesforceSync.Persistence;
using SalesforceSync.Sync;

namespace SalesforceSync.Tests;

/// <summary>
/// Three ingests as the app builds them (registers each once in DI), plus the aggregate. Tests using only the
/// projection need no inbox.
/// </summary>
internal static class Syncs
{
    public static readonly AccountIngest AccountIngest = new(null!);
    public static readonly AccountRelationshipIngest RelationshipIngest = new(null!);
    public static readonly ContactIngest ContactIngest = new(null!);

    public static readonly AccountAggregate Aggregate = new();

    /// <summary>One entity's event from an aggregate: its type (which inbox stores it) and event as stored.</summary>
    public sealed record EntityEvent(string EntityType, string Json);

    /// <summary>Per-entity events of an aggregate, in type order (Account, its relationships, its contacts).</summary>
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
