using SalesforceSync.Sync;

namespace SalesforceSync.Tests;

/// <summary>The three synced objects as the app builds them (it registers each once in DI): the projections of an aggregate, the syncs, and the aggregate over the projections.</summary>
internal static class Syncs
{
    public static readonly AccountProjection AccountProjection = new();
    public static readonly AccountRelationshipProjection RelationshipProjection = new();
    public static readonly ContactProjection ContactProjection = new();

    public static readonly AccountSync Account = new();
    public static readonly AccountRelationshipSync Relationship = new();
    public static readonly ContactSync Contact = new();

    /// <summary>The Account aggregate over the three, parents first, as <c>AddInboxSync</c> registers them.</summary>
    public static readonly AccountAggregate Aggregate = new([AccountProjection, RelationshipProjection, ContactProjection]);
}
