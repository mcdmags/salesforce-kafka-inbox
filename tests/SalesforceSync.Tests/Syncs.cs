using SalesforceSync.Sync;

namespace SalesforceSync.Tests;

/// <summary>The three synced objects as the app builds them (it registers each once in DI), and the aggregate over them.</summary>
internal static class Syncs
{
    public static readonly AccountSync Account = new();
    public static readonly AccountRelationshipSync Relationship = new();
    public static readonly ContactSync Contact = new();

    /// <summary>The Account aggregate over the three, parents first, as <c>AddInboxSync</c> registers them.</summary>
    public static readonly AccountAggregate Aggregate = new([Account, Relationship, Contact]);
}
