namespace SalesforceSync.Inbox;

public sealed class SyncOptions
{
    public const string Section = "Sync";

    /// <summary>Parallel worker loops in this process.</summary>
    public int Workers { get; set; } = 4;

    /// <summary>Max in-flight Salesforce batches across ALL processes.</summary>
    public int MaxConcurrentRequests { get; set; } = 6;

    /// <summary>Accounts per batch (= graphs per Composite Graph request, max 75).</summary>
    public int BatchSize { get; set; } = 75;

    public TimeSpan Lease { get; set; } = TimeSpan.FromMinutes(5);
    public int MaxRetries { get; set; } = 8;
    public TimeSpan IdleDelay { get; set; } = TimeSpan.FromSeconds(1);
}
