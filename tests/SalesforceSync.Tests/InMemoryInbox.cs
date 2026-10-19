using SalesforceSync.Data.Inbox;

namespace SalesforceSync.Tests;

/// <summary>In-memory inbox table: every pending row claimed; what the sync records is kept for inspection.</summary>
internal sealed class InMemoryInbox<TRow> : IRepository<TRow> where TRow : InboxRow
{
    private readonly List<InboxEvent> _pending = [];

    public List<string> Synced { get; } = [];
    public List<FailedEvent> Dead { get; } = [];
    public List<FailedEvent> Failed { get; } = [];

    public int Claims { get; private set; }

    /// <summary>Claims handed back without an attempt spent, with how.</summary>
    public List<Release> Releases { get; } = [];

    public IEnumerable<string> Pending => _pending.Select(e => e.EntityId);

    public Task AddAsync(string entityId, DateTimeOffset eventTime, string payload, CancellationToken ct)
    {
        _pending.RemoveAll(e => e.EntityId == entityId);
        _pending.Add(new InboxEvent(entityId, payload, eventTime, DateTimeOffset.MaxValue));
        return Task.CompletedTask;
    }

    public Task<InboxClaim> ClaimAsync(CancellationToken ct)
    {
        Claims++;
        return Task.FromResult(new InboxClaim([.. _pending]));
    }

    public Task RecordAsync(InboxClaim claim, BatchOutcomes outcomes, CancellationToken ct)
    {
        Synced.AddRange(outcomes.Synced);
        Dead.AddRange(outcomes.Dead);
        Failed.AddRange(outcomes.Failed);
        _pending.Clear();
        return Task.CompletedTask;
    }

    public Task ReleaseAsync(InboxClaim claim, Release release, CancellationToken ct)
    {
        Releases.Add(release);
        return Task.CompletedTask;
    }
}
