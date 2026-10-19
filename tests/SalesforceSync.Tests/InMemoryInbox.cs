using SalesforceSync.Data.Inbox;

namespace SalesforceSync.Tests;

/// <summary>An inbox table in memory: every pending row is claimed, and what the sync records is kept to look at.</summary>
internal sealed class InMemoryInbox<TRow> : IRepository<TRow> where TRow : InboxRow, IInboxRow, new()
{
    private readonly List<InboxEvent> _pending = [];

    public List<string> Synced { get; } = [];
    public List<FailedEvent> Dead { get; } = [];
    public List<FailedEvent> Failed { get; } = [];

    /// <summary>How many times a batch was claimed.</summary>
    public int Claims { get; private set; }

    /// <summary>The claims handed back without an attempt spent, with how.</summary>
    public List<Release> Releases { get; } = [];

    /// <summary>The error a whole failed batch was retried with.</summary>
    public List<string> FailedAll { get; } = [];

    /// <summary>The ids of the rows not yet recorded.</summary>
    public IEnumerable<string> Pending => _pending.Select(e => e.EntityId);

    public Task AddAsync(string entityId, DateTimeOffset eventTime, string payload, CancellationToken ct)
    {
        _pending.RemoveAll(e => e.EntityId == entityId);
        _pending.Add(new InboxEvent(entityId, payload, eventTime));
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

    public Task FailAllAsync(InboxClaim claim, string error, CancellationToken ct)
    {
        FailedAll.Add(error);
        Failed.AddRange(claim.Events.Select(e => new FailedEvent(e.EntityId, error)));
        _pending.Clear();
        return Task.CompletedTask;
    }
}
