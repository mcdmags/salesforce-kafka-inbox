using SalesforceSync.Data.Inbox;

namespace SalesforceSync.Tests;

/// <summary>An inbox table in memory: every pending row is claimed, and what the sync records is kept to look at.</summary>
internal sealed class InMemoryInbox<TRow> : IRepository<TRow> where TRow : InboxRow, IInboxRow, new()
{
    private readonly List<InboxEvent> _pending = [];

    public List<string> Synced { get; } = [];
    public List<FailedEvent> Dead { get; } = [];
    public List<FailedEvent> Failed { get; } = [];

    public Task AddAsync(string entityId, DateTimeOffset eventTime, string payload, CancellationToken ct)
    {
        _pending.RemoveAll(e => e.EntityId == entityId);
        _pending.Add(new InboxEvent(entityId, payload, eventTime));
        return Task.CompletedTask;
    }

    public Task<InboxClaim> ClaimAsync(CancellationToken ct)
    {
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
        return Task.CompletedTask;
    }

    public Task FailAllAsync(InboxClaim claim, string error, CancellationToken ct)
    {
        Failed.AddRange(claim.Events.Select(e => new FailedEvent(e.EntityId, error)));
        _pending.Clear();
        return Task.CompletedTask;
    }
}
