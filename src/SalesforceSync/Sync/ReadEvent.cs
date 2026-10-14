using SalesforceSync.Inbox.Payloads;

namespace SalesforceSync.Sync;

/// <summary>An inbox event that passed <see cref="UpsertStrategy{TEvent}.TryRead"/>: the typed event, whose it is and when.</summary>
public sealed record ReadEvent<TEvent>(TEvent Event, string EntityId, DateTimeOffset Time, EventType Type);
