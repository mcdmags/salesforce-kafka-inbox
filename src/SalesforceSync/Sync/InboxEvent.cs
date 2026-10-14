namespace SalesforceSync.Sync;

/// <summary>A pending inbox row as read: the entity it belongs to and the raw event.</summary>
public sealed record InboxEvent(long Id, string EntityId, string Payload, int RetryCount);