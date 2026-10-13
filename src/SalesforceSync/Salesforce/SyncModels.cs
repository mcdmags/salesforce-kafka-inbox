namespace SalesforceSync.Salesforce;

public sealed record SyncItem(
    string AccountExternalId,
    IReadOnlyDictionary<string, object?> Account,
    string ProfileExternalId,
    IReadOnlyDictionary<string, object?> Profile);

public enum SyncOutcome { Unchanged, Saved, Failed }

public sealed record SyncResult(string AccountExternalId, SyncOutcome Outcome, IReadOnlyList<string> Errors);
