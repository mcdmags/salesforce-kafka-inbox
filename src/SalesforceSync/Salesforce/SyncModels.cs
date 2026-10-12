namespace SalesforceSync.Salesforce;

public sealed record SyncItem(
    string AccountExternalId,
    IReadOnlyDictionary<string, object?> Account,
    string ProfileExternalId,
    IReadOnlyDictionary<string, object?> Profile,
    long Version);

public enum SyncOutcome { Unchanged, Saved, Superseded, Failed }

public sealed record SyncResult(string AccountExternalId, SyncOutcome Outcome, IReadOnlyList<string> Errors);
