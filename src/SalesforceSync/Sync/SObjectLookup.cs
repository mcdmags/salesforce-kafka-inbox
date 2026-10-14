using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync;

/// <summary>
/// Sets <paramref name="Field"/> to the Id of the record of <paramref name="SObject"/> whose
/// <paramref name="ExternalIdField"/> is <paramref name="ExternalId"/>: an earlier record of the same unit (created in
/// the same request if new) or a record already in Salesforce (read by the strategy's queries). If neither exists, the
/// unit is retried later (<see cref="SalesforceErrors.DependencyNotSynced"/>).
/// </summary>
public sealed record SObjectLookup(string Field, string SObject, string ExternalIdField, string ExternalId);
