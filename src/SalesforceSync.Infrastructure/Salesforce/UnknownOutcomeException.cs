namespace SalesforceSync.Infrastructure.Salesforce;

/// <summary>
/// A write was sent (or may have been) and its result never arrived, so Salesforce may still apply it, possibly later.
/// The entities in it must not be written again until it has either landed or been abandoned.
/// </summary>
public sealed class UnknownOutcomeException(Exception inner)
    : Exception($"The outcome of the Salesforce write is unknown: {inner.Message}", inner);