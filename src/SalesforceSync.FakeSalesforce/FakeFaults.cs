namespace SalesforceSync.FakeSalesforce;

/// <summary>
/// Failures a test asked for, per record (by object and external id): errors on its next writes, and responses that
/// get lost after the write was applied. Not thread-safe; <see cref="FakeSalesforceStore"/> holds its lock.
/// </summary>
internal sealed class FakeFaults
{
    private sealed class Fault(string code, string message, int times)
    {
        public string Code { get; } = code;
        public string Message { get; } = message;
        public int Remaining { get; set; } = times;
    }

    private readonly Dictionary<(string, string), Fault> _faults = new();
    private readonly Dictionary<(string, string), int> _lostResponses = new(); // (sobject, external id) -> times left

    /// <summary>Set when the request being executed wrote a record whose response is to be lost.</summary>
    public bool LoseResponse { get; set; }

    public void Inject(string sobject, string externalId, string code, string message, int times)
        => _faults[Key(sobject, externalId)] = new Fault(code, message, times);

    public void LoseResponsesFor(string sobject, string externalId, int times)
        => _lostResponses[Key(sobject, externalId)] = times;

    /// <summary>The injected error for this write, if one is left.</summary>
    public (string Code, string Message)? Take(string sobject, string? externalId)
    {
        if (externalId is null || !_faults.TryGetValue(Key(sobject, externalId), out var fault) || fault.Remaining <= 0)
            return null;
        fault.Remaining--;
        return (fault.Code, fault.Message);
    }

    /// <summary>Called for every record written: marks the current request's response as lost if asked to.</summary>
    public void NoteWrite(string sobject, string? externalId)
    {
        if (externalId is null || !_lostResponses.TryGetValue(Key(sobject, externalId), out var left) || left <= 0) return;
        _lostResponses[Key(sobject, externalId)] = left - 1;
        LoseResponse = true;
    }

    public void Clear()
    {
        _faults.Clear();
        _lostResponses.Clear();
    }

    private static (string, string) Key(string sobject, string externalId) => (sobject.ToLowerInvariant(), externalId);
}
