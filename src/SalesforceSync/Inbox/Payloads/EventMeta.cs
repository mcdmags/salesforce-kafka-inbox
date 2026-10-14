using System.Globalization;
using System.Text.RegularExpressions;

namespace SalesforceSync.Inbox.Payloads;

/// <summary>What the source says happened to the entity. Deletes are not synced.</summary>
public enum EventType
{
    Inserted,
    Updated
}

/// <summary>
/// The <c>meta</c> object every event carries next to its entity:
/// <c>{ "time": "2026-10-14T01:02:03.456Z", "type": "inserted" }</c>.
/// The time is the event's place in its entity's history: the newest time wins, whatever order events arrive in.
/// The type must be inserted or updated (deletes are not synced); whether a record is created or updated is decided
/// by what Salesforce has.
/// </summary>
public sealed partial class EventMeta
{
    /// <summary>As sent. Kept as text so a time without an offset can be told apart and rejected.</summary>
    public string? Time { get; set; }

    /// <summary>
    /// ISO-8601 date and time with an offset or Z. Nothing else: a time without an offset would be read in whatever
    /// zone the worker runs in, so the same event could be ordered differently on another pod.
    /// </summary>
    [GeneratedRegex(@"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$", RegexOptions.CultureInvariant)]
    private static partial Regex IsoTimeWithOffset();

    /// <summary>"inserted" or "updated", in any case. Kept as text so anything else is rejected by name rather than as bad JSON.</summary>
    public string? Type { get; set; }

    /// <summary><see cref="Type"/> as an <see cref="EventType"/>, or null if it is missing or anything else (e.g. "deleted").</summary>
    public EventType? Kind()
    {
        return "inserted".Equals(Type, StringComparison.OrdinalIgnoreCase) ? EventType.Inserted
                : "updated".Equals(Type, StringComparison.OrdinalIgnoreCase) ? EventType.Updated
                : null;
    }

    /// <summary><see cref="Time"/> as an instant, or null if it is missing or not an ISO-8601 time with an offset.</summary>
    public DateTimeOffset? Timestamp()
    {
        return Time is not null && IsoTimeWithOffset().IsMatch(Time) &&
               DateTimeOffset.TryParse(Time, CultureInfo.InvariantCulture, DateTimeStyles.None, out var time)
                ? time
                : null;
    }
}