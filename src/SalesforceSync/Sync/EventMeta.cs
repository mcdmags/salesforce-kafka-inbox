using System.Globalization;
using System.Text.RegularExpressions;

namespace SalesforceSync.Sync;

/// <summary>
/// The <c>meta</c> object every event carries next to its entity:
/// <c>{ "time": "2026-10-14T01:02:03.456Z", "type": "inserted" }</c>.
/// The time is the event's place in its entity's history: the newest time wins, whatever order events arrive in, a
/// delete like any other. The type is inserted or updated (the same to the sync: what Salesforce has decides
/// create or update), or deleted.
/// </summary>
public sealed partial class EventMeta
{
    /// <summary>As sent. Kept as text so a time without an offset can be told apart and rejected.</summary>
    public string? Time { get; set; }

    /// <summary>
    /// ISO-8601 date and time with an offset or Z. Nothing else: a time without an offset would be read in the
    /// worker's zone, so another pod could order the same event differently.
    /// </summary>
    [GeneratedRegex(@"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$", RegexOptions.CultureInvariant)]
    private static partial Regex IsoTimeWithOffset();

    /// <summary>
    /// "inserted", "updated" or "deleted", in any case. Kept as text so anything else is rejected by name, not
    /// as bad JSON.
    /// </summary>
    public string? Type { get; set; }

    /// <summary>
    /// Whether <see cref="Type"/> is inserted or updated. Which of the two decides nothing: Salesforce's state
    /// decides create or update.
    /// </summary>
    public bool IsInsertOrUpdate()
    {
        return "inserted".Equals(Type, StringComparison.OrdinalIgnoreCase)
               || "updated".Equals(Type, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Whether <see cref="Type"/> is deleted: the event's entity is to be deleted.</summary>
    public bool IsDelete()
    {
        return "deleted".Equals(Type, StringComparison.OrdinalIgnoreCase);
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