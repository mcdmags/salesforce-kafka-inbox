using System.Globalization;
using System.Text.RegularExpressions;

namespace SalesforceSync.Sync;

/// <summary>
/// <c>meta</c> object every event carries next to its entity:
/// <c>{ "time": "2026-10-14T01:02:03.456Z", "type": "inserted" }</c>.
/// Time = event's place in entity history: newest wins whatever arrival order, deletes too. Type is inserted or updated
/// (same to the sync: Salesforce state decides create/update), or deleted.
/// </summary>
public sealed partial class EventMeta
{
    /// <summary>As sent. Text, so offset-less times can be spotted and rejected.</summary>
    public string? Time { get; set; }

    /// <summary>
    /// ISO-8601 date-time with offset or Z, nothing else: offset-less time would be read in worker's zone, so pods
    /// could order the same event differently.
    /// </summary>
    [GeneratedRegex(@"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$", RegexOptions.CultureInvariant)]
    private static partial Regex IsoTimeWithOffset();

    /// <summary>
    /// "inserted", "updated" or "deleted", any case. Text, so anything else is rejected by name, not as bad JSON.
    /// </summary>
    public string? Type { get; set; }

    /// <summary>
    /// <see cref="Type"/> is inserted or updated. Which one decides nothing: Salesforce state decides create/update.
    /// </summary>
    public bool IsInsertOrUpdate()
    {
        return "inserted".Equals(Type, StringComparison.OrdinalIgnoreCase)
               || "updated".Equals(Type, StringComparison.OrdinalIgnoreCase);
    }

    public bool IsDelete()
    {
        return "deleted".Equals(Type, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary><see cref="Time"/> as instant; null if missing or not ISO-8601 with offset.</summary>
    public DateTimeOffset? Timestamp()
    {
        return Time is not null && IsoTimeWithOffset().IsMatch(Time) &&
               DateTimeOffset.TryParse(Time, CultureInfo.InvariantCulture, DateTimeStyles.None, out var time)
                ? time
                : null;
    }
}