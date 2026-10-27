namespace SalesforceSync.ToSalesforce;

/// <summary>A record as an event wants it in Salesforce.</summary>
/// <param name="EntityId">The inbox entity the record belongs to; its failures are recorded there.</param>
/// <param name="ExternalId">The value the record is matched by in Salesforce.</param>
/// <param name="Fields">Every field the event sets, by API name. Null clears the field.</param>
public sealed record DesiredRecord(string EntityId, string ExternalId, Dictionary<string, object?> Fields);

/// <summary>A record to delete: its Salesforce Id, and the inbox entity it belongs to.</summary>
public sealed record RecordToDelete(string EntityId, string Id);

public static class DesiredRecords
{
    /// <summary>Every field the records set, so a read fetches what the diff compares.</summary>
    public static IEnumerable<string> FieldNames(this IEnumerable<DesiredRecord> records)
    {
        return records.SelectMany(r => r.Fields.Keys);
    }
}
