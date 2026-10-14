using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync.Upsert;

/// <summary>Compares a record's normalized values with what Salesforce has.</summary>
internal static class RecordDiffer
{
    /// <summary>
    /// Fields to send: every non-empty one for a new record, otherwise only those whose normalized value differs from
    /// Salesforce. A field Salesforce won't let us set (not createable / updateable) is a problem only if it must
    /// actually be written.
    /// </summary>
    /// <param name="existing">The record in Salesforce, or null if it is new.</param>
    public static Dictionary<string, object?> Changes(NormalizedRecord record, CurrentRecord? existing, List<SalesforceError> problems)
    {
        var changes = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var (field, value) in record.Fields)
        {
            // A new record has no value to clear, and an explicit null would override the field's default
            if (existing is null && value is null)
            {
                continue;
            }

            if (existing is not null && existing.TryGetValue(field.Name, out var stored) && FieldValues.AreEqual(field, value, stored))
            {
                continue;
            }

            if (NotWritable(record, field, existing) is { } problem)
            {
                problems.Add(problem);
                continue;
            }

            changes[field.Name] = FieldValues.ToWire(value);
        }
        return changes;
    }

    /// <summary>The problem with writing <paramref name="field"/>, if Salesforce doesn't allow it on a create / an update.</summary>
    public static SalesforceError? NotWritable(NormalizedRecord record, SalesforceField field, CurrentRecord? existing)
    {
        var creating = existing is null;
        return (creating ? field.Createable : field.Updateable)
            ? null
            : new SalesforceError(record.ReferenceId, "INVALID_FIELD_FOR_INSERT_UPDATE",
                $"{record.Schema.Name}.{field.Name} is not {(creating ? "createable" : "updateable")}");
    }
}