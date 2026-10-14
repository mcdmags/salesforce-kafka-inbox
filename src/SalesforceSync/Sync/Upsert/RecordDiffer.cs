using System.Text.Json;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Sync.Upsert;

/// <summary>A field value to send, and what Salesforce holds in its place now.</summary>
/// <param name="StoredBefore">The current value as JSON; null for a record that doesn't exist yet.</param>
internal sealed record FieldWrite(WriteEcho Echo, string? StoredBefore, string FieldType);

/// <summary>Compares a record's normalized values with what Salesforce has.</summary>
internal static class RecordDiffer
{
    /// <summary>
    /// Fields to send: every non-empty one for a new record, otherwise only those whose normalized value differs from
    /// Salesforce. A field Salesforce won't let us set (not createable / updateable) is a problem only if it must
    /// actually be written.
    /// </summary>
    /// <param name="existing">The record in Salesforce, or null if it is new.</param>
    /// <param name="sent">Receives each value to send, as sent.</param>
    public static Dictionary<string, object?> Changes(NormalizedRecord record, CurrentRecord? existing, List<FieldWrite> sent,
        List<SalesforceError> problems)
    {
        var changes = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var (field, value) in record.Fields)
        {
            // A new record has no value to clear, and an explicit null would override the field's default
            if (existing is null && value is null)
            {
                continue;
            }

            JsonElement stored = default;
            if (existing is not null && existing.TryGetValue(field.Name, out stored) && FieldValues.AreEqual(field, value, stored))
            {
                continue;
            }

            if (NotWritable(record, field, existing) is { } problem)
            {
                problems.Add(problem);
                continue;
            }

            var wire = FieldValues.ToWire(value);
            changes[field.Name] = wire;
            var echo = new WriteEcho(record.Schema.Name, record.Record.ExternalId, field.Name, JsonSerializer.Serialize(wire));
            var storedJson = stored.ValueKind == JsonValueKind.Undefined ? "null" : stored.GetRawText();
            sent.Add(new FieldWrite(echo, existing is null ? null : storedJson, field.Type));
        }
        return changes;
    }

    /// <summary>The problem with writing <paramref name="field"/>, if Salesforce doesn't allow it on a create / an update.</summary>
    public static SalesforceError? NotWritable(NormalizedRecord record, SalesforceField field, CurrentRecord? existing)
    {
        var creating = existing is null;
        return creating ? field.Createable : field.Updateable
            ? null
            : new SalesforceError(record.ReferenceId, "INVALID_FIELD_FOR_INSERT_UPDATE",
            $"{record.Schema.Name}.{field.Name} is not {(creating ? "createable" : "updateable")}");
    }
}