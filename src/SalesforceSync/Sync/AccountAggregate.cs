using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace SalesforceSync.Sync;

/// <summary>
/// Kafka event: Account aggregate, state before + after change. Each snapshot = Account (with Profile, as in account
/// event) + children (account relationships, contacts):
/// <code>{ "meta": { "time": "2026-10-14T01:02:03.456Z", "type": "updated" },
///   "before": { "externalId": "ACC-1", "name": "Acme", "profile": { ... },
///               "accountRelationships": [ { "externalId": "REL-1", "relatedAccountExternalId": "ACC-2", ... } ],
///               "contacts": [ { "externalId": "CON-1", "lastName": "Lovelace", ... } ] },
///   "after": { ... } }</code>
/// <see cref="TryRead"/> validates it into an <see cref="AggregateChange"/>; each entity type projects its events
/// (<see cref="Persistence.IAggregateIngest"/>), stores in its inbox. Only changes stored: unchanged child = no inbox
/// row, claim or Salesforce read. No I/O.
/// </summary>
public sealed class AccountAggregate
{
    public const string DefaultTopic = "account-changes";

    /// <summary>
    /// Reads aggregate message: valid JSON, meta with time (with offset) + type, before or after, one account.
    /// </summary>
    /// <returns>False with <paramref name="problem"/> when message isn't a readable aggregate.</returns>
    public bool TryRead(string json, [NotNullWhen(true)] out AggregateChange? change, [NotNullWhen(false)] out string? problem)
    {
        change = null;
        AccountAggregateEvent? aggregate;
        try
        {
            aggregate = JsonSerializer.Deserialize<AccountAggregateEvent>(json, EventJson.Options);
        }
        catch (JsonException ex)
        {
            problem = $"Not a valid account aggregate event: {ex.Message}";
            return false;
        }

        if (aggregate is null)
        {
            problem = "The event is not a JSON object";
            return false;
        }

        problem = ProblemWith(aggregate.Meta, aggregate.Before, aggregate.After);
        if (problem is not null)
        {
            return false;
        }

        change = new AggregateChange(aggregate.Meta!, aggregate.Before, aggregate.After);
        return true;
    }

    private static string? ProblemWith(EventMeta? meta, AccountSnapshot? before, AccountSnapshot? after)
    {
        if (meta?.Time is null)
        {
            return "The event needs its time (meta.time), e.g. 2026-10-14T01:02:03.456Z";
        }

        if (!meta.IsInsertOrUpdate() && !meta.IsDelete())
        {
            return "The event type must be \"inserted\", \"updated\" or \"deleted\"";
        }

        if (before is null && after is null)
        {
            return "The event needs before or after";
        }

        var (account, other) = (after ?? before, after is null ? null : before);
        return string.IsNullOrWhiteSpace(account!.ExternalId) ? "The account needs a non-empty externalId"
            : other is not null && other.ExternalId != account.ExternalId
                ? $"before and after are different accounts ('{other.ExternalId}', '{account.ExternalId}')"
                : null;
    }
}

public sealed record AccountAggregateEvent(EventMeta? Meta, AccountSnapshot? Before, AccountSnapshot? After);

/// <summary>Account in a snapshot: account event's Account + Profile, plus children.</summary>
public sealed class AccountSnapshot : AccountPayload
{
    public List<AccountRelationshipPayload?>? AccountRelationships { get; set; }
    public List<ContactPayload?>? Contacts { get; set; }
}
