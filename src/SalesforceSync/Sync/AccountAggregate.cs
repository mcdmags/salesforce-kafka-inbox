using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace SalesforceSync.Sync;

/// <summary>
/// The Kafka event: an Account aggregate, its state before and after the change. Each snapshot holds the Account (with
/// its Profile, as an account event has it) and its children, the account relationships and the contacts:
/// <code>{ "meta": { "time": "2026-10-14T01:02:03.456Z", "type": "updated" },
///   "before": { "externalId": "ACC-1", "name": "Acme", "profile": { ... },
///               "accountRelationships": [ { "externalId": "REL-1", "relatedAccountExternalId": "ACC-2", ... } ],
///               "contacts": [ { "externalId": "CON-1", "lastName": "Lovelace", ... } ] },
///   "after": { ... } }</code>
/// <see cref="TryRead"/> checks it into an <see cref="AggregateChange"/>; each entity type then projects the events it needs
/// (its <see cref="Persistence.IAggregateIngest"/>) and stores them in its inbox. Only what changed is stored: an unchanged child costs
/// no inbox row, no claim, no Salesforce read. No I/O.
/// </summary>
public sealed class AccountAggregate
{
    public const string DefaultTopic = "account-changes";

    /// <summary>
    /// Reads an aggregate message: valid JSON, a meta with a time (with an offset) and a type, before or after, one account.
    /// </summary>
    /// <returns>False with <paramref name="problem"/> when the message is no aggregate the sync can read.</returns>
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

    /// <summary>What is wrong with the event's meta (a time with an offset, a type) and snapshots (not both missing, one account), if anything.</summary>
    private static string? ProblemWith(EventMeta? meta, AccountSnapshot? before, AccountSnapshot? after)
    {
        if (meta?.Timestamp() is null)
        {
            return "The event time must be an ISO-8601 time with an offset or Z, e.g. 2026-10-14T01:02:03.456Z";
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

/// <summary>The Kafka message: meta and the Account aggregate's snapshots before and after the change.</summary>
public sealed record AccountAggregateEvent(EventMeta? Meta, AccountSnapshot? Before, AccountSnapshot? After);

/// <summary>An Account as an aggregate snapshot has it: the account event's Account and Profile, plus its children.</summary>
public sealed class AccountSnapshot : AccountPayload
{
    public List<AccountRelationshipPayload?>? AccountRelationships { get; set; }
    public List<ContactPayload?>? Contacts { get; set; }
}
