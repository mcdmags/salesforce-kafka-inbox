using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SalesforceSync.Inbox;
using SalesforceSync.RateLimiting;
using SalesforceSync.Salesforce;
using SalesforceSync.Sync;

namespace SalesforceSync.Tests;

/// <summary>
/// The engine wired for its Salesforce step alone (<see cref="SalesforceBatchUpsertService.WriteToSalesforceAsync"/>:
/// describe, read, decide, send, interpret): a real client on the given HTTP stack, its own caches, and no database.
/// </summary>
internal static class SalesforceStep
{
    public static SalesforceBatchUpsertService Service(HttpClient http, IOptions<SalesforceOptions> options, SyncOptions? sync = null)
    {
        return new(new NoDatabase(), new NoDatabase(), new SalesforceRestClient(http), new SalesforceSchemaCache(TimeProvider.System),
                new SalesforceErrors(options), new WriteEchoTracker(), options, Options.Create(sync ?? new SyncOptions()),
                NullLogger<SalesforceBatchUpsertService>.Instance);
    }

    /// <summary>The Salesforce step never claims, records or takes a slot.</summary>
    private sealed class NoDatabase : ISalesforceBatchGate, IInboxRepository
    {
        private static NotSupportedException NotUsed()
        {
            return new("The Salesforce step does not use the inbox");
        }

        public Task<IAsyncDisposable?> TryEnterAsync(CancellationToken ct)
        {
            throw NotUsed();
        }

        public Task<InboxClaim> ClaimAsync(string inboxTable, string entityType, CancellationToken ct)
        {
            throw NotUsed();
        }

        public Task RecordAsync(InboxClaim claim, BatchOutcomes outcomes, CancellationToken ct)
        {
            throw NotUsed();
        }

        public Task HoldAsync(InboxClaim claim, string error, CancellationToken ct)
        {
            throw NotUsed();
        }

        public Task DeferAsync(InboxClaim claim, TimeSpan delay, CancellationToken ct)
        {
            throw NotUsed();
        }

        public Task FailAllAsync(InboxClaim claim, string error, string? code, CancellationToken ct)
        {
            throw NotUsed();
        }
    }
}