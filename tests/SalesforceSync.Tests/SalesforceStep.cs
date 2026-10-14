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
    public static SalesforceBatchUpsertService Service(HttpClient http, IOptions<SalesforceOptions> options)
    {
        return new(new NoDatabase(), new SalesforceRestClient(http), new SalesforceSchemaCache(TimeProvider.System),
                new SalesforceErrors(options), options, Options.Create(new SyncOptions()),
                NullLogger<SalesforceBatchUpsertService>.Instance);
    }

    /// <summary>The Salesforce step never reads, records or checks the gate.</summary>
    private sealed class NoDatabase : ISalesforceBatchGate, IRepository<AccountInboxRow>
    {
        private static NotSupportedException NotUsed()
        {
            return new("The Salesforce step does not use the inbox");
        }

        public Task<bool> IsOpenAsync(CancellationToken ct)
        {
            throw NotUsed();
        }

        public Task AddAsync(string entityId, string payload, CancellationToken ct)
        {
            throw NotUsed();
        }

        public Task<InboxBatch> ReadBatchAsync(string entityType, CancellationToken ct)
        {
            throw NotUsed();
        }

        public Task RecordAsync(InboxBatch batch, BatchOutcomes outcomes, CancellationToken ct)
        {
            throw NotUsed();
        }

        public Task HoldAsync(InboxBatch batch, string error, CancellationToken ct)
        {
            throw NotUsed();
        }

        public Task DeferAsync(InboxBatch batch, TimeSpan delay, CancellationToken ct)
        {
            throw NotUsed();
        }

        public Task FailAllAsync(InboxBatch batch, string error, string? code, CancellationToken ct)
        {
            throw NotUsed();
        }
    }
}