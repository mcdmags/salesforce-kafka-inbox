using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Memory;
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
        return new(new NoDatabase(), new SalesforceRestClient(http), SchemaCache(),
                new SalesforceErrors(options), options, Options.Create(new SyncOptions()),
                NullLogger<SalesforceBatchUpsertService>.Instance);
    }

    /// <summary>A Describe cache of its own; with <paramref name="shared"/>, a pod sharing that cache with others.</summary>
    public static SalesforceSchemaCache SchemaCache(IDistributedCache? shared = null, TimeProvider? clock = null)
    {
        return new(clock ?? TimeProvider.System, shared ?? new MemoryDistributedCache(Options.Create(new MemoryDistributedCacheOptions())),
            Options.Create(new SalesforceOptions { InstanceUrl = "https://fake.my.salesforce.com" }),
            NullLogger<SalesforceSchemaCache>.Instance);
    }

    /// <summary>The Salesforce step never claims, records or takes a slot.</summary>
    private sealed class NoDatabase : ISalesforceBatchGate, IRepository<AccountInboxRow>
    {
        private static NotSupportedException NotUsed()
        {
            return new("The Salesforce step does not use the inbox");
        }

        public Task<IAsyncDisposable?> TryEnterAsync(CancellationToken ct)
        {
            throw NotUsed();
        }

        public Task AddAsync(string entityId, string payload, CancellationToken ct)
        {
            throw NotUsed();
        }

        public Task<InboxClaim> ClaimAsync(string entityType, CancellationToken ct)
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