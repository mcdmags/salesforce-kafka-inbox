using System.Text;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using SalesforceSync.Salesforce;

namespace SalesforceSync.Tests;

/// <summary>
/// <see cref="SalesforceSchemaCache"/>'s two layers: each instance is a pod, and the pods share one
/// <see cref="IDistributedCache"/> (in memory here; Redis in production).
/// </summary>
public sealed class SalesforceSchemaCacheTests
{
    private static readonly TimeSpan Refresh = TimeSpan.FromMinutes(15);

    private static readonly SObjectSchema Account = new("Account",
        [new SalesforceField("Name", "string", 255, 0, 0, true, true, false)],
        [new SalesforceRecordType("Business", "012000000000001AAA", true, true)]);

    private readonly ManualClock _clock = new();
    private readonly MemoryDistributedCache _shared = new(Options.Create(new MemoryDistributedCacheOptions()));

    /// <summary>The If-Modified-Since value of every Describe call (null = unconditional).</summary>
    private readonly List<DateTimeOffset?> _describes = [];

    /// <summary>What Describe answers: the schema, or null for 304 Not Modified.</summary>
    private SObjectSchema? _answer = Account;

    [Fact]
    public async Task A_second_pod_uses_the_shared_describe_without_calling_Salesforce()
    {
        await Get(Pod());

        var schema = await Get(Pod());

        _describes.Should().ContainSingle("the second pod found the first pod's describe");
        schema.Fields["name"].Should().Be(Account.Fields["Name"]);
        schema.RecordTypes["business"].Should().Be(Account.RecordTypes["Business"], "record types are shared with the fields");
    }

    [Fact]
    public async Task A_stale_shared_entry_is_revalidated_and_a_304_reuses_it()
    {
        await Get(Pod());
        var fetchedAt = _clock.Now;
        _clock.Now += Refresh + TimeSpan.FromSeconds(1);

        _answer = null;   // 304 Not Modified
        var schema = await Get(Pod());
        await Get(Pod());

        _describes.Should().Equal([null, fetchedAt], "the second describe is conditional on the shared entry's date");
        schema.RecordTypes.Should().ContainKey("Business", "the stored schema is reused");
    }

    [Fact]
    public async Task Invalidating_removes_the_shared_entry_so_every_pod_describes_again()
    {
        var first = Pod();
        await Get(first);

        await first.InvalidateAsync("Account", CancellationToken.None);

        (await _shared.GetAsync(first.Key("Account"))).Should().BeNull();
        await Get(Pod());
        _describes.Should().Equal([null, null], "nothing is left to revalidate");
    }

    [Fact]
    public async Task An_unreachable_shared_cache_never_fails_the_describe()
    {
        var pod = SalesforceStep.SchemaCache(new BrokenCache(), _clock);

        (await Get(pod)).Should().BeSameAs(Account);
        await Get(pod);

        _describes.Should().ContainSingle("the in-process layer still works");
    }

    [Fact]
    public async Task An_unreadable_shared_entry_is_described_again_and_replaced()
    {
        var pod = Pod();
        await _shared.SetAsync(pod.Key("Account"), Encoding.UTF8.GetBytes("{ not json"));

        await Get(pod);
        await Get(Pod());

        _describes.Should().ContainSingle("the first pod's describe replaced the bad entry");
    }

    private SalesforceSchemaCache Pod()
    {
        return SalesforceStep.SchemaCache(_shared, _clock);
    }

    private Task<SObjectSchema> Get(SalesforceSchemaCache pod)
    {
        return pod.GetAsync("Account", Refresh, (since, _) =>
        {
            _describes.Add(since);
            return Task.FromResult(_answer);
        }, CancellationToken.None);
    }

    private sealed class ManualClock : TimeProvider
    {
        public DateTimeOffset Now { get; set; } = new(2026, 10, 14, 0, 0, 0, TimeSpan.Zero);

        public override DateTimeOffset GetUtcNow()
        {
            return Now;
        }
    }

    /// <summary>Redis down: every call fails.</summary>
    private sealed class BrokenCache : IDistributedCache
    {
        private static InvalidOperationException Down()
        {
            return new("Redis is down");
        }

        public byte[]? Get(string key)
        {
            throw Down();
        }

        public Task<byte[]?> GetAsync(string key, CancellationToken token = default)
        {
            throw Down();
        }

        public void Set(string key, byte[] value, DistributedCacheEntryOptions options)
        {
            throw Down();
        }

        public Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token = default)
        {
            throw Down();
        }

        public void Refresh(string key)
        {
            throw Down();
        }

        public Task RefreshAsync(string key, CancellationToken token = default)
        {
            throw Down();
        }

        public void Remove(string key)
        {
            throw Down();
        }

        public Task RemoveAsync(string key, CancellationToken token = default)
        {
            throw Down();
        }
    }
}