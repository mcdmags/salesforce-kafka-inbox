using Microsoft.EntityFrameworkCore;
using SalesforceSync.Data.RateLimiting;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// The concurrency slots (leases on the shared rate-limit row), in a schema of their own so the pods' slots are never
/// touched. Every test starts with all slots free.
/// </summary>
[Collection(SyncCollection.Name)]
public sealed class ConcurrencySlotsTests(SyncFixture app)
{
    private const string Schema = "slots_test";

    [Fact]
    public async Task At_most_the_configured_number_of_slots_are_held_and_a_released_one_is_free_again()
    {
        var slots = await StartAsync(max: 2);

        var first = await slots.TryAcquireAsync(CancellationToken.None);
        var second = await slots.TryAcquireAsync(CancellationToken.None);
        var third = await slots.TryAcquireAsync(CancellationToken.None);

        first.Should().NotBeNull();
        second.Should().NotBeNull();
        third.Should().BeNull("both slots are busy");

        await first!.DisposeAsync();
        var fourth = await slots.TryAcquireAsync(CancellationToken.None);
        fourth.Should().NotBeNull("the released slot is free");
        (await slots.TryAcquireAsync(CancellationToken.None)).Should().BeNull();
    }

    [Fact]
    public async Task Many_pods_asking_at_once_never_hold_more_than_the_configured_number()
    {
        var slots = await StartAsync(max: 5);

        var held = await Task.WhenAll(Enumerable.Range(0, 40).Select(_ => Task.Run(() => slots.TryAcquireAsync(CancellationToken.None))));

        held.Count(h => h is not null).Should().Be(5, "the row lock decides who takes the last slots, whatever the interleaving");
    }

    [Fact]
    public async Task A_slot_whose_holder_died_is_free_when_its_lease_ends()
    {
        var slots = await StartAsync(max: 1, lease: TimeSpan.FromMilliseconds(400));
        (await slots.TryAcquireAsync(CancellationToken.None)).Should().NotBeNull(); // never released: the holder "died"

        (await slots.TryAcquireAsync(CancellationToken.None)).Should().BeNull("the lease is still running");

        await Eventually.ValueAsync(() => slots.TryAcquireAsync(CancellationToken.None), slot => slot is not null,
            "the dead holder's lease to end and its slot to be free");
    }

    [Fact]
    public async Task Releasing_one_slot_leaves_the_others_held()
    {
        var slots = await StartAsync(max: 2);
        var first = await slots.TryAcquireAsync(CancellationToken.None);
        await slots.TryAcquireAsync(CancellationToken.None);

        await first!.DisposeAsync();

        (await slots.TryAcquireAsync(CancellationToken.None)).Should().NotBeNull("one slot was free");
        (await slots.TryAcquireAsync(CancellationToken.None)).Should().BeNull("the other holder still has its slot");
    }

    private async Task<SalesforceConcurrencySlots> StartAsync(int max, TimeSpan? lease = null)
    {
        var (db, contexts) = await app.MigratedSchemaAsync(Schema);
        await using var conn = await db.OpenConnectionAsync();
        await conn.ExecuteAsync("UPDATE sf_rate_limit SET slots = '[]'::jsonb");
        return new SalesforceConcurrencySlots(contexts, max, lease ?? TimeSpan.FromMinutes(5));
    }
}
