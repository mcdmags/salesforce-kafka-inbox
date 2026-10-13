using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using SalesforceSync.Data;

namespace SalesforceSync.Tests;

public sealed class PostgresExecutionStrategyTests
{
    private static PostgresExecutionStrategy Strategy(int maxRetries = 3) => new(
        NpgsqlDataSource.Create("Host=unused"),
        Options.Create(new PostgresRetryOptions { MaxRetryCount = maxRetries, BaseDelay = TimeSpan.FromMilliseconds(1), MaxRetryDelay = TimeSpan.FromMilliseconds(5) }),
        NullLogger<PostgresExecutionStrategy>.Instance);

    public static TheoryData<Exception> Transient => new()
    {
        new NpgsqlException("connection reset", new IOException("broken pipe")),
        new PostgresException("could not serialize access", "ERROR", "ERROR", "40001"),
        new PostgresException("deadlock detected", "ERROR", "ERROR", "40P01"),
        new PostgresException("terminating connection due to administrator command", "FATAL", "FATAL", "57P01"),
        new PostgresException("the database system is starting up", "FATAL", "FATAL", "57P03"),
        new TimeoutException("timeout")
    };

    [Theory]
    [MemberData(nameof(Transient))]
    public async Task Transient_errors_are_retried_until_the_unit_succeeds(Exception error)
    {
        var attempts = 0;

        var result = await Strategy().RetryAsync(_ => ++attempts < 3 ? throw error : Task.FromResult("ok"), CancellationToken.None);

        Assert.Equal("ok", result);
        Assert.Equal(3, attempts);
    }

    [Fact]
    public async Task Non_transient_errors_are_not_retried()
    {
        var attempts = 0;
        var duplicate = new PostgresException("duplicate key value", "ERROR", "ERROR", "23505");

        await Assert.ThrowsAsync<PostgresException>(() =>
            Strategy().RetryAsync<int>(_ => { attempts++; throw duplicate; }, CancellationToken.None));

        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task Gives_up_after_MaxRetryCount()
    {
        var attempts = 0;

        await Assert.ThrowsAsync<TimeoutException>(() =>
            Strategy(maxRetries: 2).RetryAsync<int>(_ => { attempts++; throw new TimeoutException(); }, CancellationToken.None));

        Assert.Equal(3, attempts); // the first attempt + 2 retries
    }

    [Fact]
    public async Task Cancellation_stops_retrying()
    {
        using var cts = new CancellationTokenSource();
        var attempts = 0;

        await Assert.ThrowsAnyAsync<Exception>(() => Strategy().RetryAsync<int>(_ =>
        {
            attempts++;
            cts.Cancel();
            throw new TimeoutException();
        }, cts.Token));

        Assert.Equal(1, attempts);
    }
}
