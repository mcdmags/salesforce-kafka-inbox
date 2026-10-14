using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using SalesforceSync.Data;

namespace SalesforceSync.Tests;

public sealed class PostgresExecutionStrategyTests
{
    private readonly Fixture _any = new();

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
        var value = _any.Create<string>();

        var result = await Strategy().RetryAsync(_ => ++attempts < 3 ? throw error : Task.FromResult(value), CancellationToken.None);

        result.Should().Be(value, "the unit's result is returned once an attempt succeeds");
        attempts.Should().Be(3);
    }

    [Fact]
    public async Task Non_transient_errors_are_not_retried()
    {
        var attempts = 0;
        var duplicate = new PostgresException("duplicate key value", "ERROR", "ERROR", "23505");

        var act = () => Strategy().RetryAsync<int>(_ => { attempts++; throw duplicate; }, CancellationToken.None);

        await act.Should().ThrowExactlyAsync<PostgresException>();
        attempts.Should().Be(1, "a unique violation won't succeed on retry");
    }

    [Fact]
    public async Task Gives_up_after_MaxRetryCount()
    {
        var attempts = 0;

        var act = () => Strategy(maxRetries: 2).RetryAsync<int>(_ => { attempts++; throw new TimeoutException(); }, CancellationToken.None);

        await act.Should().ThrowExactlyAsync<TimeoutException>();
        attempts.Should().Be(3, "the first attempt + 2 retries");
    }

    [Fact]
    public void Lost_connections_are_told_apart_from_failed_transactions()
    {
        // Lost: the pool is cleared before retrying, since its idle connections are likely dead too
        PostgresExecutionStrategy.IsConnectionLost(new NpgsqlException("connection reset", new IOException("broken pipe"))).Should().BeTrue();
        PostgresExecutionStrategy.IsConnectionLost(
            new PostgresException("terminating connection due to administrator command", "FATAL", "FATAL", "57P01")).Should().BeTrue();
        PostgresExecutionStrategy.IsConnectionLost(new PostgresException("connection failure", "FATAL", "FATAL", "08006")).Should().BeTrue();

        // The session is fine: retry on the same pool
        PostgresExecutionStrategy.IsConnectionLost(new PostgresException("could not serialize access", "ERROR", "ERROR", "40001")).Should().BeFalse();
        PostgresExecutionStrategy.IsConnectionLost(new PostgresException("deadlock detected", "ERROR", "ERROR", "40P01")).Should().BeFalse();
        PostgresExecutionStrategy.IsConnectionLost(new TimeoutException()).Should().BeFalse();
    }

    [Fact]
    public async Task Cancellation_stops_retrying()
    {
        using var cts = new CancellationTokenSource();
        var attempts = 0;

        var act = () => Strategy().RetryAsync<int>(_ =>
        {
            attempts++;
            cts.Cancel();
            throw new TimeoutException();
        }, cts.Token);

        await act.Should().ThrowAsync<Exception>();
        attempts.Should().Be(1, "no retry is started once the token is cancelled");
    }
}
