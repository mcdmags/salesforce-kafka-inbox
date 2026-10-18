using System.Text.RegularExpressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Npgsql;
using SalesforceSync.Data.Inbox;
using SalesforceSync.Data.RateLimiting;

namespace SalesforceSync.Data;

/// <summary>
/// The sync's tables, defined here and created by EF migrations (Data/Migrations, applied at startup by
/// <see cref="SyncSchema"/>): one inbox table per synced object type, and the rate limiter's
/// sf_rate_limit and sf_api_usage. The claim's statements are raw SQL (<see cref="InboxSql"/>) run through
/// <c>Database.SqlQueryRaw</c>, since FOR UPDATE SKIP LOCKED and UPDATE ... RETURNING have no LINQ form.
/// <para>
/// To sync another object: a subclass of <see cref="InboxRow"/> with its limits (<see cref="IInboxRow"/>) and its
/// <see cref="ConfigureInbox{TRow}"/> line in <see cref="OnModelCreating"/>, with its table; then a migration
/// (<c>dotnet ef migrations add ...</c>) creates it.
/// </para>
/// </summary>
public partial class SyncDbContext(DbContextOptions<SyncDbContext> options) : DbContext(options)
{
    /// <summary>The tables of all inbox row types, as the model names them.</summary>
    public IReadOnlyList<string> InboxTables => [.. Model.GetEntityTypes()
        .Where(e => e.ClrType.IsAssignableTo(typeof(InboxRow)))
        .Select(e => e.GetTableName()!)];

    /// <summary>The inbox table of a row type: the model is the one place that names it.</summary>
    public string TableOf<TRow>() where TRow : InboxRow
    {
        return Model.FindEntityType(typeof(TRow))?.GetTableName()
               ?? throw new InvalidOperationException($"{typeof(TRow).Name} is not mapped in {nameof(SyncDbContext)}");
    }

    /// <summary>The factory the worker, the benchmarks and the tests share: contexts pooled over <paramref name="db"/>.</summary>
    public static IDbContextFactory<SyncDbContext> CreateFactory(NpgsqlDataSource db, PostgresRetryOptions? retry = null)
    {
        var options = new DbContextOptionsBuilder<SyncDbContext>();
        Configure(options, db, retry ?? new PostgresRetryOptions());
        return new PooledDbContextFactory<SyncDbContext>(options.Options);
    }

    /// <summary>A context for its model alone (table names, pending model changes): it never connects.</summary>
    public static SyncDbContext ForModel()
    {
        return new(new DbContextOptionsBuilder<SyncDbContext>().UseNpgsql("Host=unused").Options);
    }

    /// <summary>Over the shared data source, with transient failures retried as <see cref="PostgresRetryStrategy"/> does.</summary>
    public static void Configure(DbContextOptionsBuilder options, NpgsqlDataSource db, PostgresRetryOptions retry)
    {
        options.UseNpgsql(db, npgsql => npgsql.ExecutionStrategy(dependencies => new PostgresRetryStrategy(dependencies, db, retry)));
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // The base is only shared code: without this, EF would make it an entity and map the rows as one hierarchy
        modelBuilder.Ignore<InboxRow>();
        modelBuilder.Entity<AccountInboxRow>(entity => ConfigureInbox(entity, "sf_account_inbox"));
        modelBuilder.Entity<AccountRelationshipInboxRow>(entity => ConfigureInbox(entity, "sf_account_relationship_inbox"));
        modelBuilder.Entity<ContactInboxRow>(entity => ConfigureInbox(entity, "sf_contact_inbox"));

        // The shared token bucket and daily budget guard: one row, created here. Bucket size, refill rate and budget
        // are set from configuration by the deploy's migrate step.
        modelBuilder.Entity<RateLimitState>(entity =>
        {
            entity.ToTable("sf_rate_limit");
            entity.HasKey(r => r.Name).HasName("sf_rate_limit_pkey");
            entity.Property(r => r.UpdatedAt).HasDefaultValueSql("clock_timestamp()");
            entity.HasData(new { Name = "salesforce", Tokens = 5.0, Capacity = 5.0, BaseRefillPerSec = 2.0, RefillPerSec = 2.0 });
        });
        modelBuilder.HasDbFunction(() => RateLimitSql.ClockTimestamp()).HasName("clock_timestamp").IsBuiltIn();

        modelBuilder.Entity<ApiUsageBucket>(entity =>
        {
            entity.ToTable("sf_api_usage");
            entity.HasKey(u => u.Bucket).HasName("sf_api_usage_pkey");
        });

        // Every column is its property's name in snake_case (EntityId -> entity_id)
        foreach (var property in modelBuilder.Model.GetEntityTypes().SelectMany(e => e.GetProperties()))
        {
            property.SetColumnName(WordStart().Replace(property.Name, "_$1").ToLowerInvariant());
        }
    }

    /// <summary>An upper-case letter that starts a word inside a name.</summary>
    [GeneratedRegex("(?<=[a-z0-9])([A-Z])", RegexOptions.CultureInvariant)]
    private static partial Regex WordStart();

    /// <summary>
    /// An inbox table: one row per entity, holding its newest Kafka event, what became of it and the entity's sync state
    /// (due when, leased by whom); the id is a bigserial. Columns with a database default are left out of
    /// an insert while they hold the CLR default, so a new row gets the database's clock and 'pending'.
    /// </summary>
    private static void ConfigureInbox<TRow>(EntityTypeBuilder<TRow> entity, string table) where TRow : InboxRow
    {
        entity.ToTable(table);
        // A hot table: vacuumed after 2% (plus 1,000 rows) of it is dead instead of the default 20%. Every event rewrites
        // its entity's row and every claim and outcome updates it again, and the claims read the front of the ready
        // index, where the dead index entries those updates leave behind collect. The default lets that grow with the table.
        entity.HasStorageParameter("autovacuum_vacuum_scale_factor", 0.02);
        entity.HasStorageParameter("autovacuum_vacuum_threshold", 1000);
        entity.HasKey(r => r.Id).HasName($"{table}_pkey");
        entity.Property(r => r.Id).UseSerialColumn();
        entity.Property(r => r.Payload).HasColumnType("jsonb");
        // No default: every insert sets it from the event (InboxSql.Store)
        entity.Property(r => r.EventTime).IsRequired();
        entity.Property(r => r.Status).HasDefaultValueSql("'pending'");
        entity.Property(r => r.AttemptCount).HasDefaultValue(0);
        entity.Property(r => r.LastUpdated).HasDefaultValueSql("now()");
        // One row per entity, which a newer event overwrites (InboxSql.Store's on conflict)
        entity.HasIndex(r => r.EntityId).IsUnique().HasDatabaseName($"{table}_entity_id");
        // The claim (InboxSql.Claim): the due rows, the longest-waiting first. Idle rows (null) are not in the index.
        entity.HasIndex(r => r.NextAttemptAt).HasDatabaseName($"{table}_ready").HasFilter("next_attempt_at is not null");
    }
}