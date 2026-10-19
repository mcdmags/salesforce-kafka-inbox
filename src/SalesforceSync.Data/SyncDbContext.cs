using System.Text.RegularExpressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Npgsql;
using SalesforceSync.Data.Inbox;

namespace SalesforceSync.Data;

/// <summary>
/// One inbox table per synced object type, created by EF migrations (<see cref="SyncSchema"/>). Statements are raw
/// SQL (<c>InboxRepository</c>): FOR UPDATE SKIP LOCKED and UPDATE ... RETURNING have no LINQ form.
/// <para>
/// New object: a subclass of <see cref="InboxRow"/>, its <see cref="ConfigureInbox{TRow}"/> line below, then
/// <c>dotnet ef migrations add ...</c>.
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
        // Else EF maps the subclasses as one hierarchy in one table
        modelBuilder.Ignore<InboxRow>();
        modelBuilder.Entity<AccountInboxRow>(entity => ConfigureInbox(entity, "sf_account_inbox"));
        modelBuilder.Entity<AccountRelationshipInboxRow>(entity => ConfigureInbox(entity, "sf_account_relationship_inbox"));
        modelBuilder.Entity<ContactInboxRow>(entity => ConfigureInbox(entity, "sf_contact_inbox"));

        // snake_case columns: EntityId -> entity_id
        foreach (var property in modelBuilder.Model.GetEntityTypes().SelectMany(e => e.GetProperties()))
        {
            property.SetColumnName(WordStart().Replace(property.Name, "_$1").ToLowerInvariant());
        }
    }

    [GeneratedRegex("(?<=[a-z0-9])([A-Z])", RegexOptions.CultureInvariant)]
    private static partial Regex WordStart();

    private static void ConfigureInbox<TRow>(EntityTypeBuilder<TRow> entity, string table) where TRow : InboxRow
    {
        entity.ToTable(table);
        // Vacuum at 2% dead, not the default 20%: every event, claim and outcome rewrites a row, and the claim reads the
        // front of the ready index, where those dead entries collect and slow it down as the table grows.
        entity.HasStorageParameter("autovacuum_vacuum_scale_factor", 0.02);
        entity.HasStorageParameter("autovacuum_vacuum_threshold", 1000);
        entity.HasKey(r => r.EntityId).HasName($"{table}_pkey");
        entity.Property(r => r.Payload).HasColumnType("jsonb");
        entity.Property(r => r.EventTime).IsRequired();
        entity.Property(r => r.AttemptCount).HasDefaultValue(0);
        entity.Property(r => r.LastUpdated).HasDefaultValueSql("now()");
        // Partial: done and dead rows (null) are most of the table and never claimed.
        entity.HasIndex(r => r.NextAttemptAt).HasDatabaseName($"{table}_ready").HasFilter("next_attempt_at is not null");
    }
}