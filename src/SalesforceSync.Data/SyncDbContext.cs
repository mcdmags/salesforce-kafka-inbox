using System.Text.RegularExpressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Npgsql;
using SalesforceSync.Data.Inbox;

namespace SalesforceSync.Data;

/// <summary>
/// The sync's tables, defined here and created by EF migrations (Data/Migrations, applied at startup by
/// <see cref="SyncSchema"/>): one inbox table per synced object type. The claim's statements are raw SQL
/// (<see cref="InboxSql"/>) run through <c>Database.SqlQueryRaw</c>, since FOR UPDATE SKIP LOCKED and UPDATE ... RETURNING have no LINQ form.
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
    /// (due when, leased by whom); its key is the entity id. Columns with a database default are left out of
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
        // The entity id is the key: one row per entity (InboxSql.Store's on conflict), so no surrogate id and no second
        // unique index. Every claim and outcome finds its rows by it.
        entity.HasKey(r => r.EntityId).HasName($"{table}_pkey");
        entity.Property(r => r.Payload).HasColumnType("jsonb");
        // No default: every insert sets it from the event (InboxSql.Store)
        entity.Property(r => r.EventTime).IsRequired();
        entity.Property(r => r.Status).HasDefaultValueSql("'pending'");
        entity.Property(r => r.AttemptCount).HasDefaultValue(0);
        entity.Property(r => r.LastUpdated).HasDefaultValueSql("now()");
        // The claim (InboxSql.Claim): the due rows, the longest-waiting first. Idle rows (null) are not in the index.
        entity.HasIndex(r => r.NextAttemptAt).HasDatabaseName($"{table}_ready").HasFilter("next_attempt_at is not null");
    }
}