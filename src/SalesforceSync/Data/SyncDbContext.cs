using System.Text.RegularExpressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Npgsql;
using SalesforceSync.Inbox;
using SalesforceSync.RateLimiting;
using SalesforceSync.Sync;

namespace SalesforceSync.Data;

/// <summary>
/// The sync's tables, defined here and created by EF migrations (Data/Migrations, applied at startup by
/// <see cref="SyncSchema"/>): one inbox table per synced object type, sf_sync_state, and the rate limiter's
/// sf_rate_limit and sf_api_usage. The claim's statements are raw SQL (<see cref="InboxSql"/>) run through
/// <c>Database.SqlQueryRaw</c>, since FOR UPDATE SKIP LOCKED and UPDATE ... RETURNING have no LINQ form.
/// <para>
/// To sync another object: a subclass of <see cref="InboxRow"/>, a <c>DbSet</c> here and its
/// <see cref="ConfigureInbox{TRow}"/> line in <see cref="OnModelCreating"/>, with its table and the entity type its
/// rows have in sf_sync_state; then a migration (<c>dotnet ef migrations add ...</c>) creates the table.
/// </para>
/// </summary>
public partial class SyncDbContext(DbContextOptions<SyncDbContext> options) : DbContext(options)
{
    private const string EntityTypeAnnotation = "SalesforceSync:EntityType";

    public DbSet<AccountInboxRow> AccountInbox => Set<AccountInboxRow>();

    public DbSet<AccountRelationshipInboxRow> AccountRelationshipInbox => Set<AccountRelationshipInboxRow>();

    public DbSet<ContactInboxRow> ContactInbox => Set<ContactInboxRow>();

    public DbSet<SyncState> SyncStates => Set<SyncState>();

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

    /// <summary>The entity type of a row type's entities in sf_sync_state (its strategy's type name, e.g. "account").</summary>
    public string EntityTypeOf<TRow>() where TRow : InboxRow
    {
        return Model.FindEntityType(typeof(TRow))?.FindAnnotation(EntityTypeAnnotation)?.Value as string
               ?? throw new InvalidOperationException($"{typeof(TRow).Name} is not mapped in {nameof(SyncDbContext)}");
    }

    /// <summary>The factory the worker, the benchmarks and the tests share: contexts pooled over <paramref name="db"/>.</summary>
    public static IDbContextFactory<SyncDbContext> CreateFactory(NpgsqlDataSource db, PostgresRetryOptions? retry = null)
    {
        var options = new DbContextOptionsBuilder<SyncDbContext>();
        Configure(options, db, retry ?? new PostgresRetryOptions());
        return new PooledDbContextFactory<SyncDbContext>(options.Options);
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
        modelBuilder.Entity<AccountInboxRow>(entity => ConfigureInbox(entity, "sf_account_inbox", AccountSync.Type));
        modelBuilder.Entity<AccountRelationshipInboxRow>(entity =>
            ConfigureInbox(entity, "sf_account_relationship_inbox", AccountRelationshipSync.Type));
        modelBuilder.Entity<ContactInboxRow>(entity => ConfigureInbox(entity, "sf_contact_inbox", ContactSync.Type));

        // Every synced entity's sync state, one row per (entity type, entity id) for all object types. Claiming an
        // entity is locking and leasing its row here; the inboxes hold the events only.
        modelBuilder.Entity<SyncState>(entity =>
        {
            entity.ToTable("sf_sync_state");
            VacuumOften(entity);
            entity.HasKey(s => new { s.EntityType, s.EntityId }).HasName("sf_sync_state_pkey");
            // The claim (InboxSql.ClaimEntities): an entity type's due entities, the longest-waiting first. Idle
            // entities (null) are not in the index.
            entity.HasIndex(s => new { s.EntityType, s.NextAttemptAt }).HasDatabaseName("sf_sync_state_ready")
                .HasFilter("next_attempt_at is not null");
        });

        // The shared token bucket and daily budget guard: one row, created here. Bucket size, refill rate and budget
        // are set from configuration at worker startup (RateLimitInitializer).
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
    /// An inbox table: one row per Kafka event and what became of it; the id is a bigserial. Columns with a database
    /// default are left out of an insert while they hold the CLR default, so a new row gets the database's clock and
    /// 'pending'.
    /// </summary>
    private static void ConfigureInbox<TRow>(EntityTypeBuilder<TRow> entity, string table, string entityType) where TRow : InboxRow
    {
        entity.ToTable(table).HasAnnotation(EntityTypeAnnotation, entityType);
        VacuumOften(entity);
        entity.HasKey(r => r.Id).HasName($"{table}_pkey");
        entity.Property(r => r.Id).UseSerialColumn();
        entity.Property(r => r.Payload).HasColumnType("jsonb");
        // No default: every insert sets it from the event (InboxSql.Store)
        entity.Property(r => r.EventTime).IsRequired();
        entity.Property(r => r.Status).HasDefaultValueSql("'pending'");
        entity.Property(r => r.AttemptCount).HasDefaultValue(0);
        entity.Property(r => r.ReceivedAt).HasDefaultValueSql("now()");
        entity.Property(r => r.LastUpdated).HasDefaultValueSql("now()");
        // An entity's pending events in arrival order: the events a claim takes (InboxSql.ClaimEvents), and whether any
        // are left when its lease ends
        entity.HasIndex(r => new { r.EntityId, r.Id }).HasDatabaseName($"{table}_pending").HasFilter("status = 'pending'");
    }

    /// <summary>
    /// For the hot tables (the inboxes, sf_sync_state): vacuum after 2% (plus 1,000 rows) of the table is dead instead
    /// of the default 20%. Every event is updated at least twice (claimed, then finished), every stored event, claim and
    /// outcome updates its entity's state row, and the claims read the front of the pending and ready indexes, where the
    /// dead index entries those updates leave behind collect. The default lets that grow with the table.
    /// </summary>
    private static void VacuumOften(EntityTypeBuilder entity)
    {
        entity.HasStorageParameter("autovacuum_vacuum_scale_factor", 0.02);
        entity.HasStorageParameter("autovacuum_vacuum_threshold", 1000);
    }
}