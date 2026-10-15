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
        ConfigureInbox(modelBuilder.Entity<AccountInboxRow>(), "sf_account_inbox", AccountSync.Type);
        ConfigureInbox(modelBuilder.Entity<AccountRelationshipInboxRow>(), "sf_account_relationship_inbox", AccountRelationshipSync.Type);
        ConfigureInbox(modelBuilder.Entity<ContactInboxRow>(), "sf_contact_inbox", ContactSync.Type);

        // Every synced entity's sync state, one row per (entity type, entity id) for all object types. Claiming an
        // entity is locking and leasing its row here; the inboxes hold the events only.
        var state = modelBuilder.Entity<SyncState>();
        state.ToTable("sf_sync_state");
        VacuumOften(state);
        state.HasKey(s => new { s.EntityType, s.EntityId }).HasName("sf_sync_state_pkey");
        // The claim (InboxSql.ClaimEntities): an entity type's due entities, the longest-waiting first. Idle entities
        // (null) are not in the index.
        state.HasIndex(s => new { s.EntityType, s.NextAttemptAt }).HasDatabaseName("sf_sync_state_ready")
            .HasFilter("next_attempt_at is not null");

        // The shared token bucket and daily budget guard: one row, created here. Bucket size, refill rate and budget
        // are set from configuration at worker startup (RateLimitInitializer).
        var limit = modelBuilder.Entity<RateLimitState>();
        limit.ToTable("sf_rate_limit");
        limit.HasKey(r => r.Name).HasName("sf_rate_limit_pkey");
        limit.Property(r => r.UpdatedAt).HasDefaultValueSql("clock_timestamp()");
        limit.HasData(new { Name = "salesforce", Tokens = 5.0, Capacity = 5.0, BaseRefillPerSec = 2.0, RefillPerSec = 2.0 });

        var usage = modelBuilder.Entity<ApiUsageBucket>();
        usage.ToTable("sf_api_usage");
        usage.HasKey(u => u.Bucket).HasName("sf_api_usage_pkey");

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
    private static void ConfigureInbox<TRow>(EntityTypeBuilder<TRow> e, string table, string entityType) where TRow : InboxRow
    {
        e.ToTable(table).HasAnnotation(EntityTypeAnnotation, entityType);
        VacuumOften(e);
        e.HasKey(r => r.Id).HasName($"{table}_pkey");
        e.Property(r => r.Id).UseSerialColumn();
        e.Property(r => r.Payload).HasColumnType("jsonb");
        e.Property(r => r.Status).HasDefaultValueSql("'pending'");
        e.Property(r => r.AttemptCount).HasDefaultValue(0);
        e.Property(r => r.ReceivedAt).HasDefaultValueSql("now()");
        e.Property(r => r.LastUpdated).HasDefaultValueSql("now()");
        // An entity's pending events in arrival order: the events a claim takes (InboxSql.ClaimEvents), and whether any
        // are left when its lease ends
        e.HasIndex(r => new { r.EntityId, r.Id }).HasDatabaseName($"{table}_pending").HasFilter("status = 'pending'");
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