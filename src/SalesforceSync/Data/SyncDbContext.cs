using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Npgsql;
using SalesforceSync.Inbox;
using SalesforceSync.Sync;

namespace SalesforceSync.Data;

/// <summary>
/// The sync's tables as EF sees them. The schema itself stays sql/schema.sql (partial indexes, the table template,
/// views, an advisory lock: not expressible as migrations), so this model only describes it; an integration test
/// checks the two against a real database. The claim's statements are raw SQL (<see cref="InboxSql"/>) run through
/// <c>Database.SqlQueryRaw</c>, since FOR UPDATE SKIP LOCKED and UPDATE ... RETURNING have no LINQ form.
/// <para>
/// To sync another object: a subclass of <see cref="InboxRow"/>, a <c>DbSet</c> here and its
/// <see cref="ConfigureInbox{TRow}"/> line in <see cref="OnModelCreating"/>, with its table and the entity type its
/// rows have in sf_sync_state.
/// </para>
/// </summary>
public class SyncDbContext(DbContextOptions<SyncDbContext> options) : DbContext(options)
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

        var state = modelBuilder.Entity<SyncState>();
        state.ToTable("sf_sync_state");
        state.HasKey(s => new { s.EntityType, s.EntityId });
        state.Property(s => s.EntityType).HasColumnName("entity_type");
        state.Property(s => s.EntityId).HasColumnName("entity_id");
        state.Property(s => s.NextAttemptAt).HasColumnName("next_attempt_at");
        state.Property(s => s.LockedUntil).HasColumnName("locked_until");
        state.Property(s => s.LockToken).HasColumnName("lock_token");
        state.Property(s => s.SyncedTime).HasColumnName("synced_time");
    }

    /// <summary>
    /// The inbox template of sql/schema.sql. Columns with a database default are left out of an insert while they hold
    /// the CLR default, so a new row gets the database's clock and 'pending'.
    /// </summary>
    private static void ConfigureInbox<TRow>(EntityTypeBuilder<TRow> e, string table, string entityType) where TRow : InboxRow
    {
        e.ToTable(table).HasAnnotation(EntityTypeAnnotation, entityType);
        e.HasKey(r => r.Id);
        e.Property(r => r.Id).HasColumnName("id").ValueGeneratedOnAdd();
        e.Property(r => r.EntityId).HasColumnName("entity_id");
        e.Property(r => r.Payload).HasColumnName("payload").HasColumnType("jsonb");
        e.Property(r => r.Status).HasColumnName("status").HasDefaultValueSql("'pending'");
        e.Property(r => r.AttemptCount).HasColumnName("attempt_count").HasDefaultValue(0);
        e.Property(r => r.LastError).HasColumnName("last_error");
        e.Property(r => r.LastErrorCode).HasColumnName("last_error_code");
        e.Property(r => r.ReceivedAt).HasColumnName("received_at").HasDefaultValueSql("now()");
        e.Property(r => r.LastUpdated).HasColumnName("last_updated").HasDefaultValueSql("now()");
    }
}