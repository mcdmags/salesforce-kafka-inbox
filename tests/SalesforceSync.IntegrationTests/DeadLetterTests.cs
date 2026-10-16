using System.Text.Json.Nodes;

namespace SalesforceSync.IntegrationTests;

/// <summary>Few, fast retries, bound from configuration like in a deployment, so a dead letter is reached in seconds.</summary>
public sealed class FewRetriesFixture : SyncFixture
{
    public const int MaxRetries = 3;

    public FewRetriesFixture()
    {
        WorkerSettings["Sync:MaxRetries"] = MaxRetries.ToString();
        WorkerSettings["Sync:RetryBaseDelay"] = "00:00:00.200";
        WorkerSettings["Sync:IdleDelay"] = "00:00:00.100";
    }
}

[CollectionDefinition(Name)]
public sealed class FewRetriesCollection : ICollectionFixture<FewRetriesFixture>
{
    public const string Name = "FewRetries";
}

[Collection(FewRetriesCollection.Name)]
public sealed class DeadLetterTests(FewRetriesFixture app)
{
    [Fact]
    public async Task Relationship_whose_Account_never_arrives_is_dead_lettered_after_MaxRetries_and_syncs_once_requeued()
    {
        var (parent, child, rel) = (SyncFixture.NewAccountId(), SyncFixture.NewAccountId(), SyncFixture.NewRelationshipId());

        await app.PublishRelationshipAsync(rel, 1, parent, child, new JsonObject { ["type"] = "Subsidiary" });

        (await app.WaitForInboxSettledAsync(rel, version: 1, SyncFixture.Relationships)).Should().Be((1L, "dead"));
        var dead = (await app.GetInboxRowsAsync(rel, SyncFixture.Relationships))[0];
        dead.ErrorCode.Should().Be("DEPENDENCY_NOT_SYNCED", "the cause stays on the dead letter");
        dead.AttemptCount.Should().Be(FewRetriesFixture.MaxRetries);
        (await app.GetRecordAsync("Account_Relationship__c", rel)).Should().BeNull();

        // The Accounts arrive after all; the dead letter is re-queued by hand (the statement from the README)
        await app.PublishAsync(parent, 1, new JsonObject { ["name"] = Any.String("Late parent ") }, []);
        await app.PublishAsync(child, 1, new JsonObject { ["name"] = Any.String("Late child ") }, []);
        await app.WaitForInboxSettledAsync(parent, 1);
        await app.WaitForInboxSettledAsync(child, 1);
        await app.ExecuteSqlAsync("""
            with requeued as (
              update sf_account_relationship_inbox set status = 'pending', attempt_count = 0
              where status = 'dead' and last_error_code = 'DEPENDENCY_NOT_SYNCED'
              returning entity_id
            )
            insert into sf_sync_state as s (entity_type, entity_id, next_attempt_at)
            select 'accountRelationship', entity_id, now() from requeued
            on conflict (entity_type, entity_id) do update set next_attempt_at = coalesce(s.next_attempt_at, now())
            """);

        (await app.WaitForInboxSettledAsync(rel, version: 1, SyncFixture.Relationships)).Should().Be((1L, "done"));
        ((string?)(await app.GetRecordAsync("Account_Relationship__c", rel))!["RelatedAccountId__c"]).Should().Be(child);
    }
}