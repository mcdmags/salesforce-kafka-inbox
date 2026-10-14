using System.Text.Json.Nodes;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// The non-default write settings, bound from configuration like in a deployment: parents linked by external id,
/// allOrNone, event type mismatches dead-lettered, and few, fast retries so a dead letter is reached in seconds.
/// </summary>
public sealed class AlternativeSettingsFixture : SyncFixture
{
    public const int MaxRetries = 3;

    public AlternativeSettingsFixture()
    {
        WorkerSettings["Salesforce:ParentLink"] = "ExternalId";
        WorkerSettings["Salesforce:AllOrNone"] = "true";
        WorkerSettings["Sync:EventTypeMismatch"] = "DeadLetter";
        WorkerSettings["Sync:MaxRetries"] = MaxRetries.ToString();
        WorkerSettings["Sync:RetryBaseDelay"] = "00:00:00.200";
        WorkerSettings["Sync:IdleDelay"] = "00:00:00.100";
    }
}

[CollectionDefinition(Name)]
public sealed class AlternativeSettingsCollection : ICollectionFixture<AlternativeSettingsFixture>
{
    public const string Name = "AlternativeSettings";
}

[Collection(AlternativeSettingsCollection.Name)]
public sealed class AlternativeSettingsTests(AlternativeSettingsFixture app)
{
    [Fact]
    public async Task New_Account_and_Profile_are_linked_through_the_Accounts_external_id_with_allOrNone()
    {
        var acct = SyncFixture.NewAccountId();

        await app.PublishAsync(acct, 1, new JsonObject { ["name"] = Any.String("Acme ") }, new JsonObject { ["tier"] = "Gold" });
        (await app.WaitForInboxSettledAsync(acct, expectedRows: 1)).Should().Equal((1L, "done"));

        var write = (await app.GetWriteRequestsAsync(acct)).Should().ContainSingle().Subject;
        write["allOrNone"]!.GetValue<bool>().Should().BeTrue();
        var subrequests = write["compositeRequest"]!.AsArray();
        subrequests.Select(s => ((string?)s!["referenceId"], s["body"]!["allOrNone"]!.GetValue<bool>()))
            .Should().Equal(("Account_create", true), ("Profile__c_create", true));
        var profile = subrequests[1]!["body"]!["records"]!.AsArray()
            .Single(r => (string?)r!["External_Id__c"] == SyncFixture.ProfileIdFor(acct))!.AsObject();
        profile["Account__r"]!.ToJsonString().Should().Be($$"""{"External_Id__c":"{{acct}}"}""");
        profile.ContainsKey("Account__c").Should().BeFalse();

        ((string?)(await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(acct)))!["Account__c"])
            .Should().Be((string?)(await app.GetRecordAsync("Account", acct))!["Id"]);
    }

    [Fact]
    public async Task Relationship_whose_Account_never_arrives_is_dead_lettered_after_MaxRetries_and_syncs_once_requeued()
    {
        var (parent, child, rel) = (SyncFixture.NewAccountId(), SyncFixture.NewAccountId(), SyncFixture.NewRelationshipId());

        await app.PublishRelationshipAsync(rel, 1, parent, child, new JsonObject { ["type"] = "Subsidiary" });

        (await app.WaitForInboxSettledAsync(rel, 1, SyncFixture.Relationships)).Should().Equal((1L, "dead"));
        var dead = (await app.GetInboxRowsAsync(rel, SyncFixture.Relationships))[0];
        dead.ErrorCode.Should().Be("DEPENDENCY_NOT_SYNCED", "the cause stays on the dead letter");
        dead.RetryCount.Should().Be(AlternativeSettingsFixture.MaxRetries);
        (await app.GetRecordAsync("Account_Relationship__c", rel)).Should().BeNull();

        // The Accounts arrive after all; the dead letter is re-queued by hand (the statement from the README)
        await app.PublishAsync(parent, 1, new JsonObject { ["name"] = Any.String("Late parent ") }, []);
        await app.PublishAsync(child, 1, new JsonObject { ["name"] = Any.String("Late child ") }, []);
        await app.WaitForInboxSettledAsync(parent, 1);
        await app.WaitForInboxSettledAsync(child, 1);
        await app.ExecuteSqlAsync("""
            update sf_account_relationship_inbox
            set status = 'pending', retry_count = 0, next_attempt_at = now(), locked_until = null, lock_token = null
            where status = 'dead' and last_error_code = 'DEPENDENCY_NOT_SYNCED'
            """);

        (await app.WaitForInboxSettledAsync(rel, 1, SyncFixture.Relationships)).Should().Equal((1L, "done"));
        ((string?)(await app.GetRecordAsync("Account_Relationship__c", rel))!["Related_Account__c"])
            .Should().Be((string?)(await app.GetRecordAsync("Account", child))!["Id"]);
    }

    [Fact]
    public async Task Event_whose_type_disagrees_with_Salesforce_is_dead_lettered()
    {
        var (missing, existing) = (SyncFixture.NewAccountId(), SyncFixture.NewAccountId());

        // "updated", but Salesforce has no such Account
        await app.PublishAsync(missing, 2, new JsonObject { ["name"] = "v2" }, []);
        (await app.WaitForInboxSettledAsync(missing, expectedRows: 1)).Should().Equal((2L, "dead"));
        (await app.GetInboxRowsAsync(missing))[0].ErrorCode.Should().Be("EVENT_TYPE_MISMATCH");
        (await app.GetRecordAsync("Account", missing)).Should().BeNull();

        // "inserted", but the Account is already there
        await app.PublishAsync(existing, 1, new JsonObject { ["name"] = "v1" }, []);
        await app.WaitForInboxSettledAsync(existing, expectedRows: 1);
        var again = SyncFixture.AccountEvent(existing, 2, new JsonObject { ["name"] = "v2" }, []);
        again["meta"] = SyncFixture.Meta(2, "inserted");
        await app.PublishRawAsync(existing, again.ToJsonString());

        (await app.WaitForInboxSettledAsync(existing, expectedRows: 2)).Should().Equal((1L, "done"), (2L, "dead"));
        (await app.GetInboxRowsAsync(existing))[1].ErrorCode.Should().Be("EVENT_TYPE_MISMATCH");
        ((string?)(await app.GetRecordAsync("Account", existing))!["Name"]).Should().Be("v1");
    }
}