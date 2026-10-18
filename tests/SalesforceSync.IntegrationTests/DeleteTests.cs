using System.Text.Json.Nodes;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// Deletes end to end: an aggregate whose after no longer has a child (or no longer has the Account) -> a delete
/// event in the child's inbox -> the read finds the record's Id -> one sObject Collections delete. The mock org keeps
/// deleted records in its Recycle Bin and, set up so by <see cref="SyncFixture"/> (its automation, simulated),
/// refuses to delete an Account that records still name.
/// </summary>
[Collection(SyncCollection.Name)]
public sealed class DeleteTests(SyncFixture app)
{
    [Fact]
    public async Task Contact_removed_from_its_aggregate_is_deleted_an_older_aggregate_does_not_bring_it_back_and_a_newer_one_does()
    {
        var (acct, contact) = (SyncFixture.NewAccountId(), SyncFixture.NewContactId());
        var account = new JsonObject { ["name"] = Any.String("Acme ") };
        var withContact = SyncFixture.Snapshot(acct, account, contacts: [SyncFixture.Child(contact, new JsonObject { ["lastName"] = "Lovelace" })]);
        var withoutContact = SyncFixture.Snapshot(acct, account);

        await app.PublishAggregateAsync(acct, 1, null, withContact);
        (await app.WaitForInboxSettledAsync(contact, 1, SyncFixture.Contacts)).Should().Be((1L, "done"));
        var id = (string)(await app.GetRecordAsync("Contact", contact))!["Id"]!;

        await app.PublishAggregateAsync(acct, 3, withContact, withoutContact);
        (await app.WaitForInboxSettledAsync(contact, 3, SyncFixture.Contacts)).Should().Be((3L, "done"));
        (await app.GetRecordAsync("Contact", contact)).Should().BeNull("the contact is deleted");
        var delete = (await app.GetWriteRequestsAsync(id)).Should().ContainSingle().Subject;
        ((string?)delete["method"], (string?)delete["url"]).Should().Be(("DELETE", $"/services/data/v62.0/composite/sobjects?ids={id}&allOrNone=false"),
            "by the Id the read found, in one Collections delete");
        (await app.GetInboxAsync(acct)).Should().Be((1L, "done"), "the Account didn't change, so its row wasn't touched");

        // A late aggregate from before the delete still has the contact: older than the delete, so it changes nothing
        await app.PublishAggregateAsync(acct, 2, null, withContact);
        await app.WaitUntilConsumedAsync(acct);
        (await app.GetInboxAsync(contact, SyncFixture.Contacts)).Should().Be((3L, "done"));
        (await app.GetRecordAsync("Contact", contact)).Should().BeNull("an older insert doesn't resurrect it");

        // A newer aggregate has it again: created anew
        await app.PublishAggregateAsync(acct, 4, withoutContact, withContact);
        (await app.WaitForInboxSettledAsync(contact, 4, SyncFixture.Contacts)).Should().Be((4L, "done"));
        var recreated = await app.GetRecordAsync("Contact", contact);
        ((string?)recreated!["LastName"]).Should().Be("Lovelace");
        ((string?)recreated["Id"]).Should().NotBe(id, "a new record");
    }

    [Fact]
    public async Task Relationship_removed_from_its_aggregate_is_deleted_while_a_contact_added_by_the_same_aggregate_is_created()
    {
        var (acct, related, rel, contact) = (SyncFixture.NewAccountId(), SyncFixture.NewAccountId(), SyncFixture.NewRelationshipId(),
            SyncFixture.NewContactId());
        await app.PublishAsync(related, 1, new JsonObject { ["name"] = Any.String("Related ") }, []);
        await app.WaitForInboxSettledAsync(related, 1);
        var account = new JsonObject { ["name"] = Any.String("Acme ") };
        var relationship = SyncFixture.Child(rel, new JsonObject { ["relatedAccountExternalId"] = related, ["type"] = "Partner" });
        var v1 = SyncFixture.Snapshot(acct, account, relationships: [relationship]);

        await app.PublishAggregateAsync(acct, 1, null, v1);
        (await app.WaitForInboxSettledAsync(rel, 1, SyncFixture.Relationships)).Should().Be((1L, "done"));

        await app.PublishAggregateAsync(acct, 2, v1,
            SyncFixture.Snapshot(acct, account, contacts: [SyncFixture.Child(contact, new JsonObject { ["lastName"] = "New" })]));
        (await app.WaitForInboxSettledAsync(rel, 2, SyncFixture.Relationships)).Should().Be((2L, "done"));
        (await app.WaitForInboxSettledAsync(contact, 2, SyncFixture.Contacts)).Should().Be((2L, "done"));
        (await app.GetRecordAsync("Account_Relationship__c", rel)).Should().BeNull();
        (await app.GetRecordAsync("Contact", contact)).Should().NotBeNull();
        (await app.GetRecordAsync("Account", related)).Should().NotBeNull("the other Account is only named by the relationship");
    }

    [Fact]
    public async Task A_removed_child_that_was_never_synced_is_done_without_a_delete_call()
    {
        var (acct, contact) = (SyncFixture.NewAccountId(), SyncFixture.NewContactId());
        var before = await DeleteRequestsAsync();

        await app.PublishAggregateAsync(acct, 2,
            SyncFixture.Snapshot(acct, [], contacts: [SyncFixture.Child(contact, new JsonObject { ["lastName"] = "Never synced" })]),
            SyncFixture.Snapshot(acct, []));

        (await app.WaitForInboxSettledAsync(contact, 2, SyncFixture.Contacts)).Should().Be((2L, "done"), "not in Salesforce: nothing to delete");
        (await app.GetInboxRowAsync(contact, SyncFixture.Contacts))!.Error.Should().BeNull();
        (await DeleteRequestsAsync()).Should().Be(before, "the read found nothing, so no delete was sent");
    }

    [Fact]
    public async Task A_delete_the_org_refuses_permanently_is_dead_lettered_and_the_record_stays()
    {
        var (acct, contact) = (SyncFixture.NewAccountId(), SyncFixture.NewContactId());
        var withContact = SyncFixture.Snapshot(acct, new JsonObject { ["name"] = Any.String("Acme ") },
            contacts: [SyncFixture.Child(contact, new JsonObject { ["lastName"] = "Kept" })]);
        await app.PublishAggregateAsync(acct, 1, null, withContact);
        await app.WaitForInboxSettledAsync(contact, 1, SyncFixture.Contacts);
        await app.InjectFaultAsync("Contact", contact, "INSUFFICIENT_ACCESS_OR_READONLY", times: 1);

        await app.PublishAggregateAsync(acct, 2, withContact, SyncFixture.Snapshot(acct, new JsonObject { ["name"] = withContact["name"]!.DeepClone() }));

        (await app.WaitForInboxSettledAsync(contact, 2, SyncFixture.Contacts)).Should().Be((2L, "dead"));
        (await app.GetInboxRowAsync(contact, SyncFixture.Contacts))!.Error.Should().Contain("INSUFFICIENT_ACCESS_OR_READONLY");
        (await app.GetRecordAsync("Contact", contact)).Should().NotBeNull();
    }

    [Fact]
    public async Task A_delete_whose_answer_is_lost_is_held_and_then_done_without_deleting_twice()
    {
        var (acct, contact) = (SyncFixture.NewAccountId(), SyncFixture.NewContactId());
        var account = new JsonObject { ["name"] = Any.String("Acme ") };
        var withContact = SyncFixture.Snapshot(acct, account, contacts: [SyncFixture.Child(contact, new JsonObject { ["lastName"] = "Gone" })]);
        await app.PublishAggregateAsync(acct, 1, null, withContact);
        await app.WaitForInboxSettledAsync(contact, 1, SyncFixture.Contacts);
        await app.LoseResponsesAsync("Contact", contact);

        await app.PublishAggregateAsync(acct, 2, withContact, SyncFixture.Snapshot(acct, account));

        var held = (await Eventually.ValueAsync(() => app.GetInboxRowAsync(contact, SyncFixture.Contacts),
            row => row?.Error?.Contains("UNKNOWN_OUTCOME") == true, "the delete's outcome to be recorded as unknown"))!;
        (held.Status, held.AttemptCount, held.LockToken).Should().Be(("pending", 0, null),
            "held like an unknown upsert: a late delete must not remove a record a newer event recreated");
        (await app.GetRecordAsync("Contact", contact)).Should().BeNull("the org did delete it");

        (await app.WaitForInboxSettledAsync(contact, 2, SyncFixture.Contacts)).Should().Be((2L, "done"),
            "after the hold, the read finds nothing left to delete");
    }

    [Fact]
    public async Task A_deleted_Account_is_deleted_with_its_Profile_contacts_and_relationships()
    {
        var (acct, related, rel, contact) = (SyncFixture.NewAccountId(), SyncFixture.NewAccountId(), SyncFixture.NewRelationshipId(),
            SyncFixture.NewContactId());
        await app.PublishAsync(related, 1, new JsonObject { ["name"] = Any.String("Related ") }, []);
        await app.WaitForInboxSettledAsync(related, 1);
        var account = new JsonObject
        {
            ["name"] = Any.String("Acme "),
            ["profile"] = new JsonObject { ["externalId"] = SyncFixture.ProfileIdFor(acct), ["tier"] = "Gold" }
        };
        var v1 = SyncFixture.Snapshot(acct, account,
            relationships: [SyncFixture.Child(rel, new JsonObject { ["relatedAccountExternalId"] = related })],
            contacts: [SyncFixture.Child(contact, new JsonObject { ["lastName"] = "Child" })]);
        await app.PublishAggregateAsync(acct, 1, null, v1);
        await app.WaitForInboxSettledAsync(acct, 1);
        await app.WaitForInboxSettledAsync(rel, 1, SyncFixture.Relationships);
        await app.WaitForInboxSettledAsync(contact, 1, SyncFixture.Contacts);
        (await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(acct))).Should().NotBeNull();

        await app.PublishAggregateAsync(acct, 2, v1, null);

        // The three inboxes run independently: the Account's delete is refused while its children still name it, and
        // retried (DEPENDENCY_NOT_SYNCED) until their own deletes are through
        (await app.WaitForInboxSettledAsync(acct, 2)).Should().Be((2L, "done"));
        (await app.WaitForInboxSettledAsync(rel, 2, SyncFixture.Relationships)).Should().Be((2L, "done"));
        (await app.WaitForInboxSettledAsync(contact, 2, SyncFixture.Contacts)).Should().Be((2L, "done"));
        (await app.GetRecordAsync("Account", acct)).Should().BeNull();
        (await app.GetRecordAsync("Profile__c", SyncFixture.ProfileIdFor(acct))).Should().BeNull();
        (await app.GetRecordAsync("Contact", contact)).Should().BeNull();
        (await app.GetRecordAsync("Account_Relationship__c", rel)).Should().BeNull();
        (await app.GetRecordAsync("Account", related)).Should().NotBeNull("the other Account of the relationship is not touched");
    }

    private async Task<long> DeleteRequestsAsync()
    {
        return (await app.GetStatsAsync()).GetProperty("deleteRequests").GetInt64();
    }
}