using SalesforceSync.FakeSalesforce;
using SalesforceSync.Hosting;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.Tests;

/// <summary>
/// Hard delete with one Bulk API 2.0 job (<see cref="SalesforceRestClient.HardDeleteAsync"/>) against the mock org, and
/// the Ids file the bulk-delete command reads.
/// </summary>
public sealed class BulkDeleteTests
{
    private readonly FakeSalesforceStore _org = new(new FakeSalesforceOptions { EnforceDailyLimit = false });
    private readonly SalesforceRestClient _client;

    public BulkDeleteTests()
    {
        _client = SalesforceStep.Client(SalesforceStep.Http(new FakeSalesforceHandler(_org)));
    }

    [Fact]
    public async Task A_hard_delete_removes_the_records_and_leaves_nothing_in_the_Recycle_Bin()
    {
        var ids = await UpsertAsync("Account", "A1", "A2", "A3");

        var result = await _client.HardDeleteAsync("Account", ids, CancellationToken.None);

        (result.Processed, result.Succeeded, result.Failures.Count).Should().Be((3, 3, 0));
        _org.All("Account").Should().BeEmpty();
        var again = await _client.DeleteAsync(ids, CancellationToken.None);
        again.Select(r => r.Errors.Single().Code).Should().OnlyContain(c => c == "INVALID_CROSS_REFERENCE_KEY",
            "a hard-deleted Id is unknown, not ENTITY_IS_DELETED as one in the Recycle Bin");
    }

    [Fact]
    public async Task Ids_that_can_t_be_deleted_are_reported_one_by_one_and_the_others_are_deleted()
    {
        var accounts = await UpsertAsync("Account", "A1", "A2");
        var contact = (await UpsertAsync("Contact", "C1")).Single();
        var unknown = FakeRecords.Id18("001FAKE99999999");

        var result = await _client.HardDeleteAsync("Account", [accounts[0], unknown, contact, accounts[1]], CancellationToken.None);

        (result.Processed, result.Succeeded).Should().Be((4, 2));
        result.Failures.Select(f => f.Id).Should().BeEquivalentTo([unknown, contact]);
        result.Failures.Should().OnlyContain(f => f.Error.StartsWith("INVALID_CROSS_REFERENCE_KEY:"));
        _org.All("Account").Should().BeEmpty();
        _org.All("Contact").Should().ContainSingle("a job deletes records of its own object only");
    }

    [Fact]
    public async Task A_job_on_an_unknown_object_fails_as_a_whole()
    {
        var ids = await UpsertAsync("Account", "A1");

        var delete = () => _client.HardDeleteAsync("Nope__c", ids, CancellationToken.None);

        (await delete.Should().ThrowAsync<SalesforceBulkJobException>()).Which.Message.Should().Contain("Failed").And.Contain("Nope__c");
        _org.All("Account").Should().ContainSingle();
    }

    [Fact]
    public void The_Ids_file_skips_a_header_comments_blank_lines_quotes_and_repeats_and_reports_what_isn_t_an_Id()
    {
        string[] lines = ["Id", "# accounts to remove", "", "  001000000000001AAA ", "\"001000000000002\"", "001000000000001AAA", "not-an-id", "0010000000000"];

        var (ids, invalid) = BulkDeleteCommand.ReadIds(lines);

        ids.Should().Equal("001000000000001AAA", "001000000000002");
        invalid.Should().Equal("not-an-id", "0010000000000");
    }

    private async Task<List<string>> UpsertAsync(string sobject, params string[] keys)
    {
        var records = keys.Select(k => sobject == "Contact"
            ? new SObjectBody(sobject) { ["ExternalId__c"] = k, ["LastName"] = $"Contact {k}" }
            : new SObjectBody(sobject) { ["ExternalId__c"] = k, ["Name"] = $"Account {k}" }).ToList();
        var results = await _client.UpsertAsync(sobject, "ExternalId__c", records, CancellationToken.None);
        results.Should().OnlyContain(r => r.Success);
        return [.. results.Select(r => r.Id!)];
    }
}
