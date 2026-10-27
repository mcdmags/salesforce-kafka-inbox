using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.BulkTools;

/// <summary>
/// Bulk API 2.0 hard delete: one job takes any number of records of one object as CSV and Salesforce processes it in the
/// background, so a few calls do what Collections would do 200 records per call. Bulk API 2.0 and Bulk API Developer
/// Guide, "Ingest Jobs" (https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/bulk_api_2_0.htm).
/// </summary>
/// <param name="http">Authenticated client of the org (base address: its instance URL).</param>
/// <param name="dataPath">The REST path prefix, e.g. <c>/services/data/v62.0</c>.</param>
public sealed class BulkApiClient(HttpClient http, string dataPath)
{
    /// <summary>How often a job's state is checked while Salesforce processes it.</summary>
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(2);

    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };

    /// <summary>
    /// Hard-deletes the records with these Ids: they skip the Recycle Bin, so their storage is freed at once. Needs the
    /// "Bulk API Hard Delete" permission.
    /// </summary>
    /// <exception cref="SalesforceBulkJobException">The job failed or was aborted as a whole.</exception>
    public async Task<BulkJobResult> HardDeleteAsync(string sobject, IReadOnlyList<string> ids, CancellationToken ct)
    {
        if (ids.Count == 0)
        {
            throw new ArgumentException("No Ids to delete", nameof(ids));
        }

        var jobs = $"{dataPath}/jobs/ingest";
        var job = await SendJobAsync(HttpMethod.Post, jobs, new { @object = sobject, operation = "hardDelete", contentType = "CSV", lineEnding = "LF" }, ct);
        try
        {
            // The data, then "upload complete": Salesforce starts processing
            using (var upload = new HttpRequestMessage(HttpMethod.Put, $"{jobs}/{job.Id}/batches"))
            {
                upload.Content = new StringContent($"Id\n{string.Join('\n', ids)}\n", Encoding.UTF8, "text/csv");
                using var res = await http.SendAsync(upload, ct);
                await EnsureSuccessAsync(res, ct);
            }

            job = await SendJobAsync(HttpMethod.Patch, $"{jobs}/{job.Id}", new { state = "UploadComplete" }, ct);
        }
        catch
        {
            await AbortQuietlyAsync($"{jobs}/{job.Id}");
            throw;
        }

        while (job.State is not ("JobComplete" or "Failed" or "Aborted"))
        {
            await Task.Delay(PollInterval, ct);
            job = await SendJobAsync(HttpMethod.Get, $"{jobs}/{job.Id}", null, ct);
        }

        if (job.State != "JobComplete")
        {
            throw new SalesforceBulkJobException(job.Id, job.State, job.ErrorMessage);
        }

        var failures = job.NumberRecordsFailed > 0 ? await FailedResultsAsync($"{jobs}/{job.Id}/failedResults/", ct) : [];
        return new BulkJobResult(job.Id, job.NumberRecordsProcessed, failures);
    }

    private async Task<BulkJob> SendJobAsync(HttpMethod method, string url, object? body, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(method, url);
        if (body is not null)
        {
            request.Content = JsonContent.Create(body, options: Json);
        }

        using var res = await http.SendAsync(request, ct);
        await EnsureSuccessAsync(res, ct);
        return await res.Content.ReadFromJsonAsync<BulkJob>(Json, ct) ?? throw new InvalidOperationException("Empty job answer");
    }

    /// <summary>A job left open by a failed upload is aborted, so it doesn't wait in the org; a failure here changes nothing.</summary>
    private async Task AbortQuietlyAsync(string job)
    {
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Patch, job) { Content = JsonContent.Create(new { state = "Aborted" }, options: Json) };
            using var _ = await http.SendAsync(request, CancellationToken.None);
        }
        catch (HttpRequestException)
        {
        }
        catch (TaskCanceledException)
        {
        }
    }

    /// <summary>The records Salesforce couldn't process: CSV with <c>sf__Id</c>, <c>sf__Error</c> and the uploaded columns.</summary>
    private async Task<IReadOnlyList<BulkFailure>> FailedResultsAsync(string url, CancellationToken ct)
    {
        using var res = await http.GetAsync(url, ct);
        await EnsureSuccessAsync(res, ct);
        var rows = Csv.Parse(await res.Content.ReadAsStringAsync(ct));
        if (rows.Count == 0)
        {
            return [];
        }

        var header = rows[0];
        var id = header.IndexOf("Id") is >= 0 and var uploaded ? uploaded : header.IndexOf("sf__Id");
        var error = header.IndexOf("sf__Error");
        return [.. rows.Skip(1).Select(r => new BulkFailure(r[id], r[error]))];
    }

    private static async Task EnsureSuccessAsync(HttpResponseMessage res, CancellationToken ct)
    {
        if (!res.IsSuccessStatusCode)
        {
            throw new SalesforceResponseException(res.StatusCode, await res.Content.ReadAsStringAsync(ct));
        }
    }

    private sealed record BulkJob(string Id, string State, int NumberRecordsProcessed, int NumberRecordsFailed, string? ErrorMessage);
}

/// <summary>A processed job: records processed (failed ones included) and those that failed, with Salesforce's error.</summary>
public sealed record BulkJobResult(string JobId, int Processed, IReadOnlyList<BulkFailure> Failures)
{
    public int Succeeded => Processed - Failures.Count;
}

/// <param name="Id">The record's Id as uploaded.</param>
/// <param name="Error">Salesforce's error, e.g. <c>ENTITY_IS_DELETED:entity is deleted:--</c>.</param>
public sealed record BulkFailure(string Id, string Error);

/// <summary>A Bulk API job that failed or was aborted as a whole (e.g. an unknown object, a missing permission).</summary>
public sealed class SalesforceBulkJobException(string jobId, string state, string? error)
    : Exception($"Bulk job {jobId} {state}: {error ?? "no error message"}")
{
    public string JobId => jobId;
}

/// <summary>RFC 4180 CSV, as Bulk API writes it: quoted fields may hold commas, line breaks and doubled quotes.</summary>
public static class Csv
{
    public static List<List<string>> Parse(string text)
    {
        var rows = new List<List<string>>();
        var row = new List<string>();
        var field = new StringBuilder();
        var quoted = false;
        for (var i = 0; i < text.Length; i++)
        {
            var c = text[i];
            if (quoted)
            {
                if (c != '"')
                {
                    field.Append(c);
                }
                else if (i + 1 < text.Length && text[i + 1] == '"')
                {
                    field.Append('"');
                    i++;
                }
                else
                {
                    quoted = false;
                }
            }
            else if (c == '"')
            {
                quoted = true;
            }
            else if (c == ',')
            {
                row.Add(field.ToString());
                field.Clear();
            }
            else if (c is '\n' or '\r')
            {
                if (c == '\r' && i + 1 < text.Length && text[i + 1] == '\n')
                {
                    i++;
                }

                row.Add(field.ToString());
                field.Clear();
                rows.Add(row);
                row = [];
            }
            else
            {
                field.Append(c);
            }
        }

        if (field.Length > 0 || row.Count > 0)
        {
            row.Add(field.ToString());
            rows.Add(row);
        }

        return rows;
    }
}
