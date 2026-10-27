using System.Text;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using static SalesforceSync.FakeSalesforce.FakeJson;

namespace SalesforceSync.FakeSalesforce;

/// <summary>
/// Bulk API 2.0 ingest jobs, delete and hardDelete only: create a job, upload its Ids as CSV, close it ("upload
/// complete"), read its state and its failed records. A closed job is processed at once, as Collections deletes of
/// 200 (the same checks and faults); a hardDelete also takes the records out of the Recycle Bin.
/// </summary>
public sealed partial class FakeSalesforceStore
{
    private readonly Dictionary<string, Job> _jobs = new(StringComparer.Ordinal);
    private long _jobCounter;

    private sealed class Job(string id, string sobject, string operation)
    {
        public string Id { get; } = id;
        public string Object { get; } = sobject;
        public string Operation { get; } = operation;
        public string State { get; set; } = "Open";
        public List<string> Ids { get; } = [];
        public int Processed { get; set; }
        public List<(string Id, string Error)> Failed { get; } = [];
        public string? ErrorMessage { get; set; }
    }

    /// <summary>The job Id and part of an ingest path (<c>.../jobs/ingest[/{id}[/batches|/failedResults]]</c>), or null.</summary>
    public static (string? JobId, string? Part)? IngestPath(string path)
    {
        var match = IngestUrl().Match(path);
        return !match.Success ? null
            : (match.Groups["id"].Success ? match.Groups["id"].Value : null, match.Groups["part"].Success ? match.Groups["part"].Value : null);
    }

    [GeneratedRegex(@"/services/data/v\d+\.\d+/jobs/ingest(?:/(?<id>[^/]+)(?:/(?<part>batches|failedResults))?)?/?$", RegexOptions.IgnoreCase)]
    private static partial Regex IngestUrl();

    public JsonObject CreateIngestJob(JsonObject body)
    {
        var sobject = Str(body["object"]) ?? throw new FakeSalesforceException(400, "INVALIDJOB", "object is required");
        var operation = Str(body["operation"]);
        if (operation is not ("delete" or "hardDelete"))
        {
            throw new FakeSalesforceException(400, "INVALIDJOB", $"The mock org runs delete and hardDelete jobs, not {operation}");
        }

        lock (_gate)
        {
            var job = new Job(FakeRecords.Id18($"750FAKE{++_jobCounter:D8}"), sobject, operation);
            _jobs[job.Id] = job;
            return Info(job);
        }
    }

    /// <summary>The job's data: CSV with an <c>Id</c> header, one Id a line.</summary>
    public void UploadIngestJob(string jobId, string csv)
    {
        lock (_gate)
        {
            var job = JobOf(jobId);
            if (job.State != "Open")
            {
                throw new FakeSalesforceException(409, "INVALIDJOBSTATE", $"Job {jobId} is {job.State}: data can only be uploaded to an open job");
            }

            job.Ids.AddRange(csv.Split('\n').Select(l => l.Trim().Trim('"')).Where(l => l.Length > 0 && !l.Equals("Id", StringComparison.OrdinalIgnoreCase)));
        }
    }

    /// <summary>"UploadComplete" processes the job; "Aborted" drops it.</summary>
    public JsonObject UpdateIngestJob(string jobId, JsonObject body)
    {
        lock (_gate)
        {
            var job = JobOf(jobId);
            switch (Str(body["state"]))
            {
                case "UploadComplete" when job.State == "Open":
                    Process(job);
                    break;
                case "Aborted" when job.State is "Open" or "UploadComplete":
                    job.State = "Aborted";
                    break;
                default:
                    throw new FakeSalesforceException(409, "INVALIDJOBSTATE", $"Job {jobId} is {job.State}: can't move it to {Str(body["state"])}");
            }

            return Info(job);
        }
    }

    public JsonObject IngestJob(string jobId)
    {
        lock (_gate)
        {
            return Info(JobOf(jobId));
        }
    }

    /// <summary>The failed records as Bulk API lists them: <c>"sf__Id","sf__Error",Id</c>.</summary>
    public string IngestJobFailedResults(string jobId)
    {
        lock (_gate)
        {
            var csv = new StringBuilder("\"sf__Id\",\"sf__Error\",Id\n");
            foreach (var (id, error) in JobOf(jobId).Failed)
            {
                csv.Append($"\"\",\"{error.Replace("\"", "\"\"")}\",{id}\n");
            }

            return csv.ToString();
        }
    }

    private Job JobOf(string jobId)
    {
        return _jobs.TryGetValue(jobId, out var job) ? job : throw new FakeSalesforceException(404, "NOT_FOUND", $"Job {jobId} not found");
    }

    private void Process(Job job)
    {
        if (!FakeSchema.Objects.ContainsKey(job.Object))
        {
            job.State = "Failed";
            job.ErrorMessage = $"InvalidJob : Unable to find object: {job.Object}";
            return;
        }

        foreach (var chunk in job.Ids.Chunk(FakeCollectionWriter.MaxRecords))
        {
            // Ids of another object than the job's are refused, as a job is about one object
            var ofObject = chunk.Where(id => _records.FindById(FakeRecords.WellFormedId(id) ?? id) is not var (sobject, _)
                                             || sobject.Equals(job.Object, StringComparison.OrdinalIgnoreCase)).ToList();
            foreach (var other in chunk.Except(ofObject))
            {
                job.Failed.Add((other, $"INVALID_CROSS_REFERENCE_KEY:invalid cross reference id:--"));
            }

            var results = ofObject.Count == 0 ? [] : _collections.Delete(ofObject, allOrNone: false);
            for (var i = 0; i < ofObject.Count; i++)
            {
                if (results[i]!["success"]!.GetValue<bool>())
                {
                    if (job.Operation == "hardDelete")
                    {
                        _records.Purge(FakeRecords.WellFormedId(ofObject[i])!);
                    }
                }
                else
                {
                    var error = results[i]!["errors"]![0]!;
                    job.Failed.Add((ofObject[i], $"{Str(error["statusCode"])}:{Str(error["message"])}:--"));
                }
            }
        }

        job.Processed = job.Ids.Count;
        job.State = "JobComplete";
    }

    private static JsonObject Info(Job job)
    {
        var info = new JsonObject
        {
            ["id"] = job.Id,
            ["object"] = job.Object,
            ["operation"] = job.Operation,
            ["state"] = job.State,
            ["numberRecordsProcessed"] = job.Processed,
            ["numberRecordsFailed"] = job.Failed.Count
        };
        if (job.ErrorMessage is not null)
        {
            info["errorMessage"] = job.ErrorMessage;
        }

        return info;
    }
}
