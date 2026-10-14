namespace SalesforceSync.FakeSalesforce;

public sealed class FakeSalesforceException(int status, string errorCode, string message) : Exception(message)
{
    public int Status { get; } = status;
    public string ErrorCode { get; } = errorCode;
}

/// <summary>
/// The org processed the request but the client never gets the answer. <see cref="FakeSalesforceHandler"/> lets it
/// through as a connection that ended early, which is what an in-process client sees; the HTTP server aborts the
/// connection instead.
/// </summary>
public sealed class FakeResponseLostException()
    : HttpRequestException(HttpRequestError.ResponseEnded, "The connection was closed before the response arrived (injected)");