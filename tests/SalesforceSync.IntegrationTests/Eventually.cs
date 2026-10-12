namespace SalesforceSync.IntegrationTests;

/// <summary>Polling helper for asserting on an asynchronous pipeline.</summary>
public static class Eventually
{
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan Interval = TimeSpan.FromMilliseconds(250);

    public static async Task<T> ValueAsync<T>(Func<Task<T>> probe, Func<T, bool> done, string what, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        T last;
        while (true)
        {
            last = await probe();
            if (done(last)) return last;
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"Timed out after {(timeout ?? DefaultTimeout).TotalSeconds}s waiting for {what}. Last value: {Describe(last)}");
            await Task.Delay(Interval);
        }
    }

    private static string Describe<T>(T value) => value switch
    {
        null => "null",
        System.Collections.IEnumerable items and not string => "[" + string.Join(", ", items.Cast<object?>()) + "]",
        _ => value.ToString() ?? ""
    };
}
