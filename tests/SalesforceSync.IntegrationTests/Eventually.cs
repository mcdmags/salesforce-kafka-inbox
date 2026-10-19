namespace SalesforceSync.IntegrationTests;

public static class Eventually
{
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan Interval = TimeSpan.FromMilliseconds(250);

    public static async Task<T> ValueAsync<T>(Func<Task<T>> probe, Func<T, bool> done, string what, TimeSpan? timeout = null)
    {
        var limit = timeout ?? DefaultTimeout;
        var deadline = DateTime.UtcNow + limit;
        while (true)
        {
            var last = await probe();
            if (done(last))
            {
                return last;
            }

            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException($"Timed out after {limit.TotalSeconds}s waiting for {what}. Last value: {Describe(last)}");
            }

            await Task.Delay(Interval);
        }
    }

    private static string Describe<T>(T value)
    {
        return value switch
        {
            null => "null",
            System.Collections.IEnumerable items and not string => "[" + string.Join(", ", items.Cast<object?>()) + "]",
            _ => value.ToString() ?? ""
        };
    }
}