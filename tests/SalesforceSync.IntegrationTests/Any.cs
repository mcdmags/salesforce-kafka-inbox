using AutoFixture;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// Anonymous AutoFixture test data: values a test needs unique or present, whose content doesn't matter (record keys,
/// names, numbers only written and read back). Collections run in parallel, so the shared fixture is locked.
/// </summary>
public static class Any
{
    private static readonly Fixture Fixture = new();
    private static readonly object Gate = new();

    public static T Create<T>()
    {
        lock (Gate)
        {
            return Fixture.Create<T>();
        }
    }

    /// <summary>A unique string starting with <paramref name="prefix"/> (followed by an AutoFixture string, a GUID).</summary>
    public static string String(string prefix)
    {
        lock (Gate)
        {
            return prefix + Fixture.Create<string>();
        }
    }
}