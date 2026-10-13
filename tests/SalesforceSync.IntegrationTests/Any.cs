using AutoFixture;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// Anonymous test data from AutoFixture: values a test needs to be unique or present, but whose content doesn't
/// matter to it (record keys, names, numbers that are only written and read back). Collections run in parallel, so
/// the shared fixture is locked.
/// </summary>
public static class Any
{
    private static readonly Fixture Fixture = new();
    private static readonly object Gate = new();

    public static T Create<T>()
    {
        lock (Gate) return Fixture.Create<T>();
    }

    /// <summary>A unique string starting with <paramref name="prefix"/> (followed by an AutoFixture string, a GUID).</summary>
    public static string String(string prefix)
    {
        lock (Gate) return prefix + Fixture.Create<string>();
    }

    public static string[] Strings(string prefix, int count)
    {
        lock (Gate) return Enumerable.Range(0, count).Select(_ => prefix + Fixture.Create<string>()).ToArray();
    }
}
