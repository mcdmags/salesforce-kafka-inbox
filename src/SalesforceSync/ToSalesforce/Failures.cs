using System.Collections;
using System.Diagnostics.CodeAnalysis;

namespace SalesforceSync.ToSalesforce;

/// <summary>One reason an entity failed. Permanent means retrying the same data can't help.</summary>
public sealed record Failure(string Error, bool Permanent);

/// <summary>The entities of a batch that failed, and why. An entity that isn't listed succeeded.</summary>
public sealed class Failures : IEnumerable<KeyValuePair<string, IReadOnlyList<Failure>>>
{
    private readonly Dictionary<string, List<Failure>> _byEntity = new(StringComparer.Ordinal);

    public IReadOnlyList<Failure> this[string entityId] => _byEntity[entityId];

    /// <summary>Records a failure of the entity as <c>code: message</c>.</summary>
    public void Add(string entityId, string code, string message, bool permanent = false)
    {
        if (!_byEntity.TryGetValue(entityId, out var list))
        {
            _byEntity[entityId] = list = [];
        }

        list.Add(new Failure($"{code}: {message}", permanent));
    }

    public bool Contains(string entityId)
    {
        return _byEntity.ContainsKey(entityId);
    }

    public bool TryGetValue(string entityId, [MaybeNullWhen(false)] out IReadOnlyList<Failure> failures)
    {
        var found = _byEntity.TryGetValue(entityId, out var list);
        failures = list;
        return found;
    }

    /// <summary>
    /// The failures whose error code starts with <paramref name="codePrefix"/>, with their entity. A prefix, so
    /// <c>INVALID_FIELD</c> also finds <c>INVALID_FIELD_FOR_INSERT_UPDATE</c>.
    /// </summary>
    public IEnumerable<(string EntityId, Failure Failure)> WithCodePrefix(string codePrefix)
    {
        return _byEntity.SelectMany(e => e.Value.Where(f => f.Error.StartsWith(codePrefix, StringComparison.Ordinal)).Select(f => (e.Key, f)));
    }

    public IEnumerator<KeyValuePair<string, IReadOnlyList<Failure>>> GetEnumerator()
    {
        return _byEntity.Select(e => KeyValuePair.Create(e.Key, (IReadOnlyList<Failure>)e.Value)).GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}
