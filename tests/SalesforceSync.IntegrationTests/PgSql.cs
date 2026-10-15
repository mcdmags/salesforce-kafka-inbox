using Npgsql;

namespace SalesforceSync.IntegrationTests;

/// <summary>
/// Plain SQL for the tests' own setup and checks: a statement and an object whose properties are its <c>@named</c>
/// parameters. Rows come back as a scalar (first column), a value tuple or a record (by column position).
/// </summary>
public static class PgSql
{
    public static async Task<int> ExecuteAsync(this NpgsqlConnection conn, string sql, object? args = null, NpgsqlTransaction? transaction = null,
        CancellationToken ct = default)
    {
        await using var command = Command(conn, sql, args, transaction);
        return await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task<List<T>> QueryAsync<T>(this NpgsqlConnection conn, string sql, object? args = null, NpgsqlTransaction? transaction = null,
        CancellationToken ct = default)
    {
        await using var command = Command(conn, sql, args, transaction);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = new List<T>();
        while (await reader.ReadAsync(ct))
        {
            rows.Add(Read<T>(reader));
        }
        return rows;
    }

    public static async Task<T> QuerySingleAsync<T>(this NpgsqlConnection conn, string sql, object? args = null, NpgsqlTransaction? transaction = null)
    {
        var rows = await conn.QueryAsync<T>(sql, args, transaction);
        return rows.Single();
    }

    public static async Task<T?> ExecuteScalarAsync<T>(this NpgsqlConnection conn, string sql, object? args = null, NpgsqlTransaction? transaction = null)
    {
        var rows = await conn.QueryAsync<T>(sql, args, transaction);
        return rows.FirstOrDefault();
    }

    /// <summary>
    /// Events into an inbox table as the consumer stores them, their entities made due (InboxSql.Store) with no
    /// coalesce delay: an idle entity is claimable at once.
    /// </summary>
    public static Task<int> StoreAsync(this NpgsqlConnection conn, string table, string entityType, string[] entityIds, string[] payloads,
        NpgsqlTransaction? transaction = null)
    {
        return conn.ExecuteAsync(Inbox.InboxSql.For(table).Store,
            new { entityType, entityIds, payloads, coalesceDelay = TimeSpan.Zero }, transaction);
    }

    /// <summary>One event, as the consumer stores it.</summary>
    public static Task<int> StoreAsync(this NpgsqlConnection conn, string table, string entityType, string entityId, string payload,
        NpgsqlTransaction? transaction = null)
    {
        return conn.StoreAsync(table, entityType, [entityId], [payload], transaction);
    }

    private static NpgsqlCommand Command(NpgsqlConnection conn, string sql, object? args, NpgsqlTransaction? transaction)
    {
        var command = new NpgsqlCommand(sql, conn, transaction);
        if (args is not null)
        {
            foreach (var property in args.GetType().GetProperties())
            {
                command.Parameters.Add(new NpgsqlParameter(property.Name, property.GetValue(args) ?? DBNull.Value));
            }
        }
        return command;
    }

    private static T Read<T>(NpgsqlDataReader reader)
    {
        var type = typeof(T);
        var scalar = Nullable.GetUnderlyingType(type) ?? type;
        if (scalar.IsPrimitive || scalar == typeof(string) || scalar == typeof(Guid) || scalar == typeof(DateTime) || scalar == typeof(decimal))
        {
            return (T)Convert(reader, 0, type)!;
        }

        var constructor = type.GetConstructors().OrderByDescending(c => c.GetParameters().Length).First();
        var parameters = constructor.GetParameters();
        var values = new object?[parameters.Length];
        for (var i = 0; i < parameters.Length; i++)
        {
            values[i] = Convert(reader, i, parameters[i].ParameterType);
        }
        return (T)constructor.Invoke(values);
    }

    private static object? Convert(NpgsqlDataReader reader, int ordinal, Type target)
    {
        if (reader.IsDBNull(ordinal))
        {
            return null;
        }
        var value = reader.GetValue(ordinal);
        var type = Nullable.GetUnderlyingType(target) ?? target;
        return type.IsInstanceOfType(value) ? value : System.Convert.ChangeType(value, type);
    }
}