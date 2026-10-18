using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace SalesforceSync.Data.Migrations;

/// <inheritdoc />
public partial class DropInboxStatus : Migration
{
    private static readonly string[] Tables = ["sf_account_inbox", "sf_account_relationship_inbox", "sf_contact_inbox"];

    /// <inheritdoc />
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        // A row's state is in its other columns: waiting (next_attempt_at), done (idle, no last_error) or dead (idle, with
        // the last_error that ended it)
        foreach (var table in Tables)
        {
            migrationBuilder.DropColumn(
                name: "status",
                table: table);
        }
    }

    /// <inheritdoc />
    protected override void Down(MigrationBuilder migrationBuilder)
    {
        foreach (var table in Tables)
        {
            migrationBuilder.AddColumn<string>(
                name: "status",
                table: table,
                type: "text",
                nullable: false,
                defaultValueSql: "'pending'");

            migrationBuilder.Sql($"""
                update {table}
                set status = case when next_attempt_at is not null then 'pending' when last_error is null then 'done' else 'dead' end;
                """);
        }
    }
}
