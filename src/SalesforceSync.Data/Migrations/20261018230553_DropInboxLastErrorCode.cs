using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace SalesforceSync.Data.Migrations;

/// <inheritdoc />
public partial class DropInboxLastErrorCode : Migration
{
    private static readonly string[] Tables = ["sf_account_inbox", "sf_account_relationship_inbox", "sf_contact_inbox"];

    /// <inheritdoc />
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        // last_error names the code ("… DEPENDENCY_NOT_SYNCED …"), so a dead letter is still found by its cause
        foreach (var table in Tables)
        {
            migrationBuilder.DropColumn(
                name: "last_error_code",
                table: table);
        }
    }

    /// <inheritdoc />
    protected override void Down(MigrationBuilder migrationBuilder)
    {
        foreach (var table in Tables)
        {
            migrationBuilder.AddColumn<string>(
                name: "last_error_code",
                table: table,
                type: "text",
                nullable: true);
        }
    }
}
