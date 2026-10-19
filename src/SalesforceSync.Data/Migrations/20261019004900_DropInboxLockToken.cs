using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace SalesforceSync.Data.Migrations;

/// <inheritdoc />
public partial class DropInboxLockToken : Migration
{
    private static readonly string[] Tables = ["sf_account_inbox", "sf_account_relationship_inbox", "sf_contact_inbox"];

    /// <inheritdoc />
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        // The lease is next_attempt_at; an outcome is recorded on a pending row that still holds the event the claim read
        foreach (var table in Tables)
        {
            migrationBuilder.DropColumn(
                name: "lock_token",
                table: table);
        }
    }

    /// <inheritdoc />
    protected override void Down(MigrationBuilder migrationBuilder)
    {
        foreach (var table in Tables)
        {
            migrationBuilder.AddColumn<Guid>(
                name: "lock_token",
                table: table,
                type: "uuid",
                nullable: true);
        }
    }
}
