using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Data.SqlClient;
using RepoDb;
using RepoDb.Interfaces;

class Program
{
    const string ConnectionString = @"Server=(localdb)\MSSQLLocalDB;Database=DeviceTestDb;Integrated Security=True";

    static async Task Main()
    {
        // Setup database
        await SetupDatabase();

        // Run the working example, where the parameter name does not match a column name in the database that exists in both tables
        Console.WriteLine("Working example:");
        await RunWorkingExample();
        Console.WriteLine();

        // Run the reproduction
        Console.WriteLine("Reproduction:");
        await RunRepro();
        Console.WriteLine();
    }

    static async Task SetupDatabase()
    {
        using var connection = new SqlConnection(@"Server=(localdb)\MSSQLLocalDB;Integrated Security=True");
        await connection.OpenAsync();

        // Drop and recreate database
        await connection.ExecuteNonQueryAsync(@"
            IF EXISTS (SELECT * FROM sys.databases WHERE name = 'DeviceTestDb')
            BEGIN
                ALTER DATABASE DeviceTestDb SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE DeviceTestDb;
            END
            CREATE DATABASE DeviceTestDb;");

        // Switch to new database
        connection.ChangeDatabase("DeviceTestDb");

        // Create schema and tables
        await connection.ExecuteNonQueryAsync(@"
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'devices')
            BEGIN
                EXEC('CREATE SCHEMA devices')
            END
            
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[devices].[Devices]') AND type = N'U')
            BEGIN
                CREATE TABLE [devices].[Devices] (
                    [Id] BIGINT PRIMARY KEY IDENTITY(1,1),
                    [DeviceId] NVARCHAR(50) NOT NULL UNIQUE
                )
            END
            
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[devices].[DeviceStatuses]') AND type = N'U')
            BEGIN
                CREATE TABLE [devices].[DeviceStatuses] (
                    [Id] BIGINT PRIMARY KEY IDENTITY(1,1),
                    [DeviceId] BIGINT NOT NULL,
                    [Status] INT NOT NULL,
                    FOREIGN KEY ([DeviceId]) REFERENCES [devices].[Devices]([Id])
                )
            END");

        // Insert test data
        await connection.ExecuteNonQueryAsync(@"
            INSERT INTO [devices].[Devices] ([DeviceId])
            VALUES ('20ab3b');
            
            INSERT INTO [devices].[DeviceStatuses] ([DeviceId], [Status])
            VALUES ((SELECT [Id] FROM [devices].[Devices] WHERE [DeviceId] = '20ab3b'), 0)");
    }

    /// <summary>
    /// This is the working example where the parameter name does not match a column name in the database that exists in both tables
    /// </summary>
    static async Task RunWorkingExample()
    {
        try
        {
            GlobalConfiguration.Setup()
                .UseSqlServer();

            string deviceId = "20ab3b";
            string selectQuery = @"
                SELECT ds.Status 
                FROM [devices].[Devices] d 
                INNER JOIN [devices].[DeviceStatuses] ds ON d.[Id] = ds.[DeviceId] 
                WHERE d.[DeviceId] = @myParameterName";

            using var connection = new SqlConnection(ConnectionString).EnsureOpen();
            var param = new { myParameterName = deviceId };

            var statuses = await connection.ExecuteQueryAsync<DeviceStatus>(selectQuery, param, trace: new CustomTrace());
            var deviceStatus = statuses.FirstOrDefault();

            if (deviceStatus != null)
            {
                Console.WriteLine($"Status: {deviceStatus.Status}");
            }
            else
            {
                Console.WriteLine("No device status found");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex);
        }
    }

    /// <summary>
    /// This reproduces the issue where the parameter name matches a column name in the database that exists in both tables, but has a different type.    
    /// </summary>
    static async Task RunRepro()
    {
        try
        {
            GlobalConfiguration.Setup()
                .UseSqlServer();

            string deviceId = "20ab3b";
            string selectQuery = @"
            SELECT ds.Status 
            FROM [devices].[Devices] d 
            INNER JOIN [devices].[DeviceStatuses] ds ON d.[Id] = ds.[DeviceId] 
            WHERE d.[DeviceId] = @deviceId";

            using var connection = new SqlConnection(ConnectionString).EnsureOpen();
            var param = new { deviceId = deviceId };

            var statuses = await connection.ExecuteQueryAsync<DeviceStatus>(selectQuery, param, trace: new CustomTrace());
            var deviceStatus = statuses.FirstOrDefault();

            if (deviceStatus != null)
            {
                Console.WriteLine($"Status: {deviceStatus.Status}");
            }
            else
            {
                Console.WriteLine("No device status found");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex);
        }
    }
}

[Table("DeviceStatuses", Schema = "devices")]
public class DeviceStatus
{
    public DeviceStatusEnum Status { get; set; }
}

public enum DeviceStatusEnum
{
    Ok,
    Low,
    Medium,
    High,
    Disconnected
}
public class CustomTrace : ITrace
{
    public void BeforeExecution(CancellableTraceLog log)
    {
        Console.WriteLine("SQL: " + log.Statement);
        foreach (var p in log.Parameters)
        {
            Console.WriteLine($"Parameter: {p.ParameterName} = {p.Value} ({p.DbType})");
        }
    }

    public void AfterExecution<TResult>(ResultTraceLog<TResult> log)
    {
    }

    async Task ITrace.BeforeExecutionAsync(CancellableTraceLog log, CancellationToken cancellationToken)
    {
        BeforeExecution(log);
        await Task.CompletedTask;
    }

    async Task ITrace.AfterExecutionAsync<TResult>(ResultTraceLog<TResult> log, CancellationToken cancellationToken)
    {
        await Task.CompletedTask;
    }
}