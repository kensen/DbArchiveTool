using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;

namespace DbArchiveTool.PasswordMigration;

/// <summary>
/// 数据迁移工具：将 ArchiveDataSource 表中的明文密码加密
/// </summary>
class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("========================================");
        Console.WriteLine("密码加密迁移工具");
        Console.WriteLine("========================================");
        Console.WriteLine();

        // 设置连接字符串
        var connectionString = args.Length > 0
            ? args[0]
            : "Server=localhost;Database=DbArchiveTool;Trusted_Connection=True;TrustServerCertificate=True";

        Console.WriteLine($"连接字符串: {connectionString}");
        Console.WriteLine();

        // 初始化加密服务
        var services = new ServiceCollection();
        services.AddDataProtection();
        var serviceProvider = services.BuildServiceProvider();
        
        var dataProtectionProvider = serviceProvider.GetRequiredService<IDataProtectionProvider>();
        var protector = dataProtectionProvider.CreateProtector("DbArchiveTool.PasswordProtection");

        // 连接数据库并迁移密码
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        // 查询所有数据源
        var selectCommand = new SqlCommand(
            @"SELECT Id, Name, Password, TargetPassword 
              FROM ArchiveDataSource 
              WHERE IsDeleted = 0",
            connection);

        var dataSources = new System.Collections.Generic.List<(Guid Id, string Name, string? Password, string? TargetPassword)>();

        await using (var reader = await selectCommand.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                var id = reader.GetGuid(0);
                var name = reader.GetString(1);
                var password = reader.IsDBNull(2) ? null : reader.GetString(2);
                var targetPassword = reader.IsDBNull(3) ? null : reader.GetString(3);
                
                dataSources.Add((id, name, password, targetPassword));
            }
        }

        Console.WriteLine($"找到 {dataSources.Count} 个数据源需要处理");
        Console.WriteLine();

        int encryptedCount = 0;
        int skippedCount = 0;

        foreach (var (id, name, password, targetPassword) in dataSources)
        {
            Console.WriteLine($"处理数据源: {name} ({id})");

            string? newPassword = password;
            string? newTargetPassword = targetPassword;
            bool needsUpdate = false;

            // 检查并加密 Password
            if (!string.IsNullOrWhiteSpace(password) && !password.StartsWith("ENCRYPTED:"))
            {
                newPassword = "ENCRYPTED:" + protector.Protect(password);
                needsUpdate = true;
                Console.WriteLine($"  - 加密 Password 字段");
            }
            else if (!string.IsNullOrWhiteSpace(password))
            {
                Console.WriteLine($"  - Password 已加密，跳过");
            }

            // 检查并加密 TargetPassword
            if (!string.IsNullOrWhiteSpace(targetPassword) && !targetPassword.StartsWith("ENCRYPTED:"))
            {
                newTargetPassword = "ENCRYPTED:" + protector.Protect(targetPassword);
                needsUpdate = true;
                Console.WriteLine($"  - 加密 TargetPassword 字段");
            }
            else if (!string.IsNullOrWhiteSpace(targetPassword))
            {
                Console.WriteLine($"  - TargetPassword 已加密，跳过");
            }

            // 更新数据库
            if (needsUpdate)
            {
                var updateCommand = new SqlCommand(
                    @"UPDATE ArchiveDataSource 
                      SET Password = @Password, 
                          TargetPassword = @TargetPassword,
                          UpdatedAtUtc = @UpdatedAtUtc
                      WHERE Id = @Id",
                    connection);

                updateCommand.Parameters.AddWithValue("@Password", (object?)newPassword ?? DBNull.Value);
                updateCommand.Parameters.AddWithValue("@TargetPassword", (object?)newTargetPassword ?? DBNull.Value);
                updateCommand.Parameters.AddWithValue("@UpdatedAtUtc", DateTime.UtcNow);
                updateCommand.Parameters.AddWithValue("@Id", id);

                await updateCommand.ExecuteNonQueryAsync();
                encryptedCount++;
                Console.WriteLine($"  ✓ 已更新");
            }
            else
            {
                skippedCount++;
                Console.WriteLine($"  - 无需更新");
            }

            Console.WriteLine();
        }

        Console.WriteLine("========================================");
        Console.WriteLine($"迁移完成！");
        Console.WriteLine($"  已加密: {encryptedCount} 个数据源");
        Console.WriteLine($"  已跳过: {skippedCount} 个数据源");
        Console.WriteLine($"  总计: {dataSources.Count} 个数据源");
        Console.WriteLine("========================================");
    }
}
