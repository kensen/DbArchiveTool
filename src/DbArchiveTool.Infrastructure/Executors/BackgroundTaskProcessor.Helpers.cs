using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Domain.DataSources;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Shared.Partitions;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// BackgroundTaskProcessor 的部分类 - 辅助工具方法
/// </summary>
internal sealed partial class BackgroundTaskProcessor
{
    /// <summary>
    /// 处理校验失败的情况
    /// </summary>
    private async Task HandleValidationFailureAsync(BackgroundTask task, string reason, CancellationToken cancellationToken)
    {
        await AppendLogAsync(task.Id, "Warning", "校验失败", reason, cancellationToken);
        task.Cancel("SYSTEM", reason);
        await taskRepository.UpdateAsync(task, cancellationToken);
    }

    /// <summary>
    /// 添加任务日志
    /// </summary>
    private Task AppendLogAsync(
        Guid taskId,
        string category,
        string title,
        string message,
        CancellationToken cancellationToken,
        long? durationMs = null,
        string? extraJson = null)
    {
        var entry = BackgroundTaskLogEntry.Create(taskId, category, title, message, durationMs, extraJson);
        return logRepository.AddAsync(entry, cancellationToken);
    }

    /// <summary>
    /// 构建权限上下文描述
    /// </summary>
    private static string BuildPermissionContext(ArchiveDataSource dataSource, PartitionConfiguration configuration)
    {
        return $"目标服务器：{BuildServerDisplay(dataSource)}，目标数据库：{dataSource.DatabaseName}，目标对象：{configuration.SchemaName}.{configuration.TableName}";
    }

    /// <summary>
    /// 构建服务器显示字符串
    /// </summary>
    private static string BuildServerDisplay(ArchiveDataSource dataSource)
    {
        return dataSource.ServerPort == 1433
            ? dataSource.ServerAddress
            : $"{dataSource.ServerAddress}:{dataSource.ServerPort}";
    }

    /// <summary>
    /// 从任务的 ConfigurationSnapshot 构建临时的分区配置对象（仅用于执行，不持久化）
    /// </summary>
    private async Task<PartitionConfiguration?> BuildConfigurationFromSnapshotAsync(
        BackgroundTask task,
        CancellationToken cancellationToken)
    {
        try
        {
            // 根据不同的操作类型解析快照
            switch (task.OperationType)
            {
                case BackgroundTaskOperationType.AddBoundary:
                    return await BuildConfigForAddBoundaryAsync(task, cancellationToken);
                
                case BackgroundTaskOperationType.SplitBoundary:
                    return await BuildConfigForSplitBoundaryAsync(task, cancellationToken);
                
                case BackgroundTaskOperationType.MergeBoundary:
                    return await BuildConfigForMergeBoundaryAsync(task, cancellationToken);
                
                case BackgroundTaskOperationType.ArchiveSwitch:
                    return await BuildConfigForArchiveSwitchAsync(task, cancellationToken);
                
                default:
                    logger.LogError("不支持的操作类型：{OperationType}", task.OperationType);
                    return null;
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "解析任务快照失败：{Snapshot}", task.ConfigurationSnapshot);
            return null;
        }
    }

    /// <summary>
    /// 获取现有的临时表列表（用于清理上次失败遗留的临时表）
    /// </summary>
    private static async Task<List<string>> GetExistingTempTablesAsync(
        string connectionString,
        string schemaName,
        string baseTableName,
        CancellationToken cancellationToken)
    {
        const string sql = @"
            SELECT name 
            FROM sys.tables 
            WHERE schema_id = SCHEMA_ID(@SchemaName)
              AND name LIKE @Pattern
            ORDER BY create_date DESC";

        var pattern = $"{baseTableName}_Temp_%";
        var tempTables = new List<string>();

        using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(cancellationToken);

        using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@SchemaName", schemaName);
        cmd.Parameters.AddWithValue("@Pattern", pattern);

        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            tempTables.Add(reader.GetString(0));
        }

        return tempTables;
    }

    /// <summary>
    /// 获取指定表的行数
    /// </summary>
    private static async Task<long> GetTableRowCountAsync(
        string connectionString,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken)
    {
        const string sql = "SELECT COUNT_BIG(*) FROM [{0}].[{1}]";
        var query = string.Format(sql, schemaName, tableName);

        using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(cancellationToken);

        using var cmd = new SqlCommand(query, conn);
        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        
        return result is long count ? count : 0;
    }

    /// <summary>
    /// 从命令输出中提取最后 N 行(摘要信息)
    /// </summary>
    private static string GetCommandOutputSummary(string? output, int maxLines = 5)
    {
        if (string.IsNullOrWhiteSpace(output))
        {
            return "(无输出)";
        }

        var lines = output.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
        if (lines.Length <= maxLines)
        {
            return output;
        }

        // 返回最后 maxLines 行
        var summaryLines = lines.TakeLast(maxLines);
        return string.Join(Environment.NewLine, summaryLines);
    }

    /// <summary>
    /// 构建数据库连接字符串
    /// </summary>
    private string BuildConnectionString(ArchiveDataSource dataSource)
    {
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = dataSource.ServerPort == 1433
                ? dataSource.ServerAddress
                : $"{dataSource.ServerAddress},{dataSource.ServerPort}",
            InitialCatalog = dataSource.DatabaseName,
            IntegratedSecurity = dataSource.UseIntegratedSecurity,
            TrustServerCertificate = true,
            ConnectTimeout = 30
        };

        if (!dataSource.UseIntegratedSecurity)
        {
            builder.UserID = dataSource.UserName;
            // 解密密码
            if (!string.IsNullOrEmpty(dataSource.Password))
            {
                builder.Password = passwordEncryptionService.Decrypt(dataSource.Password);
            }
        }

        return builder.ConnectionString;
    }

    /// <summary>
    /// 构建目标服务器连接字符串(支持自定义目标服务器)
    /// </summary>
    /// <param name="dataSource">数据源配置</param>
    /// <param name="targetDatabase">目标数据库名(可选,用于覆盖默认目标数据库)</param>
    /// <returns>目标服务器连接字符串</returns>
    private string BuildTargetConnectionString(ArchiveDataSource dataSource, string? targetDatabase = null)
    {
        // 如果使用源服务器作为目标服务器
        if (dataSource.UseSourceAsTarget)
        {
            // 如果指定了目标数据库,则使用源连接字符串但切换数据库
            if (!string.IsNullOrWhiteSpace(targetDatabase))
            {
                var builder = new SqlConnectionStringBuilder(BuildConnectionString(dataSource))
                {
                    InitialCatalog = targetDatabase
                };
                return builder.ConnectionString;
            }
            // 否则直接使用源连接字符串
            return BuildConnectionString(dataSource);
        }

        // 使用自定义目标服务器配置
        var targetBuilder = new SqlConnectionStringBuilder
        {
            DataSource = dataSource.TargetServerPort == 1433
                ? dataSource.TargetServerAddress
                : $"{dataSource.TargetServerAddress},{dataSource.TargetServerPort}",
            // 优先使用传入的 targetDatabase,否则使用配置的 TargetDatabaseName,最后回退到源数据库名
            InitialCatalog = targetDatabase ?? dataSource.TargetDatabaseName ?? dataSource.DatabaseName,
            IntegratedSecurity = dataSource.TargetUseIntegratedSecurity,
            TrustServerCertificate = true,
            ConnectTimeout = 30
        };

        if (!dataSource.TargetUseIntegratedSecurity)
        {
            targetBuilder.UserID = dataSource.TargetUserName;
            // 解密密码
            if (!string.IsNullOrEmpty(dataSource.TargetPassword))
            {
                targetBuilder.Password = passwordEncryptionService.Decrypt(dataSource.TargetPassword);
            }
        }

        return targetBuilder.ConnectionString;
    }
}
