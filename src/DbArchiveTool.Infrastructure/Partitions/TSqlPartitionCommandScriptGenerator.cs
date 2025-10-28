using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Shared.Results;

namespace DbArchiveTool.Infrastructure.Partitions;

/// <summary>
/// T-SQL 脚本生成器，实现拆分、合并及 SWITCH 等分区操作脚本的拼装。
/// </summary>
internal sealed class TSqlPartitionCommandScriptGenerator : IPartitionCommandScriptGenerator
{
    /// <inheritdoc />
    public Result<string> GenerateSplitScript(
        PartitionConfiguration configuration, 
        IReadOnlyList<PartitionValue> newBoundaries,
        string? filegroupName = null)
    {
        if (newBoundaries.Count == 0)
        {
            return Result<string>.Failure("至少提供一个新的分区边界。");
        }

        if (!IsMonotonic(newBoundaries))
        {
            return Result<string>.Failure("拆分边界需要保持严格递增或递减顺序。");
        }

        var builder = new StringBuilder();
        builder.AppendLine("-- 请确保已执行备份并确认无长事务占用目标表");

        // 优先使用用户指定的文件组,否则使用配置中的默认文件组
        var targetFilegroup = !string.IsNullOrWhiteSpace(filegroupName) 
            ? filegroupName 
            : ResolveDefaultFilegroup(configuration);

        for (int i = 0; i < newBoundaries.Count; i++)
        {
            var boundary = newBoundaries[i];
            var literal = boundary.ToLiteral();
            var varName = $"@ErrMsg_{i}";

            builder.AppendLine("BEGIN TRY");
            builder.AppendLine("    BEGIN TRANSACTION");
            builder.AppendLine($"    ALTER PARTITION SCHEME [{configuration.PartitionSchemeName}] NEXT USED [{targetFilegroup}];");
            builder.AppendLine($"    ALTER PARTITION FUNCTION [{configuration.PartitionFunctionName}]() SPLIT RANGE ({literal});");
            builder.AppendLine("    COMMIT TRANSACTION");
            builder.AppendLine("END TRY");
            builder.AppendLine("BEGIN CATCH");
            builder.AppendLine("    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;");
            builder.AppendLine($"    DECLARE {varName} NVARCHAR(4000) = ERROR_MESSAGE();");
            builder.AppendLine($"    RAISERROR ('分区拆分失败 (边界={literal}): %s', 16, 1, {varName});");
            builder.AppendLine("END CATCH");
            builder.AppendLine();
        }

        builder.AppendLine("-- 建议执行完毕后重新检查分区边界与统计信息");
        return Result<string>.Success(builder.ToString());
    }

    /// <inheritdoc />
    public Result<string> GenerateMergeScript(PartitionConfiguration configuration, string boundaryKey)
    {
        if (string.IsNullOrWhiteSpace(boundaryKey))
        {
            return Result<string>.Failure("分区边界标识不能为空。");
        }

        var boundary = configuration.Boundaries.FirstOrDefault(x => x.SortKey.Equals(boundaryKey, StringComparison.Ordinal));
        if (boundary is null)
        {
            return Result<string>.Failure("未找到指定的分区边界。");
        }

        var literal = boundary.Value.ToLiteral();
        var builder = new StringBuilder();
        builder.AppendLine("-- 请确认目标分区无需要保留的数据");
        builder.AppendLine("BEGIN TRY");
        builder.AppendLine("    BEGIN TRANSACTION");
        builder.AppendLine($"    ALTER PARTITION FUNCTION [{configuration.PartitionFunctionName}]() MERGE RANGE ({literal});");
        builder.AppendLine("    COMMIT TRANSACTION");
        builder.AppendLine("END TRY");
        builder.AppendLine("BEGIN CATCH");
        builder.AppendLine("    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;");
        builder.AppendLine("    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();");
        builder.AppendLine("    RAISERROR ('分区合并失败: %s', 16, 1, @ErrorMessage);");
        builder.AppendLine("END CATCH");
        builder.AppendLine("-- 合并完成后建议重建统计信息");

        return Result<string>.Success(builder.ToString());
    }

    /// <inheritdoc />
    public Result<string> GenerateSwitchOutScript(PartitionConfiguration configuration, SwitchPayload payload)
    {
        if (payload is null)
        {
            return Result<string>.Failure("SWITCH 参数不能为空。");
        }

        if (string.IsNullOrWhiteSpace(payload.SourcePartitionKey))
        {
            return Result<string>.Failure("源分区标识不能为空。");
        }

        if (!int.TryParse(payload.SourcePartitionKey, NumberStyles.Integer, CultureInfo.InvariantCulture, out var partitionNumber))
        {
            return Result<string>.Failure("源分区标识格式不正确。");
        }

        if (string.IsNullOrWhiteSpace(payload.TargetSchema) || string.IsNullOrWhiteSpace(payload.TargetTable))
        {
            return Result<string>.Failure("目标表名称不能为空。");
        }

        var qualifiedTarget = string.IsNullOrWhiteSpace(payload.TargetDatabase)
            ? $"[{payload.TargetSchema}].[{payload.TargetTable}]"
            : $"[{payload.TargetDatabase}].[{payload.TargetSchema}].[{payload.TargetTable}]";
        var builder = new StringBuilder();
        builder.AppendLine("SET XACT_ABORT ON;");
        builder.AppendLine("SET NOCOUNT ON;");
        builder.AppendLine("BEGIN TRY");
        builder.AppendLine("    BEGIN TRANSACTION;");

        if (payload.CreateStagingTable && !string.IsNullOrWhiteSpace(payload.StagingTableName))
        {
            builder.AppendLine($"    -- 根据需要创建临时表 {payload.StagingTableName}");
            builder.AppendLine("    -- TODO: 在模板目录中补全临时表结构定义");
        }

        builder.AppendLine($"    ALTER TABLE [{configuration.SchemaName}].[{configuration.TableName}] SWITCH PARTITION {partitionNumber}");
        builder.AppendLine($"    TO {qualifiedTarget};");
        builder.AppendLine("    COMMIT TRANSACTION;");
        builder.AppendLine("END TRY");
        builder.AppendLine("BEGIN CATCH");
        builder.AppendLine("    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;");
        builder.AppendLine("    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();");
        builder.AppendLine("    RAISERROR ('分区 SWITCH 操作失败: %s', 16, 1, @ErrorMessage);");
        builder.AppendLine("END CATCH");

        return Result<string>.Success(builder.ToString());
    }

    private static bool IsMonotonic(IReadOnlyList<PartitionValue> boundaries)
    {
        if (boundaries.Count < 2)
        {
            return true;
        }

        var ascending = true;
        var descending = true;

        for (var i = 1; i < boundaries.Count; i++)
        {
            var comparison = boundaries[i - 1].CompareTo(boundaries[i]);
            if (comparison >= 0)
            {
                ascending = false;
            }

            if (comparison <= 0)
            {
                descending = false;
            }
        }

        return ascending || descending;
    }

    private static string ResolveDefaultFilegroup(PartitionConfiguration configuration)
    {
        if (configuration.StorageSettings.Mode == PartitionStorageMode.DedicatedFilegroupSingleFile &&
            !string.IsNullOrWhiteSpace(configuration.StorageSettings.FilegroupName))
        {
            return configuration.StorageSettings.FilegroupName;
        }

        return configuration.FilegroupStrategy.PrimaryFilegroup;
    }
}
