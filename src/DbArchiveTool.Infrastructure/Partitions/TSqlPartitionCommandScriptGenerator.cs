using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Shared.Results;
using System.Text;

namespace DbArchiveTool.Infrastructure.Partitions;

/// <summary>
/// 默认的 T-SQL 脚本生成器，实现分区拆分脚本拼装，并给出关键提醒。
/// </summary>
internal sealed class TSqlPartitionCommandScriptGenerator : IPartitionCommandScriptGenerator
{
    public Result<string> GenerateSplitScript(PartitionConfiguration configuration, IReadOnlyList<PartitionValue> newBoundaries)
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
        foreach (var boundary in newBoundaries)
        {
            var literal = boundary.ToLiteral();
            builder.AppendLine("BEGIN TRY");
            builder.AppendLine("    BEGIN TRANSACTION");
            builder.AppendLine($"    ALTER PARTITION FUNCTION {configuration.PartitionFunctionName}() SPLIT RANGE ({literal});");
            builder.AppendLine("    COMMIT TRANSACTION");
            builder.AppendLine("END TRY");
            builder.AppendLine("BEGIN CATCH");
            builder.AppendLine("    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;");
            builder.AppendLine("    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();");
            builder.AppendLine("    RAISERROR ('分区拆分失败: %s', 16, 1, @ErrorMessage);");
            builder.AppendLine("END CATCH");
            builder.AppendLine();
        }

        builder.AppendLine("-- 建议执行完毕后重新检查分区边界与统计信息");
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
}
