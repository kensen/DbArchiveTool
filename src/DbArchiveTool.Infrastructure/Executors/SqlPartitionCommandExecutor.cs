using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Infrastructure.SqlExecution;
using Microsoft.Extensions.Logging;
using System.Text;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// 负责渲染 SQL 模板并调用数据库执行 DDL 的执行器。
/// </summary>
internal sealed class SqlPartitionCommandExecutor
{
    private readonly ISqlTemplateProvider templateProvider;
    private readonly ILogger<SqlPartitionCommandExecutor> logger;

    public SqlPartitionCommandExecutor(ISqlTemplateProvider templateProvider, ILogger<SqlPartitionCommandExecutor> logger)
    {
        this.templateProvider = templateProvider;
        this.logger = logger;
    }

    public string RenderSplitScript(PartitionConfiguration configuration, IReadOnlyList<PartitionValue> newBoundaries)
    {
        var template = templateProvider.GetTemplate("SplitRange.sql");
        var builder = new StringBuilder();
        var filegroup = configuration.FilegroupStrategy.PrimaryFilegroup;

        foreach (var boundary in newBoundaries)
        {
            builder.AppendLine(template
                .Replace("{PartitionScheme}", configuration.PartitionSchemeName)
                .Replace("{FilegroupName}", filegroup)
                .Replace("{PartitionFunction}", configuration.PartitionFunctionName)
                .Replace("{BoundaryLiteral}", boundary.ToLiteral()));
        }

        logger.LogDebug("Split script generated for {Schema}.{Table} with {Count} boundaries", configuration.SchemaName, configuration.TableName, newBoundaries.Count);
        return builder.ToString();
    }

    public string RenderMergeScript(PartitionConfiguration configuration, string boundaryKey)
    {
        var boundary = configuration.Boundaries.FirstOrDefault(x => x.SortKey.Equals(boundaryKey, StringComparison.Ordinal));
        if (boundary is null)
        {
            throw new InvalidOperationException("未找到指定的分区边界，无法生成 MERGE 脚本。");
        }

        var template = templateProvider.GetTemplate("MergeRange.sql");
        var script = template
            .Replace("{PartitionFunction}", configuration.PartitionFunctionName)
            .Replace("{BoundaryLiteral}", boundary.Value.ToLiteral());

        logger.LogDebug("Merge script generated for {Schema}.{Table} at boundary {Boundary}", configuration.SchemaName, configuration.TableName, boundaryKey);
        return script;
    }

    public string RenderSwitchOutScript(PartitionConfiguration configuration, SwitchPayload payload)
    {
        var template = templateProvider.GetTemplate("SwitchOut.sql");
        var script = template
            .Replace("{Schema}", configuration.SchemaName)
            .Replace("{SourceTable}", configuration.TableName)
            .Replace("{TargetTable}", payload.TargetTable)
            .Replace("{SourcePartitionNumber}", payload.SourcePartitionKey)
            .Replace("{TargetPartitionNumber}", payload.SourcePartitionKey);

        logger.LogDebug("Switch script generated for {Schema}.{Table} to {Target}", configuration.SchemaName, configuration.TableName, payload.TargetTable);
        return script;
    }
}
