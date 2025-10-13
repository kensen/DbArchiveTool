using System.Text;

namespace DbArchiveTool.Infrastructure.Models;

/// <summary>
/// 表索引定义（用于分区转换时保存和重建索引）
/// </summary>
public class TableIndexDefinition
{
    /// <summary>
    /// 索引 ID（系统内部编号，聚集索引=1）
    /// </summary>
    public int IndexId { get; set; }

    /// <summary>
    /// 索引名称
    /// </summary>
    public string IndexName { get; set; } = string.Empty;

    /// <summary>
    /// 索引类型（CLUSTERED 或 NONCLUSTERED）
    /// </summary>
    public string IndexType { get; set; } = string.Empty;

    /// <summary>
    /// 是否唯一索引
    /// </summary>
    public bool IsUnique { get; set; }

    /// <summary>
    /// 是否主键
    /// </summary>
    public bool IsPrimaryKey { get; set; }

    /// <summary>
    /// 是否唯一约束
    /// </summary>
    public bool IsUniqueConstraint { get; set; }

    /// <summary>
    /// 约束名称（主键或唯一约束）
    /// </summary>
    public string? ConstraintName { get; set; }

    /// <summary>
    /// 约束类型
    /// </summary>
    public string? ConstraintType { get; set; }

    /// <summary>
    /// 索引键列（包含排序方向，例如：[Column1] ASC, [Column2] DESC）
    /// </summary>
    public string KeyColumns { get; set; } = string.Empty;

    /// <summary>
    /// INCLUDE 列（逗号分隔）
    /// </summary>
    public string? IncludedColumns { get; set; }

    /// <summary>
    /// 筛选条件（WHERE 子句）
    /// </summary>
    public string? FilterDefinition { get; set; }

    /// <summary>
    /// 索引是否包含分区列（用于判断是否对齐到分区方案）
    /// </summary>
    public bool ContainsPartitionColumn { get; set; }

    /// <summary>
    /// 是否为聚集索引
    /// </summary>
    public bool IsClustered => IndexType == "CLUSTERED";

    /// <summary>
    /// 生成删除索引的 SQL
    /// </summary>
    public string GetDropSql(string schemaName, string tableName)
    {
        // 主键或唯一约束需要通过 ALTER TABLE DROP CONSTRAINT
        if ((IsPrimaryKey || IsUniqueConstraint) && !string.IsNullOrWhiteSpace(ConstraintName))
        {
            return $"ALTER TABLE [{schemaName}].[{tableName}] DROP CONSTRAINT [{ConstraintName}];";
        }
        else
        {
            // 普通索引使用 DROP INDEX
            return $"DROP INDEX [{IndexName}] ON [{schemaName}].[{tableName}];";
        }
    }

    /// <summary>
    /// 生成创建索引的 SQL
    /// </summary>
    /// <param name="schemaName">架构名</param>
    /// <param name="tableName">表名</param>
    /// <param name="partitionScheme">分区方案名称</param>
    /// <param name="partitionColumn">分区列名</param>
    /// <returns>创建索引的 SQL 语句</returns>
    public string GetCreateSql(string schemaName, string tableName, string partitionScheme, string partitionColumn)
    {
        var sb = new StringBuilder();

        // 主键约束
        if (IsPrimaryKey && !string.IsNullOrWhiteSpace(ConstraintName))
        {
            sb.Append($"ALTER TABLE [{schemaName}].[{tableName}] ");
            sb.Append($"ADD CONSTRAINT [{ConstraintName}] PRIMARY KEY ");
            sb.Append(IsClustered ? "CLUSTERED " : "NONCLUSTERED ");
            sb.Append($"({KeyColumns})");

            // 聚集索引必须在分区方案上
            if (IsClustered)
            {
                sb.Append($" ON [{partitionScheme}]([{partitionColumn}])");
            }
            // 非聚集主键：如果包含分区列，可以对齐；否则放在 PRIMARY 文件组
            else if (ContainsPartitionColumn)
            {
                sb.Append($" ON [{partitionScheme}]([{partitionColumn}])");
            }
            else
            {
                sb.Append(" ON [PRIMARY]");
            }
        }
        // 唯一约束
        else if (IsUniqueConstraint && !string.IsNullOrWhiteSpace(ConstraintName))
        {
            sb.Append($"ALTER TABLE [{schemaName}].[{tableName}] ");
            sb.Append($"ADD CONSTRAINT [{ConstraintName}] UNIQUE ");
            sb.Append(IsClustered ? "CLUSTERED " : "NONCLUSTERED ");
            sb.Append($"({KeyColumns})");

            // 聚集索引必须在分区方案上
            if (IsClustered)
            {
                sb.Append($" ON [{partitionScheme}]([{partitionColumn}])");
            }
            // 非聚集唯一约束：如果包含分区列，可以对齐；否则放在 PRIMARY 文件组
            else if (ContainsPartitionColumn)
            {
                sb.Append($" ON [{partitionScheme}]([{partitionColumn}])");
            }
            else
            {
                sb.Append(" ON [PRIMARY]");
            }
        }
        // 普通索引
        else
        {
            sb.Append("CREATE ");
            if (IsUnique) sb.Append("UNIQUE ");
            sb.Append(IsClustered ? "CLUSTERED " : "NONCLUSTERED ");
            sb.Append($"INDEX [{IndexName}] ");
            sb.Append($"ON [{schemaName}].[{tableName}] ({KeyColumns})");

            // INCLUDE 列
            if (!string.IsNullOrWhiteSpace(IncludedColumns))
            {
                sb.Append($" INCLUDE ({IncludedColumns})");
            }

            // WHERE 筛选条件
            if (!string.IsNullOrWhiteSpace(FilterDefinition))
            {
                sb.Append($" WHERE {FilterDefinition}");
            }

            // 存储位置
            if (IsClustered)
            {
                // 聚集索引必须在分区方案上
                sb.Append($" ON [{partitionScheme}]([{partitionColumn}])");
            }
            else if (ContainsPartitionColumn)
            {
                // 非聚集索引如果包含分区列，对齐到分区方案
                sb.Append($" ON [{partitionScheme}]([{partitionColumn}])");
            }
            else
            {
                // 非聚集索引如果不包含分区列，放在 PRIMARY 文件组（非对齐）
                sb.Append(" ON [PRIMARY]");
            }
        }

        sb.Append(';');
        return sb.ToString();
    }

    /// <summary>
    /// 获取索引描述（用于日志）
    /// </summary>
    public string GetDescription()
    {
        var parts = new List<string>();

        if (IsPrimaryKey) parts.Add("主键");
        else if (IsUniqueConstraint) parts.Add("唯一约束");
        else if (IsUnique) parts.Add("唯一索引");
        else parts.Add("普通索引");

        parts.Add(IsClustered ? "聚集" : "非聚集");

        if (ContainsPartitionColumn)
            parts.Add("对齐分区");
        else if (!IsClustered)
            parts.Add("非对齐");

        if (!string.IsNullOrWhiteSpace(IncludedColumns))
            parts.Add("含INCLUDE列");

        if (!string.IsNullOrWhiteSpace(FilterDefinition))
            parts.Add("筛选索引");

        return string.Join(", ", parts);
    }
}
