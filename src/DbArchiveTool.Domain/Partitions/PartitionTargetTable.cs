using System.Text.RegularExpressions;

namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 表示分区配置中用于 SWITCH/备份的目标表信息。
/// </summary>
public sealed class PartitionTargetTable
{
    private const string IdentifierPattern = "^[A-Za-z_][A-Za-z0-9_]*$";

    private PartitionTargetTable(string databaseName, string schemaName, string tableName, string? remarks)
    {
        DatabaseName = databaseName;
        SchemaName = schemaName;
        TableName = tableName;
        Remarks = string.IsNullOrWhiteSpace(remarks) ? null : remarks.Trim();
    }

    /// <summary>目标数据库名称。</summary>
    public string DatabaseName { get; }

    /// <summary>目标架构名称。</summary>
    public string SchemaName { get; }

    /// <summary>目标表名称。</summary>
    public string TableName { get; }

    /// <summary>备注信息。</summary>
    public string? Remarks { get; }

    /// <summary>
    /// 创建目标表配置。
    /// </summary>
    public static PartitionTargetTable Create(string databaseName, string schemaName, string tableName, string? remarks)
    {
        if (string.IsNullOrWhiteSpace(databaseName))
        {
            throw new ArgumentException("目标数据库名称不能为空。", nameof(databaseName));
        }

        if (string.IsNullOrWhiteSpace(schemaName))
        {
            throw new ArgumentException("目标架构名称不能为空。", nameof(schemaName));
        }

        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentException("目标表名称不能为空。", nameof(tableName));
        }

        var normalizedDb = databaseName.Trim();
        var normalizedSchema = schemaName.Trim();
        var normalizedTable = tableName.Trim();

        if (!Regex.IsMatch(normalizedSchema, IdentifierPattern))
        {
            throw new ArgumentException("架构名称仅支持字母、数字与下划线，并且需以字母或下划线开头。", nameof(schemaName));
        }

        if (!Regex.IsMatch(normalizedTable, IdentifierPattern))
        {
            throw new ArgumentException("目标表名称仅支持字母、数字与下划线，并且需以字母或下划线开头。", nameof(tableName));
        }

        return new PartitionTargetTable(normalizedDb, normalizedSchema, normalizedTable, remarks);
    }
}
