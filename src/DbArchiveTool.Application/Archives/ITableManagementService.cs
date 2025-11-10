namespace DbArchiveTool.Application.Archives;

/// <summary>
/// 表管理服务接口,用于检查和创建归档目标表
/// </summary>
public interface ITableManagementService
{
    /// <summary>
    /// 检查目标表是否存在
    /// </summary>
    /// <param name="connectionString">数据库连接字符串</param>
    /// <param name="schemaName">架构名称</param>
    /// <param name="tableName">表名</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>如果表存在返回 true,否则返回 false</returns>
    Task<bool> CheckTableExistsAsync(
        string connectionString,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 创建目标表(根据源表结构)
    /// </summary>
    /// <param name="sourceConnectionString">源数据库连接字符串</param>
    /// <param name="targetConnectionString">目标数据库连接字符串</param>
    /// <param name="sourceSchemaName">源架构名称</param>
    /// <param name="sourceTableName">源表名</param>
    /// <param name="targetSchemaName">目标架构名称</param>
    /// <param name="targetTableName">目标表名</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>创建操作结果</returns>
    Task<TableCreationResult> CreateTargetTableAsync(
        string sourceConnectionString,
        string targetConnectionString,
        string sourceSchemaName,
        string sourceTableName,
        string targetSchemaName,
        string targetTableName,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// 表创建结果
/// </summary>
public sealed class TableCreationResult
{
    /// <summary>
    /// 是否成功
    /// </summary>
    public bool Success { get; init; }

    /// <summary>
    /// 错误消息
    /// </summary>
    public string? ErrorMessage { get; init; }

    /// <summary>
    /// 生成的 CREATE TABLE 脚本
    /// </summary>
    public string? Script { get; init; }

    /// <summary>
    /// 创建的表的列数
    /// </summary>
    public int ColumnCount { get; init; }

    public static TableCreationResult Succeeded(string script, int columnCount)
    {
        return new TableCreationResult
        {
            Success = true,
            Script = script,
            ColumnCount = columnCount
        };
    }

    public static TableCreationResult Failed(string errorMessage)
    {
        return new TableCreationResult
        {
            Success = false,
            ErrorMessage = errorMessage
        };
    }
}
