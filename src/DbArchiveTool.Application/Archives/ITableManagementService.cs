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

    /// <summary>
    /// 对比源表和目标表的结构是否一致(用于归档前的预检查)
    /// </summary>
    /// <param name="sourceConnectionString">源数据库连接字符串</param>
    /// <param name="sourceSchemaName">源架构名称</param>
    /// <param name="sourceTableName">源表名</param>
    /// <param name="targetConnectionString">目标数据库连接字符串(可为 null,表示与源相同)</param>
    /// <param name="targetDatabaseName">目标数据库名称(可为 null,表示与源相同)</param>
    /// <param name="targetSchemaName">目标架构名称</param>
    /// <param name="targetTableName">目标表名</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>表结构对比结果</returns>
    Task<TableSchemaComparisonResult> CompareTableSchemasAsync(
        string sourceConnectionString,
        string sourceSchemaName,
        string sourceTableName,
        string? targetConnectionString,
        string? targetDatabaseName,
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

/// <summary>
/// 表结构对比结果
/// </summary>
public sealed class TableSchemaComparisonResult
{
    /// <summary>
    /// 目标表是否存在
    /// </summary>
    public bool TargetTableExists { get; init; }

    /// <summary>
    /// 表结构是否一致(仅在目标表存在时有意义)
    /// </summary>
    public bool IsCompatible { get; init; }

    /// <summary>
    /// 差异描述(如果不一致)
    /// </summary>
    public string? DifferenceDescription { get; init; }

    /// <summary>
    /// 源表列数
    /// </summary>
    public int SourceColumnCount { get; init; }

    /// <summary>
    /// 目标表列数(如果目标表存在)
    /// </summary>
    public int? TargetColumnCount { get; init; }

    /// <summary>
    /// 缺失的列(在源表中存在但目标表中不存在)
    /// </summary>
    public List<string> MissingColumns { get; init; } = new();

    /// <summary>
    /// 类型不匹配的列
    /// </summary>
    public List<string> TypeMismatchColumns { get; init; } = new();

    /// <summary>
    /// 长度不足的列
    /// </summary>
    public List<string> LengthInsufficientColumns { get; init; } = new();

    /// <summary>
    /// 精度不足的列
    /// </summary>
    public List<string> PrecisionInsufficientColumns { get; init; } = new();
}
