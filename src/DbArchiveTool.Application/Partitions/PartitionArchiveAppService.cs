using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DbArchiveTool.Application.Abstractions;
using DbArchiveTool.Application.Archives;
using DbArchiveTool.Domain.DataSources;
using DbArchiveTool.Domain.Partitions;
using DbArchiveTool.Shared.Archive;
using DbArchiveTool.Shared.Partitions;
using DbArchiveTool.Shared.Results;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Application.Partitions;

/// <summary>
/// 分区归档服务,提供 BCP 和 BulkCopy 归档能力。
/// </summary>
internal sealed class PartitionArchiveAppService : IPartitionArchiveAppService
{
    private readonly IBackgroundTaskRepository taskRepository;
    private readonly IDataSourceRepository dataSourceRepository;
    private readonly ITableManagementService tableManagementService;
    private readonly IPasswordEncryptionService passwordEncryptionService;
    private readonly IBackgroundTaskDispatcher dispatcher;
    private readonly ILogger<PartitionArchiveAppService> logger;

    public PartitionArchiveAppService(
        IBackgroundTaskRepository taskRepository,
        IDataSourceRepository dataSourceRepository,
        ITableManagementService tableManagementService,
        IPasswordEncryptionService passwordEncryptionService,
        IBackgroundTaskDispatcher dispatcher,
        ILogger<PartitionArchiveAppService> logger)
    {
        this.taskRepository = taskRepository;
        this.dataSourceRepository = dataSourceRepository;
        this.tableManagementService = tableManagementService;
        this.passwordEncryptionService = passwordEncryptionService;
        this.dispatcher = dispatcher;
        this.logger = logger;
    }

    public Task<Result<ArchivePlanDto>> PlanArchiveWithBcpAsync(BcpArchivePlanRequest request, CancellationToken cancellationToken = default)
    {
        var message = BuildPlannedMessage(
            "BCP",
            request.TargetConnectionString,
            request.TargetDatabase,
            request.TargetTable);
        var dto = new ArchivePlanDto("BCP", "Planned", message);
        return Task.FromResult(Result<ArchivePlanDto>.Success(dto));
    }

    public Task<Result<ArchivePlanDto>> PlanArchiveWithBulkCopyAsync(BulkCopyArchivePlanRequest request, CancellationToken cancellationToken = default)
    {
        var message = BuildPlannedMessage(
            "BulkCopy",
            request.TargetConnectionString,
            request.TargetDatabase,
            request.TargetTable);
        var dto = new ArchivePlanDto("BulkCopy", "Planned", message);
        return Task.FromResult(Result<ArchivePlanDto>.Success(dto));
    }

    public async Task<Result<ArchiveInspectionResultDto>> InspectBcpAsync(BcpArchiveInspectRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            logger.LogInformation("开始 BCP 归档预检: DataSource={DataSourceId}, Table={Schema}.{Table}",
                request.DataSourceId, request.SchemaName, request.TableName);

            var blockingIssues = new List<ArchiveInspectionIssue>();
            var warnings = new List<ArchiveInspectionIssue>();
            var autoFixSteps = new List<ArchiveInspectionAutoFixStep>();

            // 验证数据源
            var dataSource = await dataSourceRepository.GetAsync(request.DataSourceId, cancellationToken);
            if (dataSource is null)
            {
                blockingIssues.Add(new ArchiveInspectionIssue("DS001", $"数据源不存在: {request.DataSourceId}", null));
                return Result<ArchiveInspectionResultDto>.Success(new ArchiveInspectionResultDto(
                    false, false, false, null, blockingIssues, warnings, autoFixSteps));
            }

            // 构建源连接字符串
            var sourceConnectionString = BuildConnectionString(dataSource);

            // 解析目标表的 Schema 和 TableName
            var (targetSchema, targetTable) = ParseTableName(request.TargetTable);

            // 检查目标表是否存在
            bool targetTableExists = false;
            try
            {
                // 这里假设目标数据库和源数据库相同,如果不同需要从请求中获取目标连接字符串
                targetTableExists = await tableManagementService.CheckTableExistsAsync(
                    sourceConnectionString,
                    targetSchema,
                    targetTable,
                    cancellationToken);

                logger.LogDebug("目标表存在性检查: {Schema}.{Table} = {Exists}",
                    targetSchema, targetTable, targetTableExists);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "检查目标表存在性时发生异常");
                warnings.Add(new ArchiveInspectionIssue(
                    "CHK001",
                    "无法检查目标表存在性",
                    ex.Message));
            }

            // 如果目标表存在，对比表结构
            if (targetTableExists)
            {
                try
                {
                    var comparisonResult = await tableManagementService.CompareTableSchemasAsync(
                        sourceConnectionString,
                        request.SchemaName,
                        request.TableName,
                        null, // 目标连接字符串(null = 与源相同)
                        request.TargetDatabase, // 目标数据库名称
                        targetSchema,
                        targetTable,
                        cancellationToken);

                    if (!comparisonResult.IsCompatible)
                    {
                        // 表结构不一致，添加为阻塞性问题，禁止执行
                        blockingIssues.Add(new ArchiveInspectionIssue(
                            "SCH001",
                            "表结构不一致",
                            comparisonResult.DifferenceDescription));

                        // 如果有缺失的列或类型不匹配，可以提供自动修复建议
                        if (comparisonResult.MissingColumns.Count > 0 || 
                            comparisonResult.TypeMismatchColumns.Count > 0 ||
                            comparisonResult.LengthInsufficientColumns.Count > 0 ||
                            comparisonResult.PrecisionInsufficientColumns.Count > 0)
                        {
                            var alterScript = GenerateAlterTableScript(
                                targetSchema,
                                targetTable,
                                comparisonResult);

                            autoFixSteps.Add(new ArchiveInspectionAutoFixStep(
                                "ALTER_TARGET_TABLE",
                                "调整目标表结构",
                                alterScript));
                        }

                        logger.LogError(
                            "表结构不一致(阻塞): {SourceTable} vs {TargetTable}, 差异: {Differences}",
                            $"{request.SchemaName}.{request.TableName}",
                            request.TargetTable,
                            comparisonResult.DifferenceDescription);
                    }
                    else
                    {
                        logger.LogDebug(
                            "表结构一致: 源={SourceTable}, 目标={TargetTable}, 共 {ColCount} 列",
                            $"{request.SchemaName}.{request.TableName}",
                            request.TargetTable,
                            comparisonResult.SourceColumnCount);
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "对比表结构时发生异常");
                    blockingIssues.Add(new ArchiveInspectionIssue(
                        "SCH002",
                        "无法对比表结构",
                        ex.Message));
                }
            }
            else
            {
                // 如果目标表不存在，添加自动补齐步骤
                autoFixSteps.Add(new ArchiveInspectionAutoFixStep(
                    "CREATE_TARGET_TABLE",
                    "创建目标表",
                    $"CREATE TABLE {request.TargetTable} AS (SELECT TOP 0 * FROM {request.SchemaName}.{request.TableName})"));

                warnings.Add(new ArchiveInspectionIssue(
                    "TBL001",
                    "目标表不存在",
                    "系统可以自动创建与源表结构相同的普通表"));
            }

            // 检查 BCP 命令是否可用
            string? bcpCommandPath = null;
            bool hasBcpCommand = await CheckBcpAvailabilityAsync();

            // TODO: 检查权限 - 需要查询数据库权限
            bool hasRequiredPermissions = true; // 简化实现,假设有权限

            // 检查 BCP 命令
            if (!hasBcpCommand)
            {
                blockingIssues.Add(new ArchiveInspectionIssue(
                    "BCP001",
                    "未找到 bcp.exe 命令",
                    "请安装 SQL Server 客户端工具或确保 bcp.exe 在系统 PATH 中"));
            }

            // 检查临时目录
            if (!Directory.Exists(request.TempDirectory))
            {
                warnings.Add(new ArchiveInspectionIssue(
                    "DIR001",
                    $"临时目录不存在: {request.TempDirectory}",
                    "系统将自动创建该目录"));
            }

            bool canExecute = blockingIssues.Count == 0;
            
            logger.LogInformation("BCP 预检完成: CanExecute={CanExecute}, Issues={IssueCount}", 
                canExecute, blockingIssues.Count);

            return Result<ArchiveInspectionResultDto>.Success(new ArchiveInspectionResultDto(
                canExecute,
                targetTableExists,
                hasRequiredPermissions,
                bcpCommandPath,
                blockingIssues,
                warnings,
                autoFixSteps));
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "BCP 预检失败");
            return Result<ArchiveInspectionResultDto>.Failure($"预检失败: {ex.Message}");
        }
    }

    public async Task<Result<ArchiveInspectionResultDto>> InspectBulkCopyAsync(BulkCopyArchiveInspectRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            logger.LogInformation("开始 BulkCopy 归档预检: DataSource={DataSourceId}, Table={Schema}.{Table}",
                request.DataSourceId, request.SchemaName, request.TableName);

            var blockingIssues = new List<ArchiveInspectionIssue>();
            var warnings = new List<ArchiveInspectionIssue>();
            var autoFixSteps = new List<ArchiveInspectionAutoFixStep>();

            // 验证数据源
            var dataSource = await dataSourceRepository.GetAsync(request.DataSourceId, cancellationToken);
            if (dataSource is null)
            {
                blockingIssues.Add(new ArchiveInspectionIssue("DS001", $"数据源不存在: {request.DataSourceId}", null));
                return Result<ArchiveInspectionResultDto>.Success(new ArchiveInspectionResultDto(
                    false, false, false, null, blockingIssues, warnings, autoFixSteps));
            }

            // 构建源连接字符串
            var sourceConnectionString = BuildConnectionString(dataSource);

            // 解析目标表的 Schema 和 TableName
            var (targetSchema, targetTable) = ParseTableName(request.TargetTable);

            // 检查目标表是否存在
            bool targetTableExists = false;
            try
            {
                // 这里假设目标数据库和源数据库相同,如果不同需要从请求中获取目标连接字符串
                targetTableExists = await tableManagementService.CheckTableExistsAsync(
                    sourceConnectionString,
                    targetSchema,
                    targetTable,
                    cancellationToken);

                logger.LogDebug("目标表存在性检查: {Schema}.{Table} = {Exists}",
                    targetSchema, targetTable, targetTableExists);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "检查目标表存在性时发生异常");
                warnings.Add(new ArchiveInspectionIssue(
                    "CHK001",
                    "无法检查目标表存在性",
                    ex.Message));
            }

            // 如果目标表存在，对比表结构
            if (targetTableExists)
            {
                try
                {
                    var comparisonResult = await tableManagementService.CompareTableSchemasAsync(
                        sourceConnectionString,
                        request.SchemaName,
                        request.TableName,
                        null, // 目标连接字符串(null = 与源相同)
                        request.TargetDatabase, // 目标数据库名称
                        targetSchema,
                        targetTable,
                        cancellationToken);

                    if (!comparisonResult.IsCompatible)
                    {
                        // 表结构不一致，添加为阻塞性问题，禁止执行
                        blockingIssues.Add(new ArchiveInspectionIssue(
                            "SCH001",
                            "表结构不一致",
                            comparisonResult.DifferenceDescription));

                        // 如果有缺失的列或类型不匹配，可以提供自动修复建议
                        if (comparisonResult.MissingColumns.Count > 0 || 
                            comparisonResult.TypeMismatchColumns.Count > 0 ||
                            comparisonResult.LengthInsufficientColumns.Count > 0 ||
                            comparisonResult.PrecisionInsufficientColumns.Count > 0)
                        {
                            var alterScript = GenerateAlterTableScript(
                                targetSchema,
                                targetTable,
                                comparisonResult);

                            autoFixSteps.Add(new ArchiveInspectionAutoFixStep(
                                "ALTER_TARGET_TABLE",
                                "调整目标表结构",
                                alterScript));
                        }

                        logger.LogError(
                            "表结构不一致(阻塞): {SourceTable} vs {TargetTable}, 差异: {Differences}",
                            $"{request.SchemaName}.{request.TableName}",
                            request.TargetTable,
                            comparisonResult.DifferenceDescription);
                    }
                    else
                    {
                        logger.LogDebug(
                            "表结构一致: 源={SourceTable}, 目标={TargetTable}, 共 {ColCount} 列",
                            $"{request.SchemaName}.{request.TableName}",
                            request.TargetTable,
                            comparisonResult.SourceColumnCount);
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "对比表结构时发生异常");
                    blockingIssues.Add(new ArchiveInspectionIssue(
                        "SCH002",
                        "无法对比表结构",
                        ex.Message));
                }
            }
            else
            {
                // 如果目标表不存在,添加自动补齐步骤
                autoFixSteps.Add(new ArchiveInspectionAutoFixStep(
                    "CREATE_TARGET_TABLE",
                    "创建目标表",
                    $"CREATE TABLE {request.TargetTable} AS (SELECT TOP 0 * FROM {request.SchemaName}.{request.TableName})"));

                warnings.Add(new ArchiveInspectionIssue(
                    "TBL001",
                    "目标表不存在",
                    "系统可以自动创建与源表结构相同的普通表"));
            }

            // 检查 INSERT 权限（简化实现，假设有权限）
            bool hasInsertPermission = true;

            bool canExecute = blockingIssues.Count == 0;
            
            logger.LogInformation("BulkCopy 预检完成: CanExecute={CanExecute}, TargetExists={TargetExists}, Issues={IssueCount}", 
                canExecute, targetTableExists, blockingIssues.Count);

            return Result<ArchiveInspectionResultDto>.Success(new ArchiveInspectionResultDto(
                canExecute,
                targetTableExists,
                hasInsertPermission,
                null,
                blockingIssues,
                warnings,
                autoFixSteps));
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "BulkCopy 预检失败");
            return Result<ArchiveInspectionResultDto>.Failure($"预检失败: {ex.Message}");
        }
    }

    public async Task<Result<Guid>> ExecuteWithBcpAsync(BcpArchiveExecuteRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            logger.LogInformation("开始创建 BCP 归档任务: DataSource={DataSourceId}, Table={Schema}.{Table}",
                request.DataSourceId, request.SchemaName, request.TableName);

            // 验证数据源
            var dataSource = await dataSourceRepository.GetAsync(request.DataSourceId, cancellationToken);
            if (dataSource is null)
            {
                return Result<Guid>.Failure($"数据源不存在: {request.DataSourceId}");
            }

            // 创建后台任务
            var configSnapshot = JsonSerializer.Serialize(new
            {
                request.SchemaName,
                request.TableName,
                request.SourcePartitionKey,
                request.TargetTable,
                request.TargetDatabase,
                request.TempDirectory,
                request.BatchSize,
                request.UseNativeFormat,
                request.MaxErrors,
                request.TimeoutSeconds
            });

            // 由于 BCP/BulkCopy 不需要 PartitionConfiguration, 使用 null
            var task = BackgroundTask.Create(
                partitionConfigurationId: null,
                dataSourceId: request.DataSourceId,
                requestedBy: request.RequestedBy,
                createdBy: request.RequestedBy,
                backupReference: null,
                notes: $"BCP 归档: {request.SchemaName}.{request.TableName} 分区 {request.SourcePartitionKey}",
                priority: 0,
                operationType: BackgroundTaskOperationType.ArchiveBcp,
                archiveScheme: ArchiveMethod.Bcp.ToString(),
                archiveTargetConnection: null,
                archiveTargetDatabase: request.TargetDatabase,
                archiveTargetTable: request.TargetTable);

            task.SaveConfigurationSnapshot(configSnapshot, request.RequestedBy);
            // 注意：不要提前调用 MarkQueued()，让 BackgroundTaskProcessor 按正确流程处理状态转换

            await taskRepository.AddAsync(task, cancellationToken);
            await dataSourceRepository.SaveChangesAsync(cancellationToken);

            // 将任务加入执行队列
            await dispatcher.DispatchAsync(task.Id, request.DataSourceId, cancellationToken);

            logger.LogInformation("BCP 归档任务已创建并入队: TaskId={TaskId}", task.Id);
            return Result<Guid>.Success(task.Id);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "创建 BCP 归档任务失败");
            return Result<Guid>.Failure($"创建任务失败: {ex.Message}");
        }
    }

    public async Task<Result<Guid>> ExecuteWithBulkCopyAsync(BulkCopyArchiveExecuteRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            logger.LogInformation("开始创建 BulkCopy 归档任务: DataSource={DataSourceId}, Table={Schema}.{Table}",
                request.DataSourceId, request.SchemaName, request.TableName);

            // 验证数据源
            var dataSource = await dataSourceRepository.GetAsync(request.DataSourceId, cancellationToken);
            if (dataSource is null)
            {
                return Result<Guid>.Failure($"数据源不存在: {request.DataSourceId}");
            }

            // 创建后台任务
            var configSnapshot = JsonSerializer.Serialize(new
            {
                request.SchemaName,
                request.TableName,
                request.SourcePartitionKey,
                request.TargetTable,
                request.TargetDatabase,
                request.BatchSize,
                request.NotifyAfterRows,
                request.TimeoutSeconds,
                request.EnableStreaming
            });

            // 由于 BCP/BulkCopy 不需要 PartitionConfiguration, 使用 null
            var task = BackgroundTask.Create(
                partitionConfigurationId: null,
                dataSourceId: request.DataSourceId,
                requestedBy: request.RequestedBy,
                createdBy: request.RequestedBy,
                backupReference: null,
                notes: $"BulkCopy 归档: {request.SchemaName}.{request.TableName} 分区 {request.SourcePartitionKey}",
                priority: 0,
                operationType: BackgroundTaskOperationType.ArchiveBulkCopy,
                archiveScheme: ArchiveMethod.BulkCopy.ToString(),
                archiveTargetConnection: null,
                archiveTargetDatabase: request.TargetDatabase,
                archiveTargetTable: request.TargetTable);

            task.SaveConfigurationSnapshot(configSnapshot, request.RequestedBy);
            // 注意：不要提前调用 MarkQueued()，让 BackgroundTaskProcessor 按正确流程处理状态转换

            await taskRepository.AddAsync(task, cancellationToken);
            await dataSourceRepository.SaveChangesAsync(cancellationToken);

            // 将任务加入执行队列
            await dispatcher.DispatchAsync(task.Id, request.DataSourceId, cancellationToken);

            logger.LogInformation("BulkCopy 归档任务已创建并入队: TaskId={TaskId}", task.Id);
            return Result<Guid>.Success(task.Id);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "创建 BulkCopy 归档任务失败");
            return Result<Guid>.Failure($"创建任务失败: {ex.Message}");
        }
    }

    public async Task<Result<string>> ExecuteAutoFixAsync(ArchiveAutoFixRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            logger.LogInformation(
                "开始执行自动修复: FixCode={FixCode}, DataSource={DataSourceId}, Table={TargetTable}",
                request.FixCode, request.DataSourceId, request.TargetTable);

            // 验证数据源
            var dataSource = await dataSourceRepository.GetAsync(request.DataSourceId, cancellationToken);
            if (dataSource is null)
            {
                return Result<string>.Failure($"数据源不存在: {request.DataSourceId}");
            }

            // 构建连接字符串
            var sourceConnectionString = BuildConnectionString(dataSource);
            // 构建目标连接字符串(支持自定义目标服务器)
            var targetConnectionString = BuildTargetConnectionString(dataSource, request.TargetDatabase);

            logger.LogInformation(
                "自动修复连接信息: UseSourceAsTarget={UseSourceAsTarget}, TargetDatabase={TargetDatabase}",
                dataSource.UseSourceAsTarget, request.TargetDatabase ?? "(使用源数据库)");

            // 解析表名
            var (sourceSchema, sourceTable) = (request.SchemaName, request.TableName);
            var (targetSchema, targetTable) = ParseTableName(request.TargetTable);

            // 根据修复代码执行相应的操作
            switch (request.FixCode)
            {
                case "CREATE_TARGET_TABLE":
                    // 创建目标表
                    var result = await tableManagementService.CreateTargetTableAsync(
                        sourceConnectionString,
                        targetConnectionString,
                        sourceSchema,
                        sourceTable,
                        targetSchema,
                        targetTable,
                        cancellationToken);

                    if (!result.Success)
                    {
                        return Result<string>.Failure($"创建目标表失败: {result.ErrorMessage}");
                    }

                    logger.LogInformation(
                        "目标表创建成功: {TargetSchema}.{TargetTable}, 列数={ColumnCount}",
                        targetSchema, targetTable, result.ColumnCount);

                    return Result<string>.Success(
                        $"目标表 {targetSchema}.{targetTable} 创建成功,包含 {result.ColumnCount} 列");

                default:
                    return Result<string>.Failure($"不支持的修复代码: {request.FixCode}");
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "执行自动修复失败: FixCode={FixCode}", request.FixCode);
            return Result<string>.Failure($"执行修复失败: {ex.Message}");
        }
    }

    private static string BuildPlannedMessage(string scheme, string? connection, string? database, string? table)
    {
        var targetInfo = string.Join(" / ", new[]
        {
            string.IsNullOrWhiteSpace(database) ? "目标库未指定" : database!,
            string.IsNullOrWhiteSpace(table) ? "目标表未指定" : table!
        });

        var connectionInfo = string.IsNullOrWhiteSpace(connection)
            ? "请在后续实现中提供目标实例连接。"
            : $"目标连接：{connection}";

        return $"{scheme} 归档方案规划中，当前仅输出计划结果。\n{connectionInfo}\n目标: {targetInfo}\n请选择其他方案或等待后续版本启用实际执行能力。";
    }

    /// <summary>
    /// 检查 BCP 工具是否可用
    /// </summary>
    /// <returns>如果 BCP 工具可用返回 true，否则返回 false</returns>
    private async Task<bool> CheckBcpAvailabilityAsync()
    {
        try
        {
            // 尝试执行 bcp -v 命令检查版本
            using var process = new System.Diagnostics.Process
            {
                StartInfo = new System.Diagnostics.ProcessStartInfo
                {
                    FileName = "bcp",
                    Arguments = "-v",
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = true
                }
            };

            process.Start();
            
            // 读取输出(不阻塞)
            var outputTask = process.StandardOutput.ReadToEndAsync();
            var errorTask = process.StandardError.ReadToEndAsync();
            
            // 等待进程结束,最多等待 5 秒
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await process.WaitForExitAsync(cts.Token);

            var output = await outputTask;
            var error = await errorTask;

            // BCP 命令存在且能执行
            if (process.ExitCode == 0 || !string.IsNullOrWhiteSpace(output) || !string.IsNullOrWhiteSpace(error))
            {
                logger.LogDebug("BCP 工具检查成功: ExitCode={ExitCode}, Output={Output}", 
                    process.ExitCode, output.Trim());
                return true;
            }

            logger.LogWarning("BCP 工具检查失败: ExitCode={ExitCode}", process.ExitCode);
            return false;
        }
        catch (System.ComponentModel.Win32Exception ex) when (ex.NativeErrorCode == 2)
        {
            // 文件未找到错误 (ERROR_FILE_NOT_FOUND)
            logger.LogWarning("BCP 工具未找到: {Message}", ex.Message);
            return false;
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("BCP 工具检查超时");
            return false;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "检查 BCP 工具可用性时发生异常");
            return false;
        }
    }

    /// <summary>
    /// 解析表名,分离 Schema 和 TableName
    /// 支持格式: "schema.table" 或 "[schema].[table]" 或 "table" (默认 dbo)
    /// </summary>
    private static (string Schema, string TableName) ParseTableName(string fullTableName)
    {
        if (string.IsNullOrWhiteSpace(fullTableName))
        {
            throw new ArgumentException("表名不能为空", nameof(fullTableName));
        }

        // 移除首尾空格
        fullTableName = fullTableName.Trim();

        // 查找点号分隔符
        var dotIndex = fullTableName.IndexOf('.');
        if (dotIndex < 0)
        {
            // 没有点号,使用默认 schema
            return ("dbo", RemoveBrackets(fullTableName));
        }

        var schema = fullTableName.Substring(0, dotIndex);
        var tableName = fullTableName.Substring(dotIndex + 1);

        return (RemoveBrackets(schema), RemoveBrackets(tableName));
    }

    /// <summary>
    /// 移除方括号
    /// </summary>
    private static string RemoveBrackets(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return name;
        }

        name = name.Trim();

        if (name.StartsWith("[") && name.EndsWith("]"))
        {
            return name.Substring(1, name.Length - 2);
        }

        return name;
    }

    /// <summary>
    /// 构建数据库连接字符串
    /// </summary>
    private string BuildConnectionString(ArchiveDataSource dataSource)
    {
        var builder = new Microsoft.Data.SqlClient.SqlConnectionStringBuilder
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
    /// 构建目标服务器连接字符串
    /// </summary>
    private string BuildTargetConnectionString(ArchiveDataSource dataSource, string? targetDatabase = null)
    {
        // 如果使用源服务器作为目标服务器
        if (dataSource.UseSourceAsTarget)
        {
            // 如果指定了目标数据库,修改连接字符串的数据库
            if (!string.IsNullOrWhiteSpace(targetDatabase))
            {
                var builder = new Microsoft.Data.SqlClient.SqlConnectionStringBuilder(BuildConnectionString(dataSource))
                {
                    InitialCatalog = targetDatabase
                };
                return builder.ConnectionString;
            }
            
            return BuildConnectionString(dataSource);
        }

        // 使用自定义目标服务器
        var targetBuilder = new Microsoft.Data.SqlClient.SqlConnectionStringBuilder
        {
            DataSource = dataSource.TargetServerPort == 1433
                ? dataSource.TargetServerAddress
                : $"{dataSource.TargetServerAddress},{dataSource.TargetServerPort}",
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

    /// <summary>
    /// 生成 ALTER TABLE 脚本(用于修复表结构差异)
    /// </summary>
    private static string GenerateAlterTableScript(
        string targetSchema,
        string targetTable,
        TableSchemaComparisonResult comparisonResult)
    {
        var script = new System.Text.StringBuilder();
        script.AppendLine($"-- 调整目标表结构: [{targetSchema}].[{targetTable}]");
        script.AppendLine($"-- 注意: 以下脚本仅为参考，请根据实际情况调整");
        script.AppendLine();

        if (comparisonResult.MissingColumns.Count > 0)
        {
            script.AppendLine("-- 添加缺失的列:");
            foreach (var col in comparisonResult.MissingColumns)
            {
                script.AppendLine($"-- ALTER TABLE [{targetSchema}].[{targetTable}] ADD [{col}] <数据类型> NULL;");
            }
            script.AppendLine();
        }

        if (comparisonResult.TypeMismatchColumns.Count > 0)
        {
            script.AppendLine("-- 修改类型不匹配的列:");
            foreach (var col in comparisonResult.TypeMismatchColumns)
            {
                script.AppendLine($"-- ALTER TABLE [{targetSchema}].[{targetTable}] ALTER COLUMN [{col}] <新数据类型>;");
            }
            script.AppendLine();
        }

        if (comparisonResult.LengthInsufficientColumns.Count > 0)
        {
            script.AppendLine("-- 调整长度不足的列:");
            foreach (var col in comparisonResult.LengthInsufficientColumns)
            {
                script.AppendLine($"-- ALTER TABLE [{targetSchema}].[{targetTable}] ALTER COLUMN [{col}] <数据类型>(<新长度>);");
            }
            script.AppendLine();
        }

        if (comparisonResult.PrecisionInsufficientColumns.Count > 0)
        {
            script.AppendLine("-- 调整精度不足的列:");
            foreach (var col in comparisonResult.PrecisionInsufficientColumns)
            {
                script.AppendLine($"-- ALTER TABLE [{targetSchema}].[{targetTable}] ALTER COLUMN [{col}] DECIMAL(<新精度>, <新小数位>);");
            }
            script.AppendLine();
        }

        script.AppendLine("-- 请使用 SSMS 对比两个表的架构，生成准确的修改脚本");

        return script.ToString();
    }
}

