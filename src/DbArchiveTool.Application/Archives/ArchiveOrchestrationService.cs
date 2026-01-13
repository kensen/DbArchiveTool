using DbArchiveTool.Domain.ArchiveConfigurations;
using DbArchiveTool.Domain.DataSources;
using DbArchiveTool.Application.Abstractions;
using Microsoft.Extensions.Logging;

namespace DbArchiveTool.Application.Archives;

/// <summary>
/// 归档编排服务
/// 协调从配置读取到执行完成的完整归档流程
/// </summary>
public sealed class ArchiveOrchestrationService
{
    private readonly IArchiveConfigurationRepository _configRepository;
    private readonly IDataSourceRepository _dataSourceRepository;
    private readonly IArchiveExecutor _archiveExecutor;
    private readonly IPasswordEncryptionService _passwordEncryptionService;
    private readonly ILogger<ArchiveOrchestrationService> _logger;

    /// <summary>
    /// 初始化归档编排服务
    /// </summary>
    public ArchiveOrchestrationService(
        IArchiveConfigurationRepository configRepository,
        IDataSourceRepository dataSourceRepository,
        IArchiveExecutor archiveExecutor,
        IPasswordEncryptionService passwordEncryptionService,
        ILogger<ArchiveOrchestrationService> logger)
    {
        _configRepository = configRepository;
        _dataSourceRepository = dataSourceRepository;
        _archiveExecutor = archiveExecutor;
        _passwordEncryptionService = passwordEncryptionService;
        _logger = logger;
    }

    /// <summary>
    /// 执行归档任务(基于归档配置ID)
    /// </summary>
    /// <param name="configurationId">归档配置ID</param>
    /// <param name="partitionNumber">分区号(仅分区表需要)</param>
    /// <param name="progressCallback">进度回调</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>归档结果</returns>
    public async Task<ArchiveExecutionResult> ExecuteArchiveAsync(
        Guid configurationId,
        int? partitionNumber = null,
        Action<ArchiveProgressInfo>? progressCallback = null,
        CancellationToken cancellationToken = default)
    {
        var startTime = DateTime.UtcNow;

        try
        {
            _logger.LogInformation("开始执行归档任务: ConfigId={ConfigId}", configurationId);

            // 步骤 1: 加载归档配置
            var config = await _configRepository.GetByIdAsync(configurationId, cancellationToken);
            if (config == null)
            {
                return new ArchiveExecutionResult
                {
                    Success = false,
                    ConfigurationId = configurationId,
                    Message = $"归档配置不存在: {configurationId}",
                    StartTimeUtc = startTime,
                    EndTimeUtc = DateTime.UtcNow
                };
            }

            // 步骤 2: 加载数据源配置
            var dataSource = await _dataSourceRepository.GetAsync(config.DataSourceId, cancellationToken);
            if (dataSource == null)
            {
                return new ArchiveExecutionResult
                {
                    Success = false,
                    ConfigurationId = configurationId,
                    ConfigurationName = config.Name,
                    Message = $"数据源不存在: {config.DataSourceId}",
                    StartTimeUtc = startTime,
                    EndTimeUtc = DateTime.UtcNow
                };
            }

            if (!dataSource.IsEnabled)
            {
                return new ArchiveExecutionResult
                {
                    Success = false,
                    ConfigurationId = configurationId,
                    ConfigurationName = config.Name,
                    Message = "数据源已禁用",
                    StartTimeUtc = startTime,
                    EndTimeUtc = DateTime.UtcNow
                };
            }

            // 步骤 3: 构建目标连接字符串
            var targetConnectionString = BuildTargetConnectionString(dataSource);

            // 步骤 4: 执行归档
            _logger.LogInformation(
                "执行归档: {Schema}.{Table}, 分区号={Partition}",
                config.SourceSchemaName,
                config.SourceTableName,
                partitionNumber);

            var result = await _archiveExecutor.ExecuteAsync(
                config,
                dataSource,
                targetConnectionString,
                partitionNumber,
                progressCallback,
                cancellationToken);

            // 步骤 5: 保存执行结果(仅保留简单记录,不更新配置)
            if (result.Success)
            {
                _logger.LogInformation(
                    "归档成功: ConfigId={ConfigId}, 归档行数={RowsArchived}",
                    configurationId,
                    result.RowsArchived);
            }

            // 步骤 6: 返回执行结果
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "归档任务执行失败: ConfigId={ConfigId}", configurationId);

            return new ArchiveExecutionResult
            {
                Success = false,
                ConfigurationId = configurationId,
                Message = $"归档任务执行失败: {ex.Message}",
                ErrorDetails = ex.ToString(),
                StartTimeUtc = startTime,
                EndTimeUtc = DateTime.UtcNow
            };
        }
    }

    /// <summary>
    /// 执行归档任务(基于内存归档参数,用于定时归档任务)
    /// </summary>
    /// <param name="parameters">归档参数</param>
    /// <param name="progressCallback">进度回调</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>归档结果</returns>
    public async Task<ArchiveExecutionResult> ExecuteArchiveAsync(
        ArchiveParameters parameters,
        Action<ArchiveProgressInfo>? progressCallback = null,
        CancellationToken cancellationToken = default)
    {
        var startTime = DateTime.UtcNow;

        try
        {
            _logger.LogInformation(
                "开始执行归档任务(参数模式): Source={SourceSchema}.{SourceTable}, Target={TargetSchema}.{TargetTable}",
                parameters.SourceSchemaName,
                parameters.SourceTableName,
                parameters.TargetSchemaName,
                parameters.TargetTableName);

            // 步骤 1: 加载数据源配置
            var dataSource = await _dataSourceRepository.GetAsync(parameters.DataSourceId, cancellationToken);
            if (dataSource == null)
            {
                return new ArchiveExecutionResult
                {
                    Success = false,
                    Message = $"数据源不存在: {parameters.DataSourceId}",
                    StartTimeUtc = startTime,
                    EndTimeUtc = DateTime.UtcNow
                };
            }

            if (!dataSource.IsEnabled)
            {
                return new ArchiveExecutionResult
                {
                    Success = false,
                    Message = "数据源已禁用",
                    StartTimeUtc = startTime,
                    EndTimeUtc = DateTime.UtcNow
                };
            }

            // 步骤 2: 构建目标连接字符串
            var targetConnectionString = BuildTargetConnectionString(dataSource);

            // 步骤 3: 将参数转换为临时的 ArchiveConfiguration 对象
            var tempConfig = new ArchiveConfiguration(
                name: $"Temp_{parameters.SourceTableName}_{Guid.NewGuid():N}",
                description: "Temporary config for scheduled archive job",
                dataSourceId: parameters.DataSourceId,
                sourceSchemaName: parameters.SourceSchemaName,
                sourceTableName: parameters.SourceTableName,
                isPartitionedTable: false,
                archiveFilterColumn: parameters.ArchiveFilterColumn,
                archiveFilterCondition: parameters.ArchiveFilterCondition,
                archiveMethod: parameters.ArchiveMethod,
                deleteSourceDataAfterArchive: parameters.DeleteSourceDataAfterArchive,
                batchSize: parameters.BatchSize,
                partitionConfigurationId: null,
                targetSchemaName: parameters.TargetSchemaName,
                targetTableName: parameters.TargetTableName);

            // 步骤 4: 执行归档
            _logger.LogInformation(
                "执行归档: {Schema}.{Table}, 批次大小={BatchSize}",
                parameters.SourceSchemaName,
                parameters.SourceTableName,
                parameters.BatchSize);

            var result = await _archiveExecutor.ExecuteAsync(
                tempConfig,
                dataSource,
                targetConnectionString,
                partitionNumber: null,
                progressCallback,
                cancellationToken);

            // 步骤 5: 返回执行结果
            if (result.Success)
            {
                _logger.LogInformation(
                    "归档成功(参数模式): Source={SourceSchema}.{SourceTable}, 归档行数={RowsArchived}",
                    parameters.SourceSchemaName,
                    parameters.SourceTableName,
                    result.RowsArchived);
            }
            else
            {
                _logger.LogWarning(
                    "归档失败(参数模式): Source={SourceSchema}.{SourceTable}, 错误={Error}",
                    parameters.SourceSchemaName,
                    parameters.SourceTableName,
                    result.Message);
            }

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "归档任务执行失败(参数模式): Source={SourceSchema}.{SourceTable}",
                parameters.SourceSchemaName,
                parameters.SourceTableName);

            return new ArchiveExecutionResult
            {
                Success = false,
                Message = $"归档任务执行失败: {ex.Message}",
                ErrorDetails = ex.ToString(),
                StartTimeUtc = startTime,
                EndTimeUtc = DateTime.UtcNow
            };
        }
    }

    /// <summary>
    /// 批量执行归档任务
    /// </summary>
    /// <param name="configurationIds">归档配置ID列表</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>批量归档结果</returns>
    public async Task<BatchArchiveExecutionResult> ExecuteBatchArchiveAsync(
        IEnumerable<Guid> configurationIds,
        CancellationToken cancellationToken = default)
    {
        var startTime = DateTime.UtcNow;
        var results = new List<ArchiveExecutionResult>();

        foreach (var configId in configurationIds)
        {
            if (cancellationToken.IsCancellationRequested)
                break;

            var result = await ExecuteArchiveAsync(configId, null, null, cancellationToken);
            results.Add(result);
        }

        var successCount = results.Count(r => r.Success);
        var failureCount = results.Count(r => !r.Success);
        var totalRowsArchived = results.Where(r => r.Success).Sum(r => r.RowsArchived);

        return new BatchArchiveExecutionResult
        {
            TotalTasks = results.Count,
            SuccessCount = successCount,
            FailureCount = failureCount,
            TotalRowsArchived = totalRowsArchived,
            Results = results,
            StartTimeUtc = startTime,
            EndTimeUtc = DateTime.UtcNow,
            Duration = DateTime.UtcNow - startTime
        };
    }

    /// <summary>
    /// 获取归档配置列表
    /// </summary>
    /// <param name="dataSourceId">数据源ID(可选)</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>归档配置列表</returns>
    public async Task<IEnumerable<ArchiveConfiguration>> GetArchiveConfigurationsAsync(
        Guid? dataSourceId = null,
        CancellationToken cancellationToken = default)
    {
        var configs = await _configRepository.GetAllAsync(cancellationToken);

        if (dataSourceId.HasValue)
        {
            configs = configs.Where(c => c.DataSourceId == dataSourceId.Value).ToList();
        }

        return configs;
    }

    /// <summary>
    /// 构建目标数据库连接字符串
    /// </summary>
    private string BuildTargetConnectionString(ArchiveDataSource dataSource)
    {
        var builder = new Microsoft.Data.SqlClient.SqlConnectionStringBuilder();

        if (dataSource.UseSourceAsTarget)
        {
            // 使用源服务器作为目标
            builder.DataSource = $"{dataSource.ServerAddress},{dataSource.ServerPort}";
            builder.InitialCatalog = dataSource.DatabaseName;

            if (dataSource.UseIntegratedSecurity)
            {
                builder.IntegratedSecurity = true;
            }
            else
            {
                builder.IntegratedSecurity = false;
                builder.UserID = dataSource.UserName;
                // 兼容旧数据：未加密则原样返回；已加密则解密，避免密文写入连接字符串导致长度/校验异常
                builder.Password = _passwordEncryptionService.Decrypt(dataSource.Password ?? string.Empty);
            }
        }
        else
        {
            // 使用独立的目标服务器
            builder.DataSource = $"{dataSource.TargetServerAddress},{dataSource.TargetServerPort}";
            builder.InitialCatalog = dataSource.TargetDatabaseName;

            if (dataSource.TargetUseIntegratedSecurity)
            {
                builder.IntegratedSecurity = true;
            }
            else
            {
                builder.IntegratedSecurity = false;
                builder.UserID = dataSource.TargetUserName;
                // 兼容旧数据：未加密则原样返回；已加密则解密，避免密文写入连接字符串导致长度/校验异常
                builder.Password = _passwordEncryptionService.Decrypt(dataSource.TargetPassword ?? string.Empty);
            }
        }

        builder.TrustServerCertificate = true;
        builder.ConnectTimeout = 30;
        builder.Encrypt = false;

        return builder.ConnectionString;
    }
}
