using System.Threading;
using DbArchiveTool.Domain.DataSources;
using DbArchiveTool.Infrastructure.Queries;
using DbArchiveTool.Infrastructure.SqlExecution;
using Microsoft.AspNetCore.Mvc;

namespace DbArchiveTool.Api.Controllers.V1;

/// <summary>
/// 分区相关元数据查询接口。
/// </summary>
[ApiController]
[Route("api/v1/data-sources/{dataSourceId:guid}/partitions")]
public class PartitionInfoController : ControllerBase
{
    private readonly IDataSourceRepository dataSourceRepository;
    private readonly SqlPartitionQueryService queryService;
    private readonly IDbConnectionFactory connectionFactory;

    public PartitionInfoController(
        IDataSourceRepository dataSourceRepository,
        SqlPartitionQueryService queryService,
        IDbConnectionFactory connectionFactory)
    {
        this.dataSourceRepository = dataSourceRepository;
        this.queryService = queryService;
        this.connectionFactory = connectionFactory;
    }

    /// <summary>获取数据源下的全部用户表。</summary>
    [HttpGet("~/api/v1/data-sources/{dataSourceId:guid}/tables")]
    public async Task<ActionResult<List<DatabaseTableDto>>> GetDatabaseTables(Guid dataSourceId, CancellationToken cancellationToken)
    {
        var dataSource = await dataSourceRepository.GetAsync(dataSourceId, cancellationToken);
        if (dataSource is null)
        {
            return NotFound(new { message = "数据源不存在" });
        }

        try
        {
            var connectionString = connectionFactory.BuildConnectionString(dataSource);
            var tables = await queryService.GetDatabaseTablesAsync(connectionString);
            return Ok(tables);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { message = $"查询表列表失败: {ex.Message}" });
        }
    }

    /// <summary>获取指定数据源下的分区表列表。</summary>
    [HttpGet("tables")]
    public async Task<ActionResult<List<PartitionTableDto>>> GetPartitionTables(Guid dataSourceId, CancellationToken cancellationToken)
    {
        var dataSource = await dataSourceRepository.GetAsync(dataSourceId, cancellationToken);
        if (dataSource is null)
        {
            return NotFound(new { message = "数据源不存在" });
        }

        try
        {
            var connectionString = connectionFactory.BuildConnectionString(dataSource);
            var tables = await queryService.GetPartitionTablesAsync(connectionString);
            return Ok(tables);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { message = $"查询分区表失败: {ex.Message}" });
        }
    }

    /// <summary>获取指定表的分区详情。</summary>
    [HttpGet("tables/{schemaName}/{tableName}/details")]
    public async Task<ActionResult<List<PartitionDetailDto>>> GetPartitionDetails(
        Guid dataSourceId,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken)
    {
        var dataSource = await dataSourceRepository.GetAsync(dataSourceId, cancellationToken);
        if (dataSource is null)
        {
            return NotFound(new { message = "数据源不存在" });
        }

        try
        {
            var connectionString = connectionFactory.BuildConnectionString(dataSource);
            var details = await queryService.GetPartitionDetailsAsync(connectionString, schemaName, tableName);
            return Ok(details);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { message = $"查询分区详情失败: {ex.Message}" });
        }
    }

    /// <summary>获取指定表的列信息（通用入口）。</summary>
    [HttpGet("~/api/v1/data-sources/{dataSourceId:guid}/tables/{schemaName}/{tableName}/columns")]
    public async Task<ActionResult<List<PartitionTableColumnDto>>> GetDatabaseTableColumns(
        Guid dataSourceId,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken)
    {
        var dataSource = await dataSourceRepository.GetAsync(dataSourceId, cancellationToken);
        if (dataSource is null)
        {
            return NotFound(new { message = "数据源不存在" });
        }

        try
        {
            var connectionString = connectionFactory.BuildConnectionString(dataSource);
            var columns = await queryService.GetTableColumnsAsync(connectionString, schemaName, tableName);
            return Ok(columns);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { message = $"查询列信息失败: {ex.Message}" });
        }
    }

    /// <summary>获取指定表的列信息。</summary>
    [HttpGet("tables/{schemaName}/{tableName}/columns")]
    public async Task<ActionResult<List<PartitionTableColumnDto>>> GetTableColumns(
        Guid dataSourceId,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken)
    {
        var dataSource = await dataSourceRepository.GetAsync(dataSourceId, cancellationToken);
        if (dataSource is null)
        {
            return NotFound(new { message = "数据源不存在" });
        }

        try
        {
            var connectionString = connectionFactory.BuildConnectionString(dataSource);
            var columns = await queryService.GetTableColumnsAsync(connectionString, schemaName, tableName);
            return Ok(columns);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { message = $"查询列信息失败: {ex.Message}" });
        }
    }

    /// <summary>获取指定列的统计信息（最小值、最大值等）。</summary>
    [HttpGet("tables/{schemaName}/{tableName}/columns/{columnName}/statistics")]
    public async Task<ActionResult<PartitionColumnStatisticsDto>> GetColumnStatistics(
        Guid dataSourceId,
        string schemaName,
        string tableName,
        string columnName,
        CancellationToken cancellationToken)
    {
        var dataSource = await dataSourceRepository.GetAsync(dataSourceId, cancellationToken);
        if (dataSource is null)
        {
            return NotFound(new { message = "数据源不存在" });
        }

        try
        {
            var connectionString = connectionFactory.BuildConnectionString(dataSource);
            var stats = await queryService.GetColumnStatisticsAsync(connectionString, schemaName, tableName, columnName);
            return Ok(stats);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { message = $"查询列统计信息失败: {ex.Message}" });
        }
    }

    /// <summary>获取指定列的统计信息（通用入口）。</summary>
    [HttpGet("~/api/v1/data-sources/{dataSourceId:guid}/tables/{schemaName}/{tableName}/columns/{columnName}/statistics")]
    public async Task<ActionResult<PartitionColumnStatisticsDto>> GetDatabaseColumnStatistics(
        Guid dataSourceId,
        string schemaName,
        string tableName,
        string columnName,
        CancellationToken cancellationToken)
    {
        var dataSource = await dataSourceRepository.GetAsync(dataSourceId, cancellationToken);
        if (dataSource is null)
        {
            return NotFound(new { message = "数据源不存在" });
        }

        try
        {
            var connectionString = connectionFactory.BuildConnectionString(dataSource);
            var stats = await queryService.GetColumnStatisticsAsync(connectionString, schemaName, tableName, columnName);
            return Ok(stats);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { message = $"查询列统计信息失败: {ex.Message}" });
        }
    }

    /// <summary>获取目标服务器的数据库列表。</summary>
    [HttpGet("target-databases")]
    public async Task<ActionResult<List<TargetDatabaseDto>>> GetTargetDatabases(Guid dataSourceId, CancellationToken cancellationToken)
    {
        var dataSource = await dataSourceRepository.GetAsync(dataSourceId, cancellationToken);
        if (dataSource is null)
        {
            return NotFound(new { message = "数据源不存在" });
        }

        try
        {
            var connectionString = connectionFactory.BuildTargetConnectionString(dataSource);
            var currentDb = dataSource.UseSourceAsTarget ? dataSource.DatabaseName : dataSource.TargetDatabaseName;
            var databases = await queryService.GetDatabasesAsync(connectionString, currentDb);
            return Ok(databases);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { message = $"查询目标数据库失败: {ex.Message}" });
        }
    }

    /// <summary>获取源库默认数据文件的目录。</summary>
    [HttpGet("default-file-path")]
    public async Task<ActionResult<object>> GetDefaultFilePath(Guid dataSourceId, CancellationToken cancellationToken)
    {
        var dataSource = await dataSourceRepository.GetAsync(dataSourceId, cancellationToken);
        if (dataSource is null)
        {
            return NotFound(new { message = "数据源不存在" });
        }

        try
        {
            var connectionString = connectionFactory.BuildConnectionString(dataSource);
            var path = await queryService.GetDefaultFilePathAsync(connectionString);
            return Ok(new DefaultFilePathResponse(path));
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { message = $"查询默认文件目录失败: {ex.Message}" });
        }
    }
}

internal sealed record DefaultFilePathResponse(string? Path);

