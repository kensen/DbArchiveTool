using DbArchiveTool.Application.Abstractions;
using DbArchiveTool.Domain.DataSources;
using DbArchiveTool.Infrastructure.Queries;
using Microsoft.AspNetCore.Mvc;

namespace DbArchiveTool.Api.Controllers.V1;

/// <summary>
/// 分区信息查询接口
/// </summary>
[ApiController]
[Route("api/v1/data-sources/{dataSourceId:guid}/partitions")]
public class PartitionInfoController : ControllerBase
{
    private readonly IDataSourceRepository _dataSourceRepository;
    private readonly SqlPartitionQueryService _queryService;
    private readonly IPasswordEncryptionService _encryptionService;

    public PartitionInfoController(
        IDataSourceRepository dataSourceRepository,
        SqlPartitionQueryService queryService,
        IPasswordEncryptionService encryptionService)
    {
        _dataSourceRepository = dataSourceRepository;
        _queryService = queryService;
        _encryptionService = encryptionService;
    }

    /// <summary>
    /// 获取数据源的所有分区表列表
    /// </summary>
    [HttpGet("tables")]
    public async Task<ActionResult<List<PartitionTableDto>>> GetPartitionTables(Guid dataSourceId)
    {
        var dataSource = await _dataSourceRepository.GetAsync(dataSourceId);
        if (dataSource == null)
            return NotFound(new { message = "数据源不存在" });

        try
        {
            var connectionString = BuildConnectionString(dataSource);
            var tables = await _queryService.GetPartitionTablesAsync(connectionString);
            return Ok(tables);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { message = $"查询分区表失败: {ex.Message}" });
        }
    }

    /// <summary>
    /// 获取指定分区表的边界值明细
    /// </summary>
    [HttpGet("tables/{schemaName}/{tableName}/details")]
    public async Task<ActionResult<List<PartitionDetailDto>>> GetPartitionDetails(
        Guid dataSourceId,
        string schemaName,
        string tableName)
    {
        var dataSource = await _dataSourceRepository.GetAsync(dataSourceId);
        if (dataSource == null)
            return NotFound(new { message = "数据源不存在" });

        try
        {
            var connectionString = BuildConnectionString(dataSource);
            var details = await _queryService.GetPartitionDetailsAsync(connectionString, schemaName, tableName);
            return Ok(details);
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { message = $"查询分区明细失败: {ex.Message}" });
        }
    }

    /// <summary>
    /// 构建连接字符串
    /// </summary>
    private string BuildConnectionString(ArchiveDataSource dataSource)
    {
        var builder = new Microsoft.Data.SqlClient.SqlConnectionStringBuilder
        {
            DataSource = $"{dataSource.ServerAddress},{dataSource.ServerPort}",
            InitialCatalog = dataSource.DatabaseName,
            IntegratedSecurity = dataSource.UseIntegratedSecurity,
            TrustServerCertificate = true,
            ConnectTimeout = 30
        };

        if (!dataSource.UseIntegratedSecurity)
        {
            builder.UserID = dataSource.UserName;
            
            // 解密密码（如果已加密）
            var password = dataSource.Password;
            if (!string.IsNullOrWhiteSpace(password) && _encryptionService.IsEncrypted(password))
            {
                password = _encryptionService.Decrypt(password);
            }
            builder.Password = password;
        }

        return builder.ConnectionString;
    }
}
