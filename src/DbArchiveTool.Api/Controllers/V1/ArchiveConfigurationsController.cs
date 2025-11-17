using DbArchiveTool.Api.DTOs.Archives;
using DbArchiveTool.Application.Archives;
using DbArchiveTool.Domain.ArchiveConfigurations;
using Microsoft.AspNetCore.Mvc;

namespace DbArchiveTool.Api.Controllers.V1;

/// <summary>
/// 归档配置 API 控制器
/// </summary>
[ApiController]
[Route("api/v1/archive-configurations")]
[Produces("application/json")]
public sealed class ArchiveConfigurationsController : ControllerBase
{
    private readonly IArchiveConfigurationRepository _repository;
    private readonly ILogger<ArchiveConfigurationsController> _logger;

    public ArchiveConfigurationsController(
        IArchiveConfigurationRepository repository,
        ILogger<ArchiveConfigurationsController> logger)
    {
        _repository = repository;
        _logger = logger;
    }

    /// <summary>
    /// 获取所有归档配置列表
    /// </summary>
    /// <param name="dataSourceId">数据源ID(可选)</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>归档配置列表</returns>
    [HttpGet]
    [ProducesResponseType(typeof(IEnumerable<ArchiveConfigurationListItemDto>), 200)]
    public async Task<IActionResult> GetAll(
        [FromQuery] Guid? dataSourceId = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var configs = await _repository.GetAllAsync(cancellationToken);

            // 过滤
            if (dataSourceId.HasValue)
            {
                configs = configs.Where(c => c.DataSourceId == dataSourceId.Value).ToList();
            }

            var dtos = configs.Select(c => new ArchiveConfigurationListItemDto
            {
                Id = c.Id,
                Name = c.Name,
                Description = c.Description,
                DataSourceId = c.DataSourceId,
                SourceSchemaName = c.SourceSchemaName,
                SourceTableName = c.SourceTableName,
                TargetSchemaName = c.TargetSchemaName,
                TargetTableName = c.TargetTableName,
                IsPartitionedTable = c.IsPartitionedTable,
                ArchiveMethod = c.ArchiveMethod,
                CreatedAtUtc = c.CreatedAtUtc,
                UpdatedAtUtc = c.UpdatedAtUtc
            });

            return Ok(dtos);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "获取归档配置列表失败");
            return StatusCode(500, new { message = "获取归档配置列表失败", error = ex.Message });
        }
    }

    /// <summary>
    /// 根据ID获取归档配置详情
    /// </summary>
    /// <param name="id">配置ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>归档配置详情</returns>
    [HttpGet("{id}")]
    [ProducesResponseType(typeof(ArchiveConfigurationDetailDto), 200)]
    [ProducesResponseType(404)]
    public async Task<IActionResult> GetById(Guid id, CancellationToken cancellationToken = default)
    {
        try
        {
            var config = await _repository.GetByIdAsync(id, cancellationToken);
            if (config == null)
            {
                return NotFound(new { message = $"归档配置不存在: {id}" });
            }

            var dto = new ArchiveConfigurationDetailDto
            {
                Id = config.Id,
                Name = config.Name,
                Description = config.Description,
                DataSourceId = config.DataSourceId,
                SourceSchemaName = config.SourceSchemaName,
                SourceTableName = config.SourceTableName,
                TargetSchemaName = config.TargetSchemaName,
                TargetTableName = config.TargetTableName,
                IsPartitionedTable = config.IsPartitionedTable,
                PartitionConfigurationId = config.PartitionConfigurationId,
                ArchiveFilterColumn = config.ArchiveFilterColumn,
                ArchiveFilterCondition = config.ArchiveFilterCondition,
                ArchiveMethod = config.ArchiveMethod,
                DeleteSourceDataAfterArchive = config.DeleteSourceDataAfterArchive,
                BatchSize = config.BatchSize,
                CreatedAtUtc = config.CreatedAtUtc,
                CreatedBy = config.CreatedBy,
                UpdatedAtUtc = config.UpdatedAtUtc,
                UpdatedBy = config.UpdatedBy
            };

            return Ok(dto);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "获取归档配置详情失败: {Id}", id);
            return StatusCode(500, new { message = "获取归档配置详情失败", error = ex.Message });
        }
    }

    /// <summary>
    /// 创建归档配置
    /// </summary>
    /// <param name="request">创建请求</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>创建的归档配置</returns>
    [HttpPost]
    [ProducesResponseType(typeof(ArchiveConfigurationDetailDto), 201)]
    [ProducesResponseType(400)]
    public async Task<IActionResult> Create(
        [FromBody] CreateArchiveConfigurationRequest request,
        CancellationToken cancellationToken = default)
    {
        try
        {
            // 验证
            if (string.IsNullOrWhiteSpace(request.Name))
            {
                return BadRequest(new { message = "配置名称不能为空" });
            }

            if (request.DataSourceId == Guid.Empty)
            {
                return BadRequest(new { message = "数据源ID不能为空" });
            }

            if (string.IsNullOrWhiteSpace(request.SourceTableName))
            {
                return BadRequest(new { message = "源表名称不能为空" });
            }

            // 创建实体
            var config = new ArchiveConfiguration(
                request.Name,
                request.Description,
                request.DataSourceId,
                request.SourceSchemaName,
                request.SourceTableName,
                request.IsPartitionedTable,
                request.ArchiveFilterColumn,
                request.ArchiveFilterCondition,
                request.ArchiveMethod,
                request.DeleteSourceDataAfterArchive,
                request.BatchSize,
                request.PartitionConfigurationId,
                request.TargetSchemaName,
                request.TargetTableName);

            // 保存
            await _repository.AddAsync(config, cancellationToken);

            _logger.LogInformation("创建归档配置成功: {Id} - {Name}", config.Id, config.Name);

            // 返回创建的资源
            var dto = new ArchiveConfigurationDetailDto
            {
                Id = config.Id,
                Name = config.Name,
                Description = config.Description,
                DataSourceId = config.DataSourceId,
                SourceSchemaName = config.SourceSchemaName,
                SourceTableName = config.SourceTableName,
                TargetSchemaName = config.TargetSchemaName,
                TargetTableName = config.TargetTableName,
                IsPartitionedTable = config.IsPartitionedTable,
                PartitionConfigurationId = config.PartitionConfigurationId,
                ArchiveFilterColumn = config.ArchiveFilterColumn,
                ArchiveFilterCondition = config.ArchiveFilterCondition,
                ArchiveMethod = config.ArchiveMethod,
                DeleteSourceDataAfterArchive = config.DeleteSourceDataAfterArchive,
                BatchSize = config.BatchSize,
                CreatedAtUtc = config.CreatedAtUtc,
                CreatedBy = config.CreatedBy,
                UpdatedAtUtc = config.UpdatedAtUtc,
                UpdatedBy = config.UpdatedBy
            };

            return CreatedAtAction(nameof(GetById), new { id = config.Id }, dto);
        }
        catch (ArgumentException ex)
        {
            _logger.LogWarning(ex, "创建归档配置失败: 参数验证错误");
            return BadRequest(new { message = ex.Message });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "创建归档配置失败");
            return StatusCode(500, new { message = "创建归档配置失败", error = ex.Message });
        }
    }

    /// <summary>
    /// 更新归档配置
    /// </summary>
    /// <param name="id">配置ID</param>
    /// <param name="request">更新请求</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>更新的归档配置</returns>
    [HttpPut("{id}")]
    [ProducesResponseType(typeof(ArchiveConfigurationDetailDto), 200)]
    [ProducesResponseType(400)]
    [ProducesResponseType(404)]
    public async Task<IActionResult> Update(
        Guid id,
        [FromBody] UpdateArchiveConfigurationRequest request,
        CancellationToken cancellationToken = default)
    {
        try
        {
            // 验证
            if (string.IsNullOrWhiteSpace(request.Name))
            {
                return BadRequest(new { message = "配置名称不能为空" });
            }

            // 获取现有配置
            var config = await _repository.GetByIdAsync(id, cancellationToken);
            if (config == null)
            {
                return NotFound(new { message = $"归档配置不存在: {id}" });
            }

            // 更新
            config.Update(
                request.Name,
                request.Description,
                request.DataSourceId,
                request.SourceSchemaName,
                request.SourceTableName,
                request.IsPartitionedTable,
                request.ArchiveFilterColumn,
                request.ArchiveFilterCondition,
                request.ArchiveMethod,
                request.DeleteSourceDataAfterArchive,
                request.BatchSize,
                request.PartitionConfigurationId,
                request.TargetSchemaName,
                request.TargetTableName,
                "API");

            await _repository.UpdateAsync(config, cancellationToken);

            _logger.LogInformation("更新归档配置成功: {Id} - {Name}", config.Id, config.Name);

            // 返回更新后的资源
            var dto = new ArchiveConfigurationDetailDto
            {
                Id = config.Id,
                Name = config.Name,
                Description = config.Description,
                DataSourceId = config.DataSourceId,
                SourceSchemaName = config.SourceSchemaName,
                SourceTableName = config.SourceTableName,
                TargetSchemaName = config.TargetSchemaName,
                TargetTableName = config.TargetTableName,
                IsPartitionedTable = config.IsPartitionedTable,
                PartitionConfigurationId = config.PartitionConfigurationId,
                ArchiveFilterColumn = config.ArchiveFilterColumn,
                ArchiveFilterCondition = config.ArchiveFilterCondition,
                ArchiveMethod = config.ArchiveMethod,
                DeleteSourceDataAfterArchive = config.DeleteSourceDataAfterArchive,
                BatchSize = config.BatchSize,
                CreatedAtUtc = config.CreatedAtUtc,
                CreatedBy = config.CreatedBy,
                UpdatedAtUtc = config.UpdatedAtUtc,
                UpdatedBy = config.UpdatedBy
            };

            return Ok(dto);
        }
        catch (ArgumentException ex)
        {
            _logger.LogWarning(ex, "更新归档配置失败: 参数验证错误");
            return BadRequest(new { message = ex.Message });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "更新归档配置失败: {Id}", id);
            return StatusCode(500, new { message = "更新归档配置失败", error = ex.Message });
        }
    }

    /// <summary>
    /// 删除归档配置(软删除)
    /// </summary>
    /// <param name="id">配置ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>删除结果</returns>
    [HttpDelete("{id}")]
    [ProducesResponseType(204)]
    [ProducesResponseType(404)]
    public async Task<IActionResult> Delete(Guid id, CancellationToken cancellationToken = default)
    {
        try
        {
            var config = await _repository.GetByIdAsync(id, cancellationToken);
            if (config == null)
            {
                return NotFound(new { message = $"归档配置不存在: {id}" });
            }

            config.MarkDeleted("API");
            await _repository.UpdateAsync(config, cancellationToken);

            _logger.LogInformation("删除归档配置成功: {Id} - {Name}", config.Id, config.Name);

            return NoContent();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "删除归档配置失败: {Id}", id);
            return StatusCode(500, new { message = "删除归档配置失败", error = ex.Message });
        }
    }
}
