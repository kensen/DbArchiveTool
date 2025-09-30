using DbArchiveTool.Application.ArchiveTasks;
using DbArchiveTool.Shared.Results;
using Microsoft.AspNetCore.Mvc;

namespace DbArchiveTool.Api.Controllers;

[ApiController]
[Route("api/v1/archive-tasks")]
public sealed class ArchiveTasksController : ControllerBase
{
    private readonly IArchiveTaskQueryService _queryService;
    private readonly IArchiveTaskCommandService _commandService;

    public ArchiveTasksController(IArchiveTaskQueryService queryService, IArchiveTaskCommandService commandService)
    {
        _queryService = queryService;
        _commandService = commandService;
    }

    [HttpGet]
    [ProducesResponseType(typeof(PagedResult<ArchiveTaskDto>), StatusCodes.Status200OK)]
    public async Task<IActionResult> GetAsync([FromQuery] int page = 1, [FromQuery] int pageSize = 20, CancellationToken cancellationToken = default)
    {
        var result = await _queryService.GetTasksAsync(page, pageSize, cancellationToken);
        return Ok(result);
    }

    [HttpPost]
    [ProducesResponseType(typeof(Guid), StatusCodes.Status201Created)]
    public async Task<IActionResult> PostAsync([FromBody] CreateArchiveTaskRequest request, CancellationToken cancellationToken)
    {
        var result = await _commandService.EnqueueArchiveTaskAsync(request, cancellationToken);

        if (!result.IsSuccess)
        {
            return Problem(title: "Failed to create archive task", detail: result.Error, statusCode: StatusCodes.Status400BadRequest);
        }

        return Created($"api/v1/archive-tasks/{result.Value}", result.Value);
    }
}
