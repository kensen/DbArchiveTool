using System.ComponentModel.DataAnnotations;
using DbArchiveTool.Application.Partitions;

namespace DbArchiveTool.Api.Models;

public sealed class BcpArchivePlanDto
{
    [Required]
    public Guid PartitionConfigurationId { get; set; }

    [Required]
    public string RequestedBy { get; set; } = string.Empty;

    public string? TargetConnectionString { get; set; }

    public string? TargetDatabase { get; set; }

    public string? TargetTable { get; set; }

    public BcpArchivePlanRequest ToApplicationRequest() =>
        new(PartitionConfigurationId, RequestedBy, TargetConnectionString, TargetDatabase, TargetTable);
}

public sealed class BulkCopyArchivePlanDto
{
    [Required]
    public Guid PartitionConfigurationId { get; set; }

    [Required]
    public string RequestedBy { get; set; } = string.Empty;

    public string? TargetConnectionString { get; set; }

    public string? TargetDatabase { get; set; }

    public string? TargetTable { get; set; }

    public BulkCopyArchivePlanRequest ToApplicationRequest() =>
        new(PartitionConfigurationId, RequestedBy, TargetConnectionString, TargetDatabase, TargetTable);
}
