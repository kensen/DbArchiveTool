using System.Threading;
using System.Threading.Tasks;

namespace DbArchiveTool.Domain.Partitions;

/// <summary>
/// 定义分区审计日志的持久化行为。
/// </summary>
public interface IPartitionAuditLogRepository
{
    Task AddAsync(PartitionAuditLog log, CancellationToken cancellationToken = default);
}
