using System;

namespace DbArchiveTool.Infrastructure.Executors;

/// <summary>
/// 在将表转换为分区表的过程中检测到不可恢复的校验错误时抛出。
/// </summary>
internal sealed class PartitionConversionException : Exception
{
    public PartitionConversionException(string message)
        : base(message)
    {
    }
}
