namespace DbArchiveTool.Shared.Archive;

/// <summary>
/// 归档方法枚举
/// </summary>
public enum ArchiveMethod
{
    /// <summary>
    /// 使用分区 SWITCH 进行归档 (仅适用于分区表)
    /// </summary>
    PartitionSwitch = 0,

    /// <summary>
    /// 使用 BCP 工具进行归档 (导出到文件,再导入到目标)
    /// </summary>
    Bcp = 1,

    /// <summary>
    /// 使用 SqlBulkCopy 进行归档 (流式传输,无中间文件)
    /// </summary>
    BulkCopy = 2
}
