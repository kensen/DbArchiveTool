namespace DbArchiveTool.Web.Pages;

public class PartitionTableInfo
{
    public string SchemaName { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
    public string PartitionFunction { get; set; } = string.Empty;
    public string PartitionScheme { get; set; } = string.Empty;
    public string PartitionColumn { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;
    public int TotalPartitions { get; set; }
    public bool IsRangeRight { get; set; }
}
