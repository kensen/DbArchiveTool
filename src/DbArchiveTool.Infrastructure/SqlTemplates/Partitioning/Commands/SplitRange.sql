-- 为单个分区边界拆分（不包含事务，由外层 C# 代码管理）
ALTER PARTITION SCHEME [{PartitionScheme}] NEXT USED [{FilegroupName}];
ALTER PARTITION FUNCTION [{PartitionFunction}]() SPLIT RANGE ({BoundaryLiteral});
