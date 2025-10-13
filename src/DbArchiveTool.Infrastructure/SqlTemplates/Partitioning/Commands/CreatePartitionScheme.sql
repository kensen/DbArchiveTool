-- 创建分区方案
-- 如果分区方案已存在，此脚本将会失败，需要先检查
CREATE PARTITION SCHEME [{PartitionScheme}]
AS PARTITION [{PartitionFunction}]
{FilegroupMapping};
