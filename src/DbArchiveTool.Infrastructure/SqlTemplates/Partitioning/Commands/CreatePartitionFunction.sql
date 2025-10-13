-- 创建分区函数
-- 如果分区函数已存在，此脚本将会失败，需要先检查
CREATE PARTITION FUNCTION [{PartitionFunction}] ({DataType})
AS RANGE {RangeType} FOR VALUES ({InitialBoundaries});
