-- 查询表的所有索引定义（用于分区转换时保存和重建）
-- 兼容 SQL Server 2012+，使用 XML PATH 方法聚合列名（替代 STRING_AGG）
SELECT 
    i.index_id AS IndexId,
    i.name AS IndexName,
    i.type_desc AS IndexType,
    i.is_unique AS IsUnique,
    i.is_primary_key AS IsPrimaryKey,
    i.is_unique_constraint AS IsUniqueConstraint,
    kc.name AS ConstraintName,
    kc.type_desc AS ConstraintType,
    -- 索引键列（带排序方向）- 使用 XML PATH 聚合
    STUFF((
        SELECT ', ' + QUOTENAME(c.name) + CASE WHEN ic.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
        FROM sys.index_columns ic
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE ic.object_id = i.object_id 
          AND ic.index_id = i.index_id 
          AND ic.is_included_column = 0
        ORDER BY ic.key_ordinal
        FOR XML PATH(''), TYPE
    ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS KeyColumns,
    -- INCLUDE 列 - 使用 XML PATH 聚合
    STUFF((
        SELECT ', ' + QUOTENAME(c.name)
        FROM sys.index_columns ic
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE ic.object_id = i.object_id 
          AND ic.index_id = i.index_id 
          AND ic.is_included_column = 1
        FOR XML PATH(''), TYPE
    ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS IncludedColumns,
    -- 筛选条件
    i.filter_definition AS FilterDefinition,
    -- 是否包含分区列（用于判断是否对齐到分区方案）
    CASE WHEN EXISTS (
        SELECT 1 FROM sys.index_columns ic
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE ic.object_id = i.object_id 
          AND ic.index_id = i.index_id 
          AND c.name = @PartitionColumn
          AND ic.is_included_column = 0  -- 只检查键列，不包括 INCLUDE 列
    ) THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS ContainsPartitionColumn
FROM sys.indexes i
INNER JOIN sys.tables t ON i.object_id = t.object_id
LEFT JOIN sys.key_constraints kc ON i.object_id = kc.parent_object_id 
    AND i.index_id = kc.unique_index_id
WHERE SCHEMA_NAME(t.schema_id) = @SchemaName
  AND t.name = @TableName
  AND i.type IN (1, 2) -- 1=聚集索引, 2=非聚集索引（排除堆和列存储）
ORDER BY 
    -- 排序：主键优先，然后是聚集索引，最后是非聚集索引
    CASE WHEN i.is_primary_key = 1 THEN 1 
         WHEN i.type_desc = 'CLUSTERED' THEN 2
         WHEN i.is_unique = 1 THEN 3 
         ELSE 4 END,
    i.index_id;
