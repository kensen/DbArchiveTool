-- 将现有表转换为分区表（重建聚集索引到分区方案）
-- 注意：此操作会锁表，建议在维护窗口执行

-- Step 1: 检查聚集索引是否为主键约束
DECLARE @IsPrimaryKey BIT = 0;
DECLARE @ConstraintName NVARCHAR(256) = NULL;

SELECT @IsPrimaryKey = 1, @ConstraintName = kc.name
FROM sys.indexes i
INNER JOIN sys.key_constraints kc ON i.object_id = kc.parent_object_id 
    AND i.index_id = kc.unique_index_id
    AND kc.type = 'PK'
WHERE i.object_id = OBJECT_ID('{SchemaName}.{TableName}')
  AND i.index_id = 1;

-- Step 2: 如果是主键约束，先删除约束
IF @IsPrimaryKey = 1 AND @ConstraintName IS NOT NULL
BEGIN
    DECLARE @DropPKSql NVARCHAR(MAX) = 
        N'ALTER TABLE [{SchemaName}].[{TableName}] DROP CONSTRAINT [' + @ConstraintName + '];';
    EXEC sp_executesql @DropPKSql;
END
-- 如果是普通聚集索引，直接删除
ELSE IF EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE object_id = OBJECT_ID('{SchemaName}.{TableName}') 
    AND index_id = 1
)
BEGIN
    DROP INDEX [{ClusteredIndexName}] ON [{SchemaName}].[{TableName}];
END

-- Step 3: 在分区方案上重建聚集索引
CREATE CLUSTERED INDEX [{ClusteredIndexName}]
ON [{SchemaName}].[{TableName}] ([{PartitionColumn}])
ON [{PartitionScheme}]([{PartitionColumn}]);

