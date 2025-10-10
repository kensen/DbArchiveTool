-- ========================================
-- 清理重复的分区配置记录
-- ========================================
-- 说明: 由于唯一索引更新,可能存在已删除但仍阻塞新插入的记录
-- 执行前请先备份数据库!

USE [DbArchiveTool];
GO

-- 1. 查看所有分区配置(包括已删除的)
SELECT 
    Id,
    ArchiveDataSourceId,
    SchemaName,
    TableName,
    IsDeleted,
    CreatedAtUtc,
    CreatedBy
FROM PartitionConfiguration
ORDER BY SchemaName, TableName, IsDeleted, CreatedAtUtc DESC;

-- 2. 查找重复的配置(同一数据源+架构+表名有多条记录)
SELECT 
    ArchiveDataSourceId,
    SchemaName,
    TableName,
    COUNT(*) AS TotalRecords,
    SUM(CASE WHEN IsDeleted = 0 THEN 1 ELSE 0 END) AS ActiveRecords,
    SUM(CASE WHEN IsDeleted = 1 THEN 1 ELSE 0 END) AS DeletedRecords
FROM PartitionConfiguration
GROUP BY ArchiveDataSourceId, SchemaName, TableName
HAVING COUNT(*) > 1
ORDER BY SchemaName, TableName;

-- 3. 物理删除所有已标记删除的记录
-- ⚠️ 警告: 这会永久删除数据,执行前请确认!

BEGIN TRANSACTION;

-- 删除已标记删除的配置的所有关联数据
DELETE FROM PartitionConfigurationFilegroupMapping 
WHERE ConfigurationId IN (SELECT Id FROM PartitionConfiguration WHERE IsDeleted = 1);

DELETE FROM PartitionConfigurationFilegroup 
WHERE ConfigurationId IN (SELECT Id FROM PartitionConfiguration WHERE IsDeleted = 1);

DELETE FROM PartitionConfigurationBoundary 
WHERE ConfigurationId IN (SELECT Id FROM PartitionConfiguration WHERE IsDeleted = 1);

-- 删除已标记删除的配置本身
DELETE FROM PartitionConfiguration WHERE IsDeleted = 1;

-- 检查结果
SELECT 'Cleanup completed. Remaining records:' AS Status, COUNT(*) AS RecordCount
FROM PartitionConfiguration;

-- 如果确认无误,提交事务
COMMIT;
-- 如果有问题,回滚事务
-- ROLLBACK;

-- 4. 验证唯一索引是否正确创建
SELECT 
    i.name AS IndexName,
    i.is_unique AS IsUnique,
    i.filter_definition AS FilterDefinition,
    STRING_AGG(c.name, ', ') AS IndexColumns
FROM sys.indexes i
INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE i.object_id = OBJECT_ID('PartitionConfiguration')
    AND i.name = 'IX_PartitionConfiguration_ArchiveDataSourceId_SchemaName_TableName'
GROUP BY i.name, i.is_unique, i.filter_definition;
