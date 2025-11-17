-- ==========================================
-- 归档配置表清理和优化脚本
-- 目的: 区分手动归档配置模板和定时归档任务
-- 执行日期: 2025-11-17
-- ==========================================

USE DbArchiveTool;
GO

-- ==========================================
-- 步骤 1: 查看当前配置数据
-- ==========================================
PRINT '=== 步骤 1: 查看当前所有归档配置 ==='
SELECT 
    Id,
    Name,
    SourceSchemaName,
    SourceTableName,
    TargetSchemaName,
    TargetTableName,
    ArchiveMethod,
    IsEnabled,
    EnableScheduledArchive,
    CronExpression,
    BatchSize,
    DeleteSourceDataAfterArchive,
    LastExecutionTimeUtc,
    LastExecutionStatus,
    CreatedAtUtc,
    CreatedBy
FROM ArchiveConfiguration
WHERE IsDeleted = 0
ORDER BY CreatedAtUtc DESC;

-- ==========================================
-- 步骤 2: 检查字段完整性
-- ==========================================
PRINT '=== 步骤 2: 检查缺少目标表信息的配置 ==='
SELECT 
    Id,
    Name,
    SourceSchemaName + '.' + SourceTableName AS SourceTable,
    ISNULL(TargetSchemaName, '(未设置)') AS TargetSchema,
    ISNULL(TargetTableName, '(未设置)') AS TargetTable,
    ArchiveMethod,
    IsEnabled
FROM ArchiveConfiguration
WHERE IsDeleted = 0
  AND (TargetTableName IS NULL OR TargetTableName = '' OR LEN(TargetTableName) = 0);

-- ==========================================
-- 步骤 3: 禁用所有配置(防止定时任务误触发)
-- ==========================================
PRINT '=== 步骤 3: 禁用所有归档配置(可选) ==='
-- ⚠️ 取消注释下面的代码以禁用所有配置
/*
UPDATE ArchiveConfiguration
SET 
    IsEnabled = 0,
    EnableScheduledArchive = 0, -- 同时禁用定时归档
    UpdatedAtUtc = GETUTCDATE(),
    UpdatedBy = 'System.Cleanup'
WHERE IsDeleted = 0
  AND IsEnabled = 1;

PRINT '已禁用所有归档配置';
*/

-- ==========================================
-- 步骤 4: 补全缺失的目标表信息
-- ==========================================
PRINT '=== 步骤 4: 补全缺失的目标表信息(可选) ==='
-- ⚠️ 取消注释下面的代码以自动补全目标表
-- 规则: 目标表名 = 源表名 + '_Archive'
/*
UPDATE ArchiveConfiguration
SET 
    TargetSchemaName = COALESCE(TargetSchemaName, SourceSchemaName),
    TargetTableName = CASE 
        WHEN TargetTableName IS NULL OR TargetTableName = '' OR LEN(TargetTableName) = 0
        THEN SourceTableName + '_Archive'
        ELSE TargetTableName
    END,
    UpdatedAtUtc = GETUTCDATE(),
    UpdatedBy = 'System.AutoComplete'
WHERE IsDeleted = 0
  AND (TargetTableName IS NULL OR TargetTableName = '' OR LEN(TargetTableName) = 0);

PRINT '已补全目标表信息';

-- 查看补全结果
SELECT 
    Id,
    Name,
    SourceSchemaName + '.' + SourceTableName AS SourceTable,
    TargetSchemaName + '.' + TargetTableName AS TargetTable
FROM ArchiveConfiguration
WHERE IsDeleted = 0
  AND UpdatedBy = 'System.AutoComplete';
*/

-- ==========================================
-- 步骤 5: 将所有配置标记为手动归档类型
-- ==========================================
PRINT '=== 步骤 5: 禁用所有配置的定时归档功能 ==='
-- ⚠️ 取消注释下面的代码以禁用定时归档
/*
UPDATE ArchiveConfiguration
SET 
    EnableScheduledArchive = 0,     -- 禁用定时归档
    CronExpression = NULL,          -- 清除 Cron 表达式
    NextArchiveAtUtc = NULL,        -- 清除下次执行时间
    UpdatedAtUtc = GETUTCDATE(),
    UpdatedBy = 'System.DisableScheduled'
WHERE IsDeleted = 0
  AND EnableScheduledArchive = 1;

PRINT '已禁用所有配置的定时归档功能';
*/

-- ==========================================
-- 步骤 6: 软删除测试数据(可选)
-- ==========================================
PRINT '=== 步骤 6: 软删除测试数据(可选) ==='
-- ⚠️ 取消注释下面的代码以删除特定配置
-- 注意: 这里列出的是日志中出现的 6 个配置
/*
DECLARE @ConfigsToDelete TABLE (Name NVARCHAR(100));
INSERT INTO @ConfigsToDelete VALUES
    ('A1归档'),
    ('FinishingEntryItem归档'),
    ('TaskBadResult归档'),
    ('Tasks归档'),
    ('TaskWmsInfo归档'),
    ('WmsInfo归档');

UPDATE ArchiveConfiguration
SET 
    IsDeleted = 1,
    UpdatedAtUtc = GETUTCDATE(),
    UpdatedBy = 'System.Cleanup'
WHERE IsDeleted = 0
  AND Name IN (SELECT Name FROM @ConfigsToDelete);

PRINT '已软删除指定的测试配置';
*/

-- ==========================================
-- 步骤 7: 物理删除所有配置(谨慎!!!)
-- ==========================================
PRINT '=== 步骤 7: 物理删除所有配置(谨慎!!!) ==='
-- ⚠️ 仅在确认要清空表时取消注释
/*
DELETE FROM ArchiveConfiguration;
PRINT '已物理删除所有归档配置';
*/

-- ==========================================
-- 步骤 8: 验证清理结果
-- ==========================================
PRINT '=== 步骤 8: 验证清理结果 ==='
SELECT 
    COUNT(*) AS TotalConfigs,
    SUM(CASE WHEN IsEnabled = 1 THEN 1 ELSE 0 END) AS EnabledConfigs,
    SUM(CASE WHEN EnableScheduledArchive = 1 THEN 1 ELSE 0 END) AS ScheduledConfigs,
    SUM(CASE WHEN IsDeleted = 1 THEN 1 ELSE 0 END) AS DeletedConfigs
FROM ArchiveConfiguration;

-- ==========================================
-- 清理完成
-- ==========================================
PRINT '===========================================';
PRINT '清理脚本执行完成!';
PRINT '请根据实际需求取消注释相应的步骤并执行。';
PRINT '===========================================';
GO
