-- =====================================================
-- 检查并清理启用的定时归档任务
-- 用途: 排查 API 启动时自动注册 8 个任务的问题
-- =====================================================

-- 步骤 1: 检查 ScheduledArchiveJob 表中启用的任务
PRINT '=== 步骤 1: 检查 ScheduledArchiveJob 表中启用的任务 ==='
SELECT 
    Id,
    Name,
    DataSourceId,
    IsEnabled,
    IntervalMinutes,
    CronExpression,
    LastExecutionAtUtc,
    NextExecutionAtUtc,
    CreatedAtUtc,
    CreatedBy
FROM ScheduledArchiveJob
WHERE IsEnabled = 1 AND IsDeleted = 0
ORDER BY CreatedAtUtc DESC;

PRINT '找到的启用任务数量:'
SELECT COUNT(*) AS EnabledJobCount
FROM ScheduledArchiveJob
WHERE IsEnabled = 1 AND IsDeleted = 0;

-- 步骤 2: 检查 ArchiveConfiguration 表（旧表，应该不再用于定时任务）
PRINT ''
PRINT '=== 步骤 2: 检查 ArchiveConfiguration 表（旧配置） ==='
SELECT 
    Id,
    Name,
    DataSourceId,
    SourceSchemaName + '.' + SourceTableName AS SourceTable,
    ISNULL(TargetSchemaName, 'dbo') + '.' + ISNULL(TargetTableName, SourceTableName + '_Archive') AS TargetTable,
    CreatedAtUtc,
    CreatedBy
FROM ArchiveConfiguration
WHERE IsDeleted = 0
ORDER BY CreatedAtUtc DESC;

PRINT 'ArchiveConfiguration 表记录数:'
SELECT COUNT(*) AS ConfigCount
FROM ArchiveConfiguration
WHERE IsDeleted = 0;

-- 步骤 3: 检查 Hangfire 中注册的周期任务
PRINT ''
PRINT '=== 步骤 3: 检查 Hangfire 中的周期任务 ==='
SELECT 
    [Key] AS JobId,
    CAST([Value] AS NVARCHAR(MAX)) AS JobDetails,
    Score AS NextExecutionTicks
FROM Hangfire.[Set]
WHERE [Key] LIKE 'recurring-job:scheduled-archive-job-%'
ORDER BY Score;

PRINT 'Hangfire 周期任务数量:'
SELECT COUNT(*) AS HangfireJobCount
FROM Hangfire.[Set]
WHERE [Key] LIKE 'recurring-job:scheduled-archive-job-%';

-- =====================================================
-- 修复选项（根据需要执行）
-- =====================================================

-- 选项 1: 禁用所有 ScheduledArchiveJob 中的任务（推荐）
/*
PRINT ''
PRINT '=== 选项 1: 禁用所有 ScheduledArchiveJob 任务 ==='

UPDATE ScheduledArchiveJob
SET 
    IsEnabled = 0,
    NextExecutionAtUtc = NULL,
    UpdatedAtUtc = GETUTCDATE(),
    UpdatedBy = 'SYSTEM.Cleanup'
WHERE IsEnabled = 1 AND IsDeleted = 0;

PRINT '已禁用的任务数量:'
SELECT @@ROWCOUNT AS DisabledCount;
*/

-- 选项 2: 删除所有 ScheduledArchiveJob 中的任务（慎用）
/*
PRINT ''
PRINT '=== 选项 2: 软删除所有 ScheduledArchiveJob 任务 ==='

UPDATE ScheduledArchiveJob
SET 
    IsDeleted = 1,
    DeletedAtUtc = GETUTCDATE(),
    DeletedBy = 'SYSTEM.Cleanup'
WHERE IsDeleted = 0;

PRINT '已删除的任务数量:'
SELECT @@ROWCOUNT AS DeletedCount;
*/

-- 选项 3: 从 Hangfire 中移除所有定时归档任务
/*
PRINT ''
PRINT '=== 选项 3: 从 Hangfire 移除所有定时归档任务 ==='

DELETE FROM Hangfire.[Set]
WHERE [Key] LIKE 'recurring-job:scheduled-archive-job-%';

DELETE FROM Hangfire.[Hash]
WHERE [Key] LIKE 'recurring-job:scheduled-archive-job-%';

PRINT '已从 Hangfire 移除的任务数量:'
SELECT @@ROWCOUNT AS RemovedCount;
*/

-- =====================================================
-- 验证清理结果
-- =====================================================
/*
PRINT ''
PRINT '=== 验证清理结果 ==='

PRINT 'ScheduledArchiveJob 启用任务:'
SELECT COUNT(*) AS EnabledCount
FROM ScheduledArchiveJob
WHERE IsEnabled = 1 AND IsDeleted = 0;

PRINT 'Hangfire 周期任务:'
SELECT COUNT(*) AS HangfireJobCount
FROM Hangfire.[Set]
WHERE [Key] LIKE 'recurring-job:scheduled-archive-job-%';
*/

PRINT ''
PRINT '=== 检查完成 ==='
PRINT '如果需要清理，请取消注释相应的选项并重新执行'
