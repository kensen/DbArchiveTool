-- =====================================================
-- 清理 Hangfire 中的废弃定时任务
-- 问题: API 启动后 Hangfire 自动执行废弃的周期任务
-- =====================================================

-- 步骤 1: 检查所有 Hangfire 周期任务
PRINT '=== 步骤 1: 检查 Hangfire 中的所有周期任务 ==='
SELECT 
    [Key] AS RecurringJobId,
    CAST([Value] AS NVARCHAR(MAX)) AS JobDetails
FROM Hangfire.[Set]
WHERE [Key] LIKE 'recurring-job:%'
ORDER BY [Key];

PRINT '找到的周期任务数量:'
SELECT COUNT(*) AS RecurringJobCount
FROM Hangfire.[Set]
WHERE [Key] LIKE 'recurring-job:%';

-- 步骤 2: 检查周期任务的 Hash 数据
PRINT ''
PRINT '=== 步骤 2: 检查周期任务的 Hash 配置 ==='
SELECT 
    [Key] AS RecurringJobId,
    Field,
    CAST([Value] AS NVARCHAR(MAX)) AS FieldValue
FROM Hangfire.[Hash]
WHERE [Key] LIKE 'recurring-job:%'
ORDER BY [Key], Field;

-- =====================================================
-- 修复选项: 删除废弃的周期任务
-- =====================================================

-- 选项 1: 删除 daily-archive-all 任务(推荐)
PRINT ''
PRINT '=== 选项 1: 删除 daily-archive-all 周期任务 ==='

-- 从 Set 表删除
DECLARE @SetDeleted INT = 0;
DELETE FROM Hangfire.[Set]
WHERE [Key] = 'recurring-job:daily-archive-all';
SET @SetDeleted = @@ROWCOUNT;

-- 从 Hash 表删除
DECLARE @HashDeleted INT = 0;
DELETE FROM Hangfire.[Hash]
WHERE [Key] = 'recurring-job:daily-archive-all';
SET @HashDeleted = @@ROWCOUNT;

PRINT '从 Set 表删除记录数: ' + CAST(@SetDeleted AS NVARCHAR(10));
PRINT '从 Hash 表删除记录数: ' + CAST(@HashDeleted AS NVARCHAR(10));
PRINT '总共删除记录数: ' + CAST(@SetDeleted + @HashDeleted AS NVARCHAR(10));

-- 选项 2: 删除所有归档相关的周期任务
/*
PRINT ''
PRINT '=== 选项 2: 删除所有归档相关的周期任务 ==='

-- 从 Set 表删除
DELETE FROM Hangfire.[Set]
WHERE [Key] LIKE 'recurring-job:%archive%'
   OR [Key] LIKE 'recurring-job:scheduled-archive-job-%';

-- 从 Hash 表删除
DELETE FROM Hangfire.[Hash]
WHERE [Key] LIKE 'recurring-job:%archive%'
   OR [Key] LIKE 'recurring-job:scheduled-archive-job-%';

PRINT '已删除的记录数:'
SELECT @@ROWCOUNT AS DeletedCount;
*/

-- =====================================================
-- 验证清理结果
-- =====================================================
PRINT ''
PRINT '=== 验证清理结果 ==='

PRINT 'Hangfire 周期任务:'
SELECT COUNT(*) AS RecurringJobCount
FROM Hangfire.[Set]
WHERE [Key] LIKE 'recurring-job:%';

PRINT '剩余的周期任务列表:'
SELECT 
    [Key] AS RecurringJobId
FROM Hangfire.[Set]
WHERE [Key] LIKE 'recurring-job:%'
ORDER BY [Key];

PRINT ''
PRINT '=== 清理完成 ==='
PRINT '请重启 API 验证问题是否解决'
