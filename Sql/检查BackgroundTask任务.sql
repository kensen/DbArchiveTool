-- =====================================================
-- 检查 BackgroundTask 后台任务状态
-- 用途: 排查 API 启动时可能恢复的后台任务
-- =====================================================

-- 检查所有未完成的 BackgroundTask
PRINT '=== 检查未完成的 BackgroundTask 任务 ==='
SELECT 
    Id,
    DataSourceId,
    OperationType,
    Status,
    RequestedBy,
    CreatedAtUtc,
    StartedAtUtc,
    LastHeartbeatUtc,
    CompletedAtUtc,
    ErrorMessage
FROM BackgroundTask
WHERE IsDeleted = 0
  AND Status IN (0, 1) -- 0=Pending, 1=Running
ORDER BY CreatedAtUtc DESC;

PRINT '未完成任务数量:'
SELECT 
    Status,
    CASE Status
        WHEN 0 THEN 'Pending'
        WHEN 1 THEN 'Running'
        WHEN 2 THEN 'Completed'
        WHEN 3 THEN 'Failed'
        WHEN 4 THEN 'Cancelled'
    END AS StatusName,
    COUNT(*) AS TaskCount
FROM BackgroundTask
WHERE IsDeleted = 0
GROUP BY Status
ORDER BY Status;

PRINT ''
PRINT '=== 检查所有 BackgroundTask 任务 ==='
SELECT 
    Id,
    DataSourceId,
    OperationType,
    Status,
    CASE Status
        WHEN 0 THEN 'Pending'
        WHEN 1 THEN 'Running'
        WHEN 2 THEN 'Completed'
        WHEN 3 THEN 'Failed'
        WHEN 4 THEN 'Cancelled'
    END AS StatusName,
    RequestedBy,
    CreatedAtUtc,
    StartedAtUtc,
    CompletedAtUtc
FROM BackgroundTask
WHERE IsDeleted = 0
ORDER BY CreatedAtUtc DESC;

PRINT '总任务数量:'
SELECT COUNT(*) AS TotalTaskCount
FROM BackgroundTask
WHERE IsDeleted = 0;

PRINT ''
PRINT '=== 检查可能被识别为僵尸任务的记录 ==='
-- 僵尸任务: 状态为 Running 但最后心跳时间超过 5 分钟
SELECT 
    Id,
    DataSourceId,
    OperationType,
    Status,
    RequestedBy,
    StartedAtUtc,
    LastHeartbeatUtc,
    DATEDIFF(MINUTE, LastHeartbeatUtc, GETUTCDATE()) AS MinutesSinceLastHeartbeat,
    ErrorMessage
FROM BackgroundTask
WHERE IsDeleted = 0
  AND Status = 1 -- Running
  AND LastHeartbeatUtc IS NOT NULL
  AND DATEDIFF(MINUTE, LastHeartbeatUtc, GETUTCDATE()) > 5
ORDER BY LastHeartbeatUtc DESC;

PRINT '僵尸任务数量:'
SELECT COUNT(*) AS ZombieTaskCount
FROM BackgroundTask
WHERE IsDeleted = 0
  AND Status = 1 -- Running
  AND LastHeartbeatUtc IS NOT NULL
  AND DATEDIFF(MINUTE, LastHeartbeatUtc, GETUTCDATE()) > 5;

PRINT ''
PRINT '=== 按操作类型统计 ==='
SELECT 
    OperationType,
    Status,
    CASE Status
        WHEN 0 THEN 'Pending'
        WHEN 1 THEN 'Running'
        WHEN 2 THEN 'Completed'
        WHEN 3 THEN 'Failed'
        WHEN 4 THEN 'Cancelled'
    END AS StatusName,
    COUNT(*) AS TaskCount
FROM BackgroundTask
WHERE IsDeleted = 0
GROUP BY OperationType, Status
ORDER BY OperationType, Status;

PRINT ''
PRINT '=== 检查完成 ==='
