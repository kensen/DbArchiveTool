# Hangfire 归档任务调度集成说明

## 概述

已成功将 Hangfire 集成到归档系统中,用于后台任务调度和定时归档任务管理。**重要**: 此集成采用**平行架构**,不影响现有的分区管理功能。

## 架构说明

### 双轨制设计

```
现有 BackgroundTask 系统 (保持不变)
├── 分区执行 (草稿提交模式)
├── 添加分区边界 (AddBoundary)
├── 拆分分区边界 (SplitBoundary)
├── 合并分区边界 (MergeBoundary)
└── 分区切换归档 (ArchiveSwitch)

Hangfire 归档调度系统 (新增)
├── 即时归档任务
├── 延迟归档任务
├── 定时归档任务
└── 批量归档任务
```

### 关键组件

1. **IArchiveJobService** (`Application/Archives/IArchiveJobService.cs`)
   - 归档任务服务接口
   - 供 Hangfire 调用的标准接口

2. **ArchiveJobService** (`Application/Archives/ArchiveJobService.cs`)
   - 归档任务服务实现
   - 封装 ArchiveOrchestrationService 调用
   - 提供完整的日志记录

3. **ArchiveJobsController** (`Api/Controllers/V1/ArchiveJobsController.cs`)
   - Hangfire 任务管理 API
   - 提供任务入队、调度、定时任务管理等功能

4. **HangfireAuthorizationFilter** (`Api/HangfireAuthorizationFilter.cs`)
   - Dashboard 访问授权过滤器
   - 目前允许所有访问(开发环境)

5. **ArchiveTaskScheduler** (`Infrastructure/Scheduling/ArchiveTaskScheduler.cs`)
    - 归档配置与 Hangfire 周期任务同步器
    - 在配置启用、禁用、删除时自动注册或移除定时任务

6. **CronScheduleHelper** (`Application/Archives/CronScheduleHelper.cs`)
    - 基于 NCrontab 的 Cron 解析工具
    - 创建/更新配置时计算 `NextArchiveAtUtc` 并校验表达式

## 配置详情

### Hangfire 配置 (Program.cs)

```csharp
// 数据存储: 使用 SQL Server,独立 schema "Hangfire"
// 队列: archive(归档任务), default(默认)
// Worker 数量: CPU核心数 × 2
// 服务器名称: {机器名}-archive
```

### Dashboard 访问

- **URL**: `http://localhost:5000/hangfire`
- **功能**: 
  - 查看任务执行状态
  - 监控队列情况
  - 管理定时任务
  - 查看任务历史和统计

### 预配置的定时任务

- **daily-archive-all**: 每天凌晨 2:00 执行所有启用的归档配置

## 归档配置同步策略

- `ArchiveConfigurationsController` 在创建、更新、启用、禁用、删除配置时会调用 `IArchiveTaskScheduler`。
- `ArchiveTaskScheduler` 根据配置状态决定调用 `IRecurringJobManager.AddOrUpdate` 或 `RemoveIfExists`,实现与 Hangfire 的自动对齐。
- `CronScheduleHelper` 基于 NCrontab 校验 Cron 表达式,并计算 `NextArchiveAtUtc` 用于界面展示和后续触发。
- 若配置被禁用或未启用定时归档,调度器会移除对应的周期任务,避免残留的历史计划继续执行。
- 调度失败会记录错误日志,但不会阻断配置的 CRUD 操作,便于后续人工排查。

## API 端点

### 1. 立即执行归档任务

```http
POST /api/v1/archive-jobs/execute/{configurationId}
```

**响应示例:**
```json
{
  "jobId": "12345",
  "configurationId": "guid-here",
  "message": "归档任务已加入后台队列"
}
```

### 2. 批量执行归档任务

```http
POST /api/v1/archive-jobs/execute-batch
Content-Type: application/json

{
  "configurationIds": ["guid1", "guid2", "guid3"]
}
```

### 3. 延迟执行归档任务

```http
POST /api/v1/archive-jobs/schedule/{configurationId}?delayMinutes=30
```

将在 30 分钟后执行指定的归档任务。

### 4. 创建/更新定时任务

```http
POST /api/v1/archive-jobs/recurring
Content-Type: application/json

{
  "jobId": "monthly-archive-orders",
  "configurationId": "guid-here",
  "cronExpression": "0 2 1 * *"  // 每月1号凌晨2点
}
```

**常用 Cron 表达式:**
- `0 2 * * *` - 每天凌晨2点
- `0 */6 * * *` - 每6小时
- `0 2 * * 0` - 每周日凌晨2点
- `0 2 1 * *` - 每月1号凌晨2点

### 5. 删除定时任务

```http
DELETE /api/v1/archive-jobs/recurring/{jobId}
```

### 6. 立即触发定时任务

```http
POST /api/v1/archive-jobs/recurring/{jobId}/trigger
```

## 使用示例

### C# 代码示例

```csharp
// 注入 Hangfire 客户端
public class MyService
{
    private readonly IBackgroundJobClient _backgroundJobClient;
    
    public MyService(IBackgroundJobClient backgroundJobClient)
    {
        _backgroundJobClient = backgroundJobClient;
    }
    
    public void ScheduleArchive(Guid configId)
    {
        // 立即执行
        var jobId = _backgroundJobClient.Enqueue<IArchiveJobService>(
            service => service.ExecuteArchiveJobAsync(configId));
        
        // 延迟5分钟执行
        var delayedJobId = _backgroundJobClient.Schedule<IArchiveJobService>(
            service => service.ExecuteArchiveJobAsync(configId),
            TimeSpan.FromMinutes(5));
    }
}
```

### PowerShell 调用示例

```powershell
# 立即执行归档任务
$configId = "your-config-id-here"
Invoke-RestMethod -Method Post `
    -Uri "http://localhost:5000/api/v1/archive-jobs/execute/$configId"

# 批量执行
$body = @{
    configurationIds = @("guid1", "guid2", "guid3")
} | ConvertTo-Json

Invoke-RestMethod -Method Post `
    -Uri "http://localhost:5000/api/v1/archive-jobs/execute-batch" `
    -ContentType "application/json" `
    -Body $body

# 创建定时任务
$recurringJob = @{
    jobId = "nightly-archive"
    configurationId = "your-config-id"
    cronExpression = "0 2 * * *"
} | ConvertTo-Json

Invoke-RestMethod -Method Post `
    -Uri "http://localhost:5000/api/v1/archive-jobs/recurring" `
    -ContentType "application/json" `
    -Body $recurringJob
```

## 监控和日志

### Dashboard 监控

访问 `/hangfire` 查看:
- **Jobs**: 所有任务列表(成功/失败/处理中)
- **Recurring Jobs**: 定时任务配置
- **Servers**: 服务器状态
- **Retries**: 重试队列
- **Deleted**: 已删除任务

### 日志记录

所有归档任务执行都会记录到应用日志:
```
[Info] Hangfire 归档任务开始: {ConfigId}
[Info] Hangfire 归档任务成功: {ConfigId}, 归档 {Rows} 行, 耗时 {Duration}
[Warning] Hangfire 归档任务失败: {ConfigId}, 原因: {Message}
[Error] Hangfire 归档任务异常: {ConfigId}
```

## 数据库结构

Hangfire 会自动在数据库中创建以下表(schema: Hangfire):
- `Hangfire.Job` - 任务记录
- `Hangfire.State` - 任务状态
- `Hangfire.Server` - 服务器信息
- `Hangfire.Set` - 集合存储
- `Hangfire.Hash` - 哈希存储
- `Hangfire.List` - 列表存储
- `Hangfire.Counter` - 计数器

## 与现有功能的关系

### ✅ 现有功能不受影响

- **分区执行向导** → 继续使用 `BackgroundTask` + `BackgroundTaskProcessor`
- **添加分区边界值** → 继续使用 `BackgroundTaskOperationType.AddBoundary`
- **拆分分区边界** → 继续使用 `BackgroundTaskOperationType.SplitBoundary`
- **合并分区边界** → 继续使用 `BackgroundTaskOperationType.MergeBoundary`
- **分区切换归档** → 继续使用 `BackgroundTaskOperationType.ArchiveSwitch`

### 🆕 新增 Hangfire 功能

- **定时归档** → 使用 Hangfire 定时任务
- **批量归档** → 使用 Hangfire 批处理
- **延迟归档** → 使用 Hangfire 延迟任务
- **任务监控** → 使用 Hangfire Dashboard

### 共享组件

两个系统共享底层的归档执行服务:
- `ArchiveOrchestrationService` - 归档编排服务
- `OptimizedPartitionArchiveExecutor` - 分区归档执行器
- `SqlBulkCopyExecutor` - BulkCopy 执行器
- `BcpExecutor` - BCP 执行器

## 性能考虑

### 并发控制

- **Worker 数量**: 默认为 CPU 核心数 × 2
- **队列策略**: archive 队列优先处理归档任务
- **数据源锁**: 现有 BackgroundTask 系统已实现数据源级别锁

### 资源管理

- **数据库连接**: Hangfire 使用独立连接池
- **内存占用**: 每个 Worker 约 50-100MB
- **磁盘I/O**: BCP/BulkCopy 操作主要受磁盘性能影响

## 最佳实践

### 1. 任务粒度

- ✅ 单个配置单个任务
- ✅ 相关配置组合批处理
- ❌ 避免超大批量(>100个配置)

### 2. 定时任务

- ✅ 选择业务低峰期(如凌晨2-4点)
- ✅ 错峰调度多个任务
- ❌ 避免并发执行同数据源任务

### 3. 错误处理

- Hangfire 自动重试失败任务(默认最多10次)
- 可通过 Dashboard 手动重试失败任务
- 查看详细错误信息和堆栈跟踪

### 4. 监控告警

- 定期检查 Dashboard 中的失败任务
- 监控队列积压情况
- 关注服务器健康状态

## 故障排查

### 问题: Dashboard 无法访问

**解决**: 检查 `HangfireAuthorizationFilter` 配置,确保返回 `true`

### 问题: 任务一直处于 Enqueued 状态

**解决**: 
1. 检查 Hangfire Server 是否运行
2. 查看应用日志中的错误信息
3. 验证数据库连接

### 问题: 定时任务未按时执行

**解决**:
1. 验证 Cron 表达式是否正确
2. 检查服务器时区设置
3. 查看 Dashboard 中的 Recurring Jobs 状态

## 升级和维护

### 数据库迁移

首次启动时,Hangfire 会自动创建所需表结构。后续升级时:
1. 停止应用
2. 备份 Hangfire 相关表
3. 更新 Hangfire 包版本
4. 启动应用(自动迁移)

### 清理历史数据

Hangfire 默认保留成功任务7天,失败任务永久保留。可通过代码配置:

```csharp
GlobalJobFilters.Filters.Add(new AutomaticRetryAttribute { Attempts = 3 });
```

## 总结

Hangfire 集成为归档系统提供了强大的任务调度能力,同时完全不影响现有的分区管理功能。通过双轨制架构设计,系统既保持了原有功能的稳定性,又获得了灵活的定时调度能力。
