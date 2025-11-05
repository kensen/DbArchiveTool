# 自定义 Hangfire 任务监控页面

## 概述

为了保持 UI 风格的统一性和定制化需求,我们创建了自定义的 Hangfire 任务监控页面,替代默认的 Hangfire Dashboard。

## 优势

1. **统一的 UI 风格**:使用 Ant Design Blazor 组件,与现有系统界面风格完全一致
2. **业务集成**:可以与归档配置、数据源等业务数据无缝集成
3. **简化的访问控制**:使用现有的认证授权机制,无需单独配置
4. **定制化展示**:只显示归档任务相关的信息,更加聚焦业务需求
5. **中文界面**:完全本地化的中文界面,无需额外配置

## 实现架构

### 1. 服务层

#### IHangfireMonitorService
定义监控服务接口,提供以下功能:
- 获取任务列表(支持状态筛选和分页)
- 获取任务详情
- 获取定时任务列表
- 删除任务
- 重新入队失败任务
- 触发定时任务
- 移除定时任务
- 获取统计信息

#### HangfireMonitorService
实现监控服务接口,通过 Hangfire 的 `IMonitoringApi` 访问任务数据:

```csharp
// 获取监控 API
private IMonitoringApi GetMonitoringApi()
{
    return JobStorage.Current.GetMonitoringApi();
}

// 获取任务列表
public async Task<PagedResult<HangfireJobModel>> GetJobsAsync(
    string? status = null, 
    int pageIndex = 0, 
    int pageSize = 20)
{
    // 根据状态筛选获取任务
    // 支持: Enqueued, Scheduled, Processing, Succeeded, Failed, Deleted
}
```

### 2. 数据模型

#### HangfireJobModel
任务基本信息:
- JobId: 任务ID
- MethodName: 方法名称
- Status: 任务状态
- CreatedAtUtc: 创建时间
- StartedAtUtc: 开始执行时间
- FinishedAtUtc: 结束时间
- FailureReason: 失败原因
- QueueName: 队列名称
- DurationSeconds: 执行耗时

#### HangfireJobDetailModel
任务详细信息(继承自 HangfireJobModel):
- StateHistory: 状态历史记录
- ParsedArguments: 解析后的任务参数

#### HangfireRecurringJobModel
定时任务信息:
- Id: 定时任务ID
- Cron: Cron 表达式
- MethodName: 方法名称
- NextExecutionUtc: 下次执行时间
- LastJobId: 最后一次任务ID
- LastExecutionUtc: 最后执行时间

#### HangfireStatisticsModel
统计信息:
- EnqueuedCount: 已入队数量
- ScheduledCount: 已调度数量
- ProcessingCount: 处理中数量
- SucceededCount: 成功数量
- FailedCount: 失败数量
- RecurringJobCount: 定时任务数量
- ServerCount: 服务器数量

### 3. Blazor 页面

#### ArchiveJobs.razor
路由: `/archive-jobs`

**主要功能**:

1. **统计卡片**:实时显示各状态任务数量
   - 已入队
   - 已调度
   - 执行中
   - 已成功
   - 已失败
   - 定时任务

2. **任务列表标签页**:
   - 状态筛选(已入队/已调度/执行中/已成功/已失败)
   - 分页列表显示
   - 任务详情查看
   - 失败任务重试
   - 任务删除

3. **定时任务标签页**:
   - 定时任务列表
   - Cron 表达式显示
   - 下次执行时间
   - 立即触发执行
   - 移除定时任务

4. **自动刷新**:
   - 支持自动/手动刷新切换
   - 自动刷新间隔: 5秒

5. **任务详情抽屉**:
   - 完整的任务信息
   - 状态历史时间线
   - 任务参数解析
   - 失败原因展示

## 使用方式

### 1. 服务注册

在 `Program.cs` 中注册服务:

```csharp
builder.Services.AddScoped<IHangfireMonitorService, HangfireMonitorService>();
```

### 2. 导航菜单

在 `NavMenu.razor` 中添加菜单项:

```razor
<div class="nav-item px-3">
    <NavLink class="nav-link" href="archive-jobs">
        <span class="oi oi-timer" aria-hidden="true"></span> 归档任务
    </NavLink>
</div>
```

### 3. 访问页面

启动应用后,点击侧边栏的"归档任务"菜单即可访问。

## 禁用默认 Dashboard(可选)

如果完全使用自定义页面,可以禁用默认的 Hangfire Dashboard:

### 方法 1: 移除 Dashboard 中间件

在 `DbArchiveTool.Api/Program.cs` 中注释掉或删除以下代码:

```csharp
// 注释掉这行
// app.UseHangfireDashboard("/hangfire", new DashboardOptions
// {
//     Authorization = new[] { new HangfireAuthorizationFilter() }
// });
```

### 方法 2: 限制 Dashboard 访问(推荐)

保留 Dashboard 但限制访问,仅供管理员使用:

```csharp
app.UseHangfireDashboard("/hangfire", new DashboardOptions
{
    Authorization = new[] { new HangfireAuthorizationFilter() },
    // 添加更严格的授权策略
    IsReadOnlyFunc = (DashboardContext context) => true // 只读模式
});
```

### 方法 3: 双轨制(推荐)

同时保留两个入口:
- **自定义页面** (`/archive-jobs`): 面向普通用户,提供简洁的业务视图
- **默认 Dashboard** (`/hangfire`): 面向管理员,提供完整的技术视图

## 页面截图示例

### 统计卡片
```
┌────────────┬────────────┬────────────┬────────────┬────────────┬────────────┐
│  已入队    │  已调度    │  执行中    │  已成功    │  已失败    │  定时任务  │
│    12      │     5      │     3      │   1,234    │     8      │     2      │
└────────────┴────────────┴────────────┴────────────┴────────────┴────────────┘
```

### 任务列表
```
┌──────────────────┬────────┬────────────────────────┬──────┬──────────────────┬────────┐
│ 任务ID           │ 状态   │ 方法名称               │ 队列 │ 创建时间         │ 操作   │
├──────────────────┼────────┼────────────────────────┼──────┼──────────────────┼────────┤
│ abc123def456...  │ 执行中 │ ArchiveJobService.E... │archive│2025-11-04 10:30 │ 详情   │
│ def789ghi012...  │ 已成功 │ ArchiveJobService.E... │archive│2025-11-04 10:25 │ 详情   │
│ ghi345jkl678...  │ 已失败 │ ArchiveJobService.E... │archive│2025-11-04 10:20 │详情|重试│
└──────────────────┴────────┴────────────────────────┴──────┴──────────────────┴────────┘
```

### 定时任务列表
```
┌───────────────────┬────────────────────────┬───────────┬────────┬──────────────────┬──────────┐
│ 任务ID            │ 方法名称               │Cron表达式 │ 队列   │ 下次执行         │ 操作     │
├───────────────────┼────────────────────────┼───────────┼────────┼──────────────────┼──────────┤
│ daily-archive-all │ ArchiveJobService.E... │ 0 2 * * * │archive │2025-11-05 02:00  │立即执行|移除│
└───────────────────┴────────────────────────┴───────────┴────────┴──────────────────┴──────────┘
```

## 扩展建议

### 1. 与归档配置关联

可以在任务详情中显示关联的归档配置信息:

```csharp
// 解析任务参数中的 configId
if (Guid.TryParse(argumentValue, out var configId))
{
    var config = await _archiveConfigService.GetByIdAsync(configId);
    model.ArchiveConfigName = config?.Name;
}
```

### 2. 添加任务执行图表

使用 Chart.js 或其他图表库展示:
- 任务执行趋势
- 成功率统计
- 执行耗时分析

### 3. 邮件/通知集成

当任务失败时发送通知:

```csharp
if (job.Status == "Failed")
{
    await _notificationService.SendAsync(
        $"归档任务失败: {job.MethodName}",
        job.FailureReason
    );
}
```

### 4. 任务日志查看

集成 BackgroundTask 的日志系统,在详情页显示完整的执行日志。

## 技术细节

### 状态映射

Hangfire 状态 → 中文显示:
- `Enqueued` → 已入队
- `Scheduled` → 已调度
- `Processing` → 执行中
- `Succeeded` → 已成功
- `Failed` → 已失败
- `Deleted` → 已删除

### 颜色标签

- 已入队: 青色 (Cyan)
- 已调度: 蓝色 (Blue)
- 执行中: 处理中 (Processing) - 带动画
- 已成功: 成功 (Success) - 绿色
- 已失败: 错误 (Error) - 红色
- 已删除: 默认 (Default) - 灰色

### 自动刷新机制

```csharp
private void StartAutoRefresh()
{
    _refreshTimer = new System.Threading.Timer(async _ =>
    {
        await InvokeAsync(async () =>
        {
            if (_autoRefresh)
            {
                await RefreshAsync();
                StateHasChanged();
            }
        });
    }, null, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));
}
```

## 总结

自定义 Hangfire 监控页面提供了:
- ✅ 与系统 UI 风格统一的界面
- ✅ 简化的业务视图
- ✅ 完整的任务管理功能
- ✅ 实时刷新和监控
- ✅ 易于扩展和定制

建议采用**双轨制**:
- 日常使用自定义页面 (`/archive-jobs`)
- 技术调试使用默认 Dashboard (`/hangfire`)

这样既保持了用户体验的一致性,又保留了完整的技术调试能力。
