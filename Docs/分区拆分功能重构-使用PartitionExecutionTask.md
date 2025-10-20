# 分区拆分功能重构 - 使用 BackgroundTask 机制

> **命名更新说明：** 代码中该机制现以 BackgroundTask 命名实现，文档保留旧名以便对照。

## 问题描述

用户反馈"分区拆分"功能提交成功,但在"任务调度"模块中看不到任务显示和执行。

## 根本原因

**架构理解错误:** 最初实现的"分区拆分"功能使用了 `PartitionCommand` 机制,这是一个**命令审批流程**,需要手动审批后才会加入执行队列。而"分区值添加"功能使用的是 `BackgroundTask` 机制,**直接创建任务并自动分派到执行队列**。

### 系统中的两种机制对比

| 特性 | PartitionCommand (旧机制) | BackgroundTask (正确机制) |
|------|---------------------------|----------------------------------|
| 用途 | 命令审批流程 (预留扩展) | 分区执行任务管理 |
| 创建后 | 需要手动/自动调用 `ApproveAsync` 审批 | 直接调用 `dispatcher.DispatchAsync` 加入队列 |
| 显示位置 | 无专用UI (需要通过 commandId 查询状态) | "任务调度"模块 (`Monitor.razor`) |
| 操作类型 | CommandType (Split/Merge/Switch) | `BackgroundTaskOperationType` 枚举 |
| 后台服务 | `PartitionCommandHostedService` | `PartitionExecutionHostedService` |
| 数据库表 | `PartitionCommands` | `BackgroundTasks` |

### BackgroundTaskOperationType 枚举

```csharp
public enum BackgroundTaskOperationType
{
    Unknown = 0,
    
    // 边界操作
    AddBoundary = 10,       // ✅ "分区值添加"使用
    SplitBoundary = 11,     // ✅ "分区拆分"应该使用
    MergeBoundary = 12,     // "分区合并"应该使用
    
    // 归档相关
    ArchiveSwitch = 30,     // 归档(分区切换)
    ArchiveBcp = 31,
    ArchiveBulkCopy = 32,
    
    Custom = 99
}
```

## 修复方案

### 核心思路

**参照"分区值添加"功能 (`AddBoundaryToPartitionedTableAsync`),将"分区拆分"功能改为创建 `BackgroundTask` 并使用 `BackgroundTaskOperationType.SplitBoundary`。**

### 修改文件清单

#### 1. `PartitionCommandAppService.cs` (后端服务)

**添加依赖注入:**

```csharp
internal sealed class PartitionCommandAppService : IPartitionCommandAppService
{
    // 原有依赖...
    private readonly IBackgroundTaskRepository taskRepository;
    private readonly IPartitionExecutionLogRepository logRepository;
    private readonly IPartitionAuditLogRepository auditLogRepository;
    private readonly IPartitionExecutionDispatcher dispatcher;

    public PartitionCommandAppService(
        // 原有参数...
        IBackgroundTaskRepository taskRepository,
        IPartitionExecutionLogRepository logRepository,
        IPartitionAuditLogRepository auditLogRepository,
        IPartitionExecutionDispatcher dispatcher)
    {
        // 初始化所有字段...
    }
}
```

**重写 `ExecuteSplitAsync` 方法:**

```csharp
public async Task<Result<Guid>> ExecuteSplitAsync(SplitPartitionRequest request, CancellationToken cancellationToken = default)
{
    // 1. 验证输入 (保持不变)
    // 2. 预览脚本 (保持不变)
    // 3. 获取配置 (保持不变)
    // 4. 解析边界值 (保持不变)
    
    var script = preview.Value!.Script;
    var boundaryValues = values.Value!.Select(v => v.ToInvariantString()).ToArray();
    
    // 5. 准备任务上下文
    var resourceId = $"{request.DataSourceId}/{request.SchemaName}/{request.TableName}";
    var summary = $"拆分表 {request.SchemaName}.{request.TableName} 的分区边界";
    var payload = JsonSerializer.Serialize(new
    {
        request.SchemaName,
        request.TableName,
        configuration.PartitionFunctionName,
        configuration.PartitionSchemeName,
        Boundaries = boundaryValues,
        DdlScript = script,
        request.BackupConfirmed
    });

    // 6. 创建 BackgroundTask (而非 PartitionCommand)
    var task = BackgroundTask.Create(
        partitionConfigurationId: Guid.NewGuid(), // 临时ID
        dataSourceId: request.DataSourceId,
        requestedBy: request.RequestedBy,
        createdBy: request.RequestedBy,
        backupReference: null,
        notes: $"批量拆分 {boundaryValues.Length} 个边界值",
        priority: 0,
        operationType: Shared.Partitions.BackgroundTaskOperationType.SplitBoundary, // ✅ 关键!
        archiveScheme: null,
        archiveTargetConnection: null,
        archiveTargetDatabase: null,
        archiveTargetTable: null);

    // 7. 保存配置快照
    task.SaveConfigurationSnapshot(payload, request.RequestedBy);
    await taskRepository.AddAsync(task, cancellationToken);

    // 8. 记录日志
    var initialLog = PartitionExecutionLogEntry.Create(task.Id, "Info", "任务创建", ...);
    await logRepository.AddAsync(initialLog, cancellationToken);

    // 9. 记录审计日志
    var auditLog = PartitionAuditLog.Create(
        request.RequestedBy,
        Shared.Partitions.BackgroundTaskOperationType.SplitBoundary.ToString(),
        "PartitionedTable",
        resourceId,
        summary,
        payload,
        "Queued",
        script);
    await auditLogRepository.AddAsync(auditLog, cancellationToken);

    // 10. 直接分派到执行队列 (无需审批!)
    await dispatcher.DispatchAsync(task.Id, request.DataSourceId, cancellationToken);

    return Result<Guid>.Success(task.Id); // 返回任务ID
}
```

**关键变化:**
- ❌ 删除: `PartitionCommand.CreateSplit(...)` + `commandRepository.AddAsync(command, ...)`
- ✅ 添加: `BackgroundTask.Create(operationType: SplitBoundary)` + `dispatcher.DispatchAsync(task.Id, ...)`
- ✅ 返回: 任务ID (而非命令ID)

#### 2. `PartitionSplitWizard.razor` (前端组件)

**简化提交逻辑,移除审批步骤:**

```csharp
private async Task HandleSubmit()
{
    // 收集边界值 (保持不变)
    var boundariesToSplit = _generationMode == GenerationMode.Single
        ? new[] { GetBoundaryValueForDisplay() }
        : _generatedBoundaries.OrderBy(b => b).ToArray();

    var request = new SplitPartitionRequest(...);

    // ✅ 直接执行,后端会自动分派到队列
    var executeResult = await PartitionManagementApi.ExecuteSplitAsync(DataSourceId, request);
    if (!executeResult.IsSuccess)
    {
        _validationError = $"提交拆分任务失败: {executeResult.Error}";
        return;
    }

    var taskId = executeResult.Value; // ✅ 现在返回的是任务ID

    // ❌ 删除: ApproveAsync 调用和相关错误处理
    
    var countMsg = boundariesToSplit.Length > 1 ? $"({boundariesToSplit.Length}个边界值)" : string.Empty;
    Message.Success($"拆分任务{countMsg}已提交并加入执行队列!任务ID: {taskId}");
    await OnSuccess.InvokeAsync();
    await HandleClose();
}
```

**变化总结:**
- ❌ 删除: `ApproveAsync` 调用 (20+ 行代码)
- ✅ 简化: 直接显示成功消息,使用任务ID
- ✅ 用户体验: 与"分区值添加"功能保持一致

## 测试验证

### 1. 功能测试

**测试场景 1: 单值拆分**
1. 打开"分区管理"页面
2. 选择一个分区,点击"拆分分区"
3. 输入单个边界值,勾选"已备份确认"
4. 点击"提交"
5. ✅ 应显示: `拆分任务已提交并加入执行队列!任务ID: xxx`
6. ✅ 打开"任务调度"模块,应看到类型为"拆分分区"的任务
7. ✅ 任务状态应为"已排队" → "执行中" → "已成功/已失败"

**测试场景 2: 批量拆分**
1. 打开"分区管理"页面
2. 选择一个分区,点击"拆分分区"
3. 切换到"批量生成"模式
4. 配置生成规则,生成多个边界值(例如 10 个)
5. 点击"提交"
6. ✅ 应显示: `拆分任务(10个边界值)已提交并加入执行队列!任务ID: xxx`
7. ✅ "任务调度"模块应看到任务
8. ✅ 查看任务详情,应包含所有 10 个边界值的 DDL 脚本

**测试场景 3: 任务执行**
1. 观察"任务调度"模块中的任务状态变化
2. ✅ 状态流转: 已排队 → 执行中 → 已成功
3. ✅ 点击任务行,查看执行日志
4. ✅ 日志应包含: 任务创建、DDL脚本生成、执行成功等信息

### 2. 对比测试

| 功能 | 创建机制 | 显示位置 | 状态流转 |
|------|---------|---------|---------|
| **分区值添加** | BackgroundTask + AddBoundary | 任务调度 | 已排队 → 执行中 → 已成功 |
| **分区拆分 (修复后)** | BackgroundTask + SplitBoundary | 任务调度 | 已排队 → 执行中 → 已成功 |

两者应保持完全一致的行为!

### 3. 数据库验证

**检查任务表:**
```sql
SELECT TOP 10 
    Id,
    DataSourceId,
    OperationType,  -- 应为 'SplitBoundary'
    Status,
    RequestedBy,
    CreatedAtUtc
FROM BackgroundTasks
WHERE OperationType = 'SplitBoundary'
ORDER BY CreatedAtUtc DESC;
```

**检查审计日志:**
```sql
SELECT TOP 10
    OperatedBy,
    Action,  -- 应为 'SplitBoundary'
    ResourceType,
    Summary,
    Status,
    CreatedAtUtc
FROM PartitionAuditLogs
WHERE Action = 'SplitBoundary'
ORDER BY CreatedAtUtc DESC;
```

## 后续优化建议

### 1. 统一命令机制

目前 `PartitionCommand` 机制仍然存在但未被实际使用。建议:
- **短期:** 保留 `PartitionCommandAppService` 但标记为 `[Obsolete]`
- **长期:** 如果确定不需要审批流程,可以移除整个 `PartitionCommand` 机制

### 2. 任务调度页面增强

"任务调度"模块 (`Monitor.razor`) 已支持显示操作类型:
```csharp
private string GetOperationDisplay(BackgroundTaskSummaryModel task) =>
    task.OperationType switch
    {
        BackgroundTaskOperationType.AddBoundary => "添加分区值",
        BackgroundTaskOperationType.SplitBoundary => "拆分分区",
        BackgroundTaskOperationType.MergeBoundary => "合并分区",
        BackgroundTaskOperationType.ArchiveSwitch => "归档（分区切换）",
        // ...
    };
```

建议增加:
- 筛选条件: 按操作类型过滤
- 批量操作: 取消任务、重试任务
- 详情展示: DDL 脚本预览

### 3. 用户提示优化

在提交成功后,除了显示任务ID,还可以:
```csharp
Message.Success($"拆分任务已提交!点击查看", onClose: () => 
{
    NavigationManager.NavigateTo($"/partition-executions/monitor?taskId={taskId}");
});
```

直接跳转到"任务调度"页面并高亮显示该任务。

## 总结

**修复关键点:**
1. ✅ 使用 `BackgroundTask` 而非 `PartitionCommand`
2. ✅ 设置 `operationType: SplitBoundary`
3. ✅ 调用 `dispatcher.DispatchAsync` 直接分派到队列
4. ✅ 前端移除 `ApproveAsync` 审批调用
5. ✅ 返回任务ID供用户查看

**效果:**
- ✅ "分区拆分"任务会立即出现在"任务调度"模块
- ✅ 后台服务 `PartitionExecutionHostedService` 会自动执行
- ✅ 用户体验与"分区值添加"功能完全一致

---

**修复日期:** 2025-10-20  
**涉及组件:** Application (PartitionCommandAppService), Web (PartitionSplitWizard)  
**验证状态:** ⏳ 待重启应用后测试

