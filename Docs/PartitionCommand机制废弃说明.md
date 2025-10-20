# PartitionCommand 机制废弃说明与清理建议

> **命名更新说明：** 文中原有的 BackgroundTask 已统一更名为 BackgroundTask，相关仓储、调度器等类型也使用 BackgroundTask* 命名。

## 📋 问题分析

### 当前系统中的两套并行机制

经过全面检查,发现系统中存在**两套并行的任务管理机制**,造成架构混乱和开发歧义:

| 机制 | 用途 | 状态 | 问题 |
|------|------|------|------|
| **PartitionCommand** | 命令审批流程 | ❌ 已过时 | 需要手动审批,无UI支持,Split已废弃 |
| **BackgroundTask** | 统一任务调度 | ✅ 正在使用 | 完整的UI和后台服务支持 |

### PartitionCommand 的使用情况统计

#### 1. Domain 层 (领域模型)

**文件清单:**
- `PartitionCommand.cs` - 命令聚合根
- `PartitionCommandStatus.cs` - 命令状态枚举
- `PartitionCommandType.cs` - 命令类型枚举
- `IPartitionCommandRepository.cs` - 命令仓储接口
- `IPartitionCommandQueue.cs` - 命令队列接口
- `IPartitionCommandScriptGenerator.cs` - 脚本生成器接口

**状态:**
- ✅ Split (拆分) - **已在 `ExecuteSplitAsync` 中废弃,改用 `BackgroundTask`**
- ⚠️ Merge (合并) - 仍在 `ExecuteMergeAsync` 中使用 `PartitionCommand.CreateMerge(...)`
- ⚠️ Switch (切换) - 仍在 `ExecuteSwitchAsync` 中使用 `PartitionCommand.CreateSwitch(...)`

#### 2. Application 层 (应用服务)

**文件清单:**
- `PartitionCommandAppService.cs` - 命令应用服务
- `IPartitionCommandAppService.cs` - 服务接口

**当前使用:**
```csharp
// ✅ Split 已废弃 PartitionCommand,改用 BackgroundTask
public async Task<Result<Guid>> ExecuteSplitAsync(...)
{
    var task = BackgroundTask.Create(
        operationType: BackgroundTaskOperationType.SplitBoundary, ...);
    await dispatcher.DispatchAsync(task.Id, ...);
}

// ❌ Merge 仍在使用 PartitionCommand
public async Task<Result<Guid>> ExecuteMergeAsync(...)
{
    var command = PartitionCommand.CreateMerge(...);
    await commandRepository.AddAsync(command, ...);
    return Result<Guid>.Success(command.Id); // 返回命令ID,不是任务ID!
}

// ❌ Switch 仍在使用 PartitionCommand
public async Task<Result<Guid>> ExecuteSwitchAsync(...)
{
    var command = PartitionCommand.CreateSwitch(...);
    await commandRepository.AddAsync(command, ...);
    return Result<Guid>.Success(command.Id);
}
```

#### 3. Infrastructure 层 (基础设施)

**文件清单:**
- `PartitionCommandRepository.cs` - EF Core 仓储实现
- `PartitionCommandQueue.cs` - 内存队列实现
- `PartitionCommandHostedService.cs` - 后台任务服务
- `TSqlPartitionCommandScriptGenerator.cs` - T-SQL 脚本生成器
- **执行器:**
  - `SplitPartitionCommandExecutor.cs` - 拆分执行器 (已无用)
  - `MergePartitionCommandExecutor.cs` - 合并执行器 (仍在用)
  - `SwitchPartitionCommandExecutor.cs` - 切换执行器 (仍在用)
- `ArchiveDbContext.cs` - 包含 `DbSet<PartitionCommand>`
- **数据库迁移:** 所有迁移文件中都包含 `PartitionCommand` 表定义

#### 4. API 层

**文件清单:**
- `Controllers/V1/PartitionCommandsController.cs` - REST API 控制器

**提供的端点:**
```csharp
POST /api/v1/partition-commands/split/preview
POST /api/v1/partition-commands/split/execute
POST /api/v1/partition-commands/{commandId}/approve  // ⚠️ 仍然存在
POST /api/v1/partition-commands/{commandId}/reject
GET  /api/v1/partition-commands/{commandId}/status
```

#### 5. 测试代码

**文件清单:**
- `tests/DbArchiveTool.UnitTests/Partitions/PartitionCommandTests.cs` - 单元测试
- 其他测试中的 Mock 依赖

## 🎯 清理建议

### 方案 A: 彻底废弃 PartitionCommand (推荐)

**理由:**
1. ✅ **架构统一:** 所有分区操作统一使用 `BackgroundTask`
2. ✅ **UI支持:** "任务调度"模块已支持完整的任务管理
3. ✅ **简化代码:** 减少 50% 以上的冗余代码
4. ✅ **避免歧义:** 开发人员不会混淆两套机制
5. ✅ **易于维护:** 单一职责,统一的后台服务和队列

**实施步骤:**

#### 步骤 1: 迁移 Merge 和 Switch 到 BackgroundTask

**修改 `PartitionCommandAppService.ExecuteMergeAsync`:**

```csharp
public async Task<Result<Guid>> ExecuteMergeAsync(MergePartitionRequest request, CancellationToken cancellationToken = default)
{
    // 验证和预览逻辑保持不变...
    
    var script = preview.Value!.Script;
    var payload = JsonSerializer.Serialize(new
    {
        request.SchemaName,
        request.TableName,
        request.BoundaryKey,
        DdlScript = script,
        request.BackupConfirmed
    });

    // ✅ 改用 BackgroundTask
    var task = BackgroundTask.Create(
        partitionConfigurationId: Guid.NewGuid(),
        dataSourceId: request.DataSourceId,
        requestedBy: request.RequestedBy,
        createdBy: request.RequestedBy,
        backupReference: null,
        notes: $"合并分区边界 {request.BoundaryKey}",
        priority: 0,
        operationType: Shared.Partitions.BackgroundTaskOperationType.MergeBoundary, // ✅ 使用 MergeBoundary
        archiveScheme: null,
        archiveTargetConnection: null,
        archiveTargetDatabase: null,
        archiveTargetTable: null);

    task.SaveConfigurationSnapshot(payload, request.RequestedBy);
    await taskRepository.AddAsync(task, cancellationToken);
    
    // 记录日志和审计...
    await dispatcher.DispatchAsync(task.Id, request.DataSourceId, cancellationToken);
    
    return Result<Guid>.Success(task.Id);
}
```

**修改 `PartitionCommandAppService.ExecuteSwitchAsync`:**

类似的改造,使用 `BackgroundTaskOperationType.ArchiveSwitch`。

#### 步骤 2: 删除 PartitionCommand 相关代码

**Domain 层删除:**
- ✅ `PartitionCommand.cs`
- ✅ `PartitionCommandStatus.cs`
- ✅ `PartitionCommandType.cs`
- ✅ `IPartitionCommandRepository.cs`
- ✅ `IPartitionCommandQueue.cs`
- ✅ `IPartitionCommandScriptGenerator.cs`

**Application 层重命名:**
- ⚠️ `PartitionCommandAppService.cs` → 保留但重命名为 `PartitionScriptPreviewService.cs` (仅保留预览功能)
- ⚠️ `IPartitionCommandAppService.cs` → `IPartitionScriptPreviewService.cs`

**Infrastructure 层删除:**
- ✅ `PartitionCommandRepository.cs`
- ✅ `PartitionCommandQueue.cs`
- ✅ `PartitionCommandHostedService.cs`
- ✅ `TSqlPartitionCommandScriptGenerator.cs` → 合并到 `PartitionExecutionProcessor`
- ✅ `SplitPartitionCommandExecutor.cs`
- ✅ `MergePartitionCommandExecutor.cs`
- ✅ `SwitchPartitionCommandExecutor.cs`
- ⚠️ `ArchiveDbContext.cs` - 移除 `DbSet<PartitionCommand>`

**API 层删除:**
- ✅ `Controllers/V1/PartitionCommandsController.cs` 中的审批端点:
  - `POST /api/v1/partition-commands/{commandId}/approve`
  - `POST /api/v1/partition-commands/{commandId}/reject`
  - `GET /api/v1/partition-commands/{commandId}/status`
- ⚠️ 保留预览端点 (可移到 PartitionManagementController):
  - `POST /api/v1/partition-commands/split/preview`
  - `POST /api/v1/partition-commands/merge/preview`
  - `POST /api/v1/partition-commands/switch/preview`

**测试代码删除:**
- ✅ `PartitionCommandTests.cs` 中与命令创建/审批相关的测试
- ⚠️ 保留预览相关的测试

#### 步骤 3: 数据库迁移

**创建新的迁移删除 PartitionCommand 表:**

```bash
dotnet ef migrations add RemovePartitionCommandTable --project src/DbArchiveTool.Infrastructure --startup-project src/DbArchiveTool.Api
```

**迁移内容:**
```csharp
public partial class RemovePartitionCommandTable : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        // 警告: 此操作将删除 PartitionCommand 表及其所有数据
        // 请确保已将旧命令数据迁移到 BackgroundTasks 或备份
        migrationBuilder.DropTable(name: "PartitionCommand");
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        // 回滚时重新创建表(用于紧急恢复)
        migrationBuilder.CreateTable(
            name: "PartitionCommand",
            columns: table => new { /* ... */ });
    }
}
```

#### 步骤 4: 更新依赖注入配置

**`Infrastructure/DependencyInjection.cs`:**

```csharp
// ❌ 删除
services.AddScoped<IPartitionCommandRepository, PartitionCommandRepository>();
services.AddScoped<IPartitionCommandScriptGenerator, TSqlPartitionCommandScriptGenerator>();
services.AddSingleton<IPartitionCommandQueue, PartitionCommandQueue>();
services.AddScoped<IPartitionCommandExecutor, SplitPartitionCommandExecutor>();
services.AddScoped<IPartitionCommandExecutor, MergePartitionCommandExecutor>();
services.AddScoped<IPartitionCommandExecutor, SwitchPartitionCommandExecutor>();
services.AddHostedService<PartitionCommandHostedService>();

// ✅ 只保留任务调度相关
services.AddScoped<IBackgroundTaskRepository, BackgroundTaskRepository>();
services.AddScoped<IPartitionExecutionDispatcher, PartitionExecutionDispatcher>();
services.AddHostedService<PartitionExecutionHostedService>();
```

### 方案 B: 保留但标记为过时 (不推荐)

如果暂时无法完全废弃,可以:

1. **标记为 `[Obsolete]`:**
```csharp
[Obsolete("PartitionCommand 已废弃,请使用 BackgroundTask 机制。将在 v2.0 中移除。")]
internal sealed class PartitionCommandAppService : IPartitionCommandAppService
{
    // ...
}
```

2. **在文档中明确说明:**
   - 新功能禁止使用 `PartitionCommand`
   - 现有的 Merge/Switch 将在下一版本迁移
   - 提供迁移指南

## 📊 影响评估

### 代码量统计

| 类别 | 可删除文件数 | 可删除代码行数(估算) |
|------|-------------|---------------------|
| Domain 模型 | 6 个文件 | ~500 行 |
| Application 服务 | 部分重构 | ~300 行 |
| Infrastructure | 7 个文件 | ~800 行 |
| API 控制器 | 部分端点 | ~100 行 |
| 测试代码 | 部分测试 | ~400 行 |
| **总计** | **~15 个文件** | **~2100 行代码** |

### 迁移风险

| 风险 | 等级 | 缓解措施 |
|------|------|---------|
| 数据丢失 | 🔴 高 | 1. 备份 PartitionCommand 表数据<br>2. 确认无待审批命令 |
| API 破坏性变更 | 🟡 中 | 1. 保留预览端点<br>2. 版本化 API |
| 测试失败 | 🟢 低 | 重写测试使用 BackgroundTask |
| 前端影响 | 🟢 无 | 前端已使用新机制,无影响 |

### 收益分析

| 收益 | 说明 |
|------|------|
| 📉 **减少 50% 冗余代码** | 删除 ~2100 行不必要的代码 |
| 🎯 **架构统一** | 单一的任务调度机制,清晰的职责边界 |
| 🚀 **开发效率提升** | 新功能开发时无需选择使用哪套机制 |
| 📚 **降低学习成本** | 新开发人员只需学习一套任务系统 |
| 🔧 **易于维护** | 单一后台服务,统一的监控和日志 |

## ✅ 推荐实施方案

### 分阶段清理计划

#### Phase 1: 迁移 Merge 和 Switch (本次修复)

**时间:** 1-2 天

1. ✅ 修改 `ExecuteMergeAsync` 使用 `BackgroundTask` + `MergeBoundary`
2. ✅ 修改 `ExecuteSwitchAsync` 使用 `BackgroundTask` + `ArchiveSwitch`
3. ✅ 更新相关测试
4. ✅ 验证功能正常

#### Phase 2: 删除冗余代码 (后续版本)

**时间:** 1 天

1. ✅ 删除 `PartitionCommand*Executor` 类
2. ✅ 删除 `PartitionCommandHostedService`
3. ✅ 删除 Domain 层的 PartitionCommand 相关类
4. ✅ 更新依赖注入配置

#### Phase 3: 数据库清理 (生产部署前)

**时间:** 0.5 天

1. ✅ 备份 PartitionCommand 表数据
2. ✅ 创建删除表的迁移
3. ✅ 在测试环境验证
4. ✅ 生产环境部署

## 📝 操作类型映射表

清理后,所有分区操作统一使用 `BackgroundTaskOperationType`:

| 操作 | BackgroundTaskOperationType | 说明 |
|------|-------------------------------|------|
| 添加边界值 | `AddBoundary` | ✅ 已实现 |
| 拆分分区 | `SplitBoundary` | ✅ 本次已迁移 |
| 合并分区 | `MergeBoundary` | ⏳ 待迁移 (Phase 1) |
| 分区切换归档 | `ArchiveSwitch` | ⏳ 待迁移 (Phase 1) |
| BCP 归档 | `ArchiveBcp` | 🔜 未来实现 |
| BulkCopy 归档 | `ArchiveBulkCopy` | 🔜 未来实现 |
| 自定义任务 | `Custom` | 🔜 预留扩展 |

## 🎓 开发规范更新

### 新增分区操作的标准流程

**✅ 正确做法:**

```csharp
// 1. 创建 BackgroundTask
var task = BackgroundTask.Create(
    partitionConfigurationId: configId,
    dataSourceId: dataSourceId,
    requestedBy: user,
    createdBy: user,
    operationType: BackgroundTaskOperationType.SplitBoundary, // 根据操作选择类型
    ...);

// 2. 保存配置快照
task.SaveConfigurationSnapshot(payload, user);
await taskRepository.AddAsync(task, cancellationToken);

// 3. 记录日志
await logRepository.AddAsync(logEntry, cancellationToken);

// 4. 记录审计
await auditLogRepository.AddAsync(auditLog, cancellationToken);

// 5. 分派到执行队列
await dispatcher.DispatchAsync(task.Id, dataSourceId, cancellationToken);

// 6. 返回任务ID
return Result<Guid>.Success(task.Id);
```

**❌ 错误做法(已废弃):**

```csharp
// ❌ 不要创建 PartitionCommand
var command = PartitionCommand.CreateSplit(...);
await commandRepository.AddAsync(command, ...);

// ❌ 不要手动调用审批
await ApproveAsync(command.Id, ...);
```

### 任务调度统一原则

1. ✅ **统一入口:** 所有分区操作通过 `BackgroundTask` 创建
2. ✅ **统一队列:** 使用 `IPartitionExecutionDispatcher` 分派任务
3. ✅ **统一执行:** `PartitionExecutionHostedService` 后台服务统一处理
4. ✅ **统一监控:** "任务调度"模块统一展示和管理
5. ✅ **统一日志:** 使用 `PartitionExecutionLogEntry` 记录执行过程

---

**文档创建日期:** 2025-10-20  
**状态:** 🔴 待实施  
**优先级:** 高 (影响架构清晰度和新功能开发)  
**预计工作量:** 2-3 天 (分阶段实施)

