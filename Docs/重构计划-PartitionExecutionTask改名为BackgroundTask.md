# BackgroundTask → BackgroundTask 重构计划

## 📋 重构目标

将 `BackgroundTask` 及相关类重命名为更通用的 `BackgroundTask`,使其能够承载所有类型的后台任务,不仅限于分区操作。

## ✅ 数据安全保证

### 现有数据不会丢失
- ✅ 使用 EF Core 迁移安全重命名数据库表 `BackgroundTasks` → `BackgroundTasks`
- ✅ 所有历史任务记录、状态、日志都会完整保留
- ✅ 主键、外键、索引关系保持不变
- ✅ 迁移支持回滚,可以安全撤销

### 迁移策略
```sql
-- 迁移会生成类似以下的 SQL:
EXEC sp_rename 'BackgroundTasks', 'BackgroundTasks';
EXEC sp_rename 'BackgroundTasks.PK_BackgroundTasks', 'PK_BackgroundTasks';
-- 其他索引和约束也会自动重命名
```

## 📦 重构范围

### 阶段 1: Domain 层 (核心模型)

#### 1.1 实体类重命名
- [x] `BackgroundTask.cs` → `BackgroundTask.cs`
- [x] `BackgroundTaskStatus.cs` → `BackgroundTaskStatus.cs`
- [x] `BackgroundTaskOperationType.cs` → `BackgroundTaskOperationType.cs`

#### 1.2 仓储接口重命名
- [x] `IBackgroundTaskRepository.cs` → `IBackgroundTaskRepository.cs`

#### 1.3 其他接口重命名
- [x] `IPartitionExecutionDispatcher.cs` → `IBackgroundTaskDispatcher.cs`
- [x] `IPartitionExecutionQueue.cs` → `IBackgroundTaskQueue.cs`

### 阶段 2: Application 层 (应用服务)

#### 2.1 服务类重命名
- [ ] `PartitionExecutionAppService.cs` → `BackgroundTaskAppService.cs`
- [ ] `IPartitionExecutionAppService.cs` → `IBackgroundTaskAppService.cs`

#### 2.2 DTO 模型重命名
- [ ] `BackgroundTaskSummaryModel.cs` → `BackgroundTaskSummaryModel.cs`
- [ ] `BackgroundTaskDetailModel.cs` → `BackgroundTaskDetailModel.cs`
- [ ] `BackgroundTaskQueryRequest.cs` → `BackgroundTaskQueryRequest.cs`

#### 2.3 更新引用
- [ ] `PartitionCommandAppService.cs` - 更新 `IBackgroundTaskRepository` 引用
- [ ] `PartitionManagementAppService.cs` - 更新 `IBackgroundTaskRepository` 引用
- [ ] `PartitionConfigurationAppService.cs` - 更新 `IBackgroundTaskRepository` 引用

### 阶段 3: Infrastructure 层 (基础设施)

#### 3.1 仓储实现重命名
- [ ] `BackgroundTaskRepository.cs` → `BackgroundTaskRepository.cs`

#### 3.2 队列和调度器重命名
- [ ] `PartitionExecutionQueue.cs` → `BackgroundTaskQueue.cs`
- [ ] `PartitionExecutionDispatcher.cs` → `BackgroundTaskDispatcher.cs`

#### 3.3 后台服务重命名
- [ ] `PartitionExecutionHostedService.cs` → `BackgroundTaskHostedService.cs`

#### 3.4 执行器重命名
- [ ] `PartitionExecutionProcessor.cs` → `BackgroundTaskProcessor.cs`

#### 3.5 数据库配置更新
- [ ] `ArchiveDbContext.cs` - 更新 `DbSet<BackgroundTask>`
- [ ] `BackgroundTaskConfiguration.cs` (原 BackgroundTaskConfiguration) - 更新表映射

#### 3.6 依赖注入配置
- [ ] `DependencyInjection.cs` - 更新所有服务注册

### 阶段 4: API 层 (控制器)

#### 4.1 控制器重命名
- [ ] `PartitionExecutionController.cs` → `BackgroundTasksController.cs`

#### 4.2 路由更新
```csharp
// 旧路由: /api/v1/partition-executions
// 新路由: /api/v1/background-tasks
[Route("api/v1/background-tasks")]
```

### 阶段 5: Web 层 (Blazor 前端)

#### 5.1 API 客户端更新
- [ ] `PartitionExecutionApiClient.cs` → `BackgroundTaskApiClient.cs`
- [ ] 更新所有 API 调用路径

#### 5.2 页面组件更新
- [ ] `Pages/TaskScheduling/Index.razor` - 更新 API 客户端引用
- [ ] 其他引用 BackgroundTask 的组件

### 阶段 6: Tests 层 (测试)

#### 6.1 单元测试更新
- [ ] `PartitionExecutionAppServiceTests.cs` → `BackgroundTaskAppServiceTests.cs`
- [ ] 更新所有 Mock 对象和测试数据

#### 6.2 集成测试更新
- [ ] 更新所有使用旧类名的集成测试

### 阶段 7: Database 迁移

#### 7.1 创建重命名迁移
```bash
dotnet ef migrations add RenameBackgroundTaskToBackgroundTask \
  --project src/DbArchiveTool.Infrastructure \
  --startup-project src/DbArchiveTool.Api
```

#### 7.2 迁移内容 (自动生成)
- 重命名表: `BackgroundTasks` → `BackgroundTasks`
- 重命名主键约束
- 重命名索引
- 重命名外键约束

### 阶段 8: 文档更新

#### 8.1 技术文档
- [ ] `总体架构设计.md` - 更新架构图和类名
- [ ] `数据模型与API规范.md` - 更新实体和API路由
- [ ] `分区拆分功能重构-使用BackgroundTask.md` - 更新为 BackgroundTask
- [ ] `PartitionCommand机制废弃说明.md` - 更新术语

#### 8.2 开发规范
- [ ] 新增 `后台任务开发指南.md` - 说明如何添加新的任务类型

## 🔧 实施步骤

### Step 1: Domain 层重构 (安全基础)
1. 重命名 `BackgroundTask.cs` → `BackgroundTask.cs`
2. 重命名枚举: `BackgroundTaskStatus` → `BackgroundTaskStatus`
3. 重命名枚举: `BackgroundTaskOperationType` → `BackgroundTaskOperationType`
4. 重命名接口: `IBackgroundTaskRepository` → `IBackgroundTaskRepository`
5. 重命名接口: `IPartitionExecutionDispatcher` → `IBackgroundTaskDispatcher`
6. 重命名接口: `IPartitionExecutionQueue` → `IBackgroundTaskQueue`
7. ✅ 编译验证

### Step 2: Infrastructure 层重构
1. 重命名 `BackgroundTaskRepository.cs` → `BackgroundTaskRepository.cs`
2. 重命名 `PartitionExecutionQueue.cs` → `BackgroundTaskQueue.cs`
3. 重命名 `PartitionExecutionDispatcher.cs` → `BackgroundTaskDispatcher.cs`
4. 重命名 `PartitionExecutionHostedService.cs` → `BackgroundTaskHostedService.cs`
5. 重命名 `PartitionExecutionProcessor.cs` → `BackgroundTaskProcessor.cs`
6. 更新 `ArchiveDbContext.cs` - `DbSet<BackgroundTask> BackgroundTasks`
7. 更新 `BackgroundTaskConfiguration.cs` - 表名映射为 `BackgroundTasks`
8. 更新 `DependencyInjection.cs` - 所有服务注册
9. ✅ 编译验证

### Step 3: Application 层重构
1. 重命名 `PartitionExecutionAppService.cs` → `BackgroundTaskAppService.cs`
2. 重命名 DTOs: `*SummaryModel`, `*DetailModel`, `*QueryRequest`
3. 更新 `PartitionCommandAppService.cs` 中的引用
4. 更新 `PartitionManagementAppService.cs` 中的引用
5. 更新 `PartitionConfigurationAppService.cs` 中的引用
6. ✅ 编译验证

### Step 4: API 层重构
1. 重命名 `PartitionExecutionController.cs` → `BackgroundTasksController.cs`
2. 更新路由: `/api/v1/background-tasks`
3. 更新 Swagger 文档注释
4. ✅ 编译验证

### Step 5: Web 层重构
1. 重命名 `PartitionExecutionApiClient.cs` → `BackgroundTaskApiClient.cs`
2. 更新所有 API 路径引用
3. 更新 `TaskScheduling/Index.razor` 页面
4. 更新服务注册
5. ✅ 编译验证

### Step 6: 数据库迁移
1. 创建 EF Core 迁移
2. 检查生成的迁移 SQL
3. 在测试环境执行 `dotnet ef database update`
4. 验证表重命名成功
5. 验证历史数据完整
6. ✅ 数据验证

### Step 7: 测试更新
1. 更新单元测试类名和引用
2. 更新集成测试
3. 运行全部测试: `dotnet test`
4. ✅ 测试通过

### Step 8: 文档更新
1. 全局搜索替换文档中的术语
2. 更新架构图
3. 创建《后台任务开发指南》
4. ✅ 文档审查

## 🔍 验证清单

### 编译验证
- [ ] `dotnet build DbArchiveTool.sln` - 无错误无警告

### 功能验证
- [ ] "添加边界值"功能正常 (AddBoundary)
- [ ] "分区拆分"功能正常 (SplitBoundary)
- [ ] 任务调度页面正常显示
- [ ] 任务状态流转正常
- [ ] 后台服务正常消费队列

### 数据验证
- [ ] 历史任务记录完整
- [ ] 任务状态正确
- [ ] 外键关系正常
- [ ] 查询性能无影响

### API 验证
- [ ] Swagger 文档更新正确
- [ ] API 路由可访问
- [ ] 响应模型正确

## 📊 影响评估

### 代码变更统计 (预估)
| 层次 | 重命名文件数 | 修改文件数 | 新增迁移 |
|------|------------|-----------|---------|
| Domain | 6 | 0 | - |
| Application | 5 | 3 | - |
| Infrastructure | 6 | 2 | 1 |
| API | 1 | 0 | - |
| Web | 2 | 1 | - |
| Tests | 2 | 5 | - |
| Docs | 0 | 6 | - |
| **总计** | **22** | **17** | **1** |

### 风险等级
- 🟢 **低风险:** 使用 EF Core 迁移,支持回滚
- 🟢 **低风险:** 编译时错误会提前发现
- 🟢 **低风险:** 有完整测试覆盖

### 回滚计划
如果迁移后出现问题:
```bash
# 回滚数据库迁移
dotnet ef database update PreviousMigrationName \
  --project src/DbArchiveTool.Infrastructure \
  --startup-project src/DbArchiveTool.Api

# Git 回滚代码
git revert <commit-hash>
```

## 🎯 预期收益

1. ✅ **语义更清晰:** `BackgroundTask` 更准确描述用途
2. ✅ **易于扩展:** 可以添加非分区相关的后台任务
3. ✅ **降低理解成本:** 新开发人员一目了然
4. ✅ **架构统一:** 所有后台任务统一管理
5. ✅ **文档友好:** 更容易编写和理解文档

## 📅 时间估算

- Domain 层重构: 1 小时
- Infrastructure 层重构: 2 小时
- Application 层重构: 1.5 小时
- API/Web 层重构: 1 小时
- 数据库迁移: 0.5 小时
- 测试更新: 1 小时
- 文档更新: 1 小时
- **总计: 约 8 小时**

---

**开始时间:** 2025-10-20  
**预计完成:** 2025-10-20 (当天完成)  
**状态:** 🚀 准备开始

