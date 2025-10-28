# 数据归档（分区切换方案）设计与任务拆解

> 更新日期：2025-10-27（里程碑 C 启动）  
> 目标：在遵循 SQL Server 分区最佳实践的前提下，实现“分区切换”驱动的数据归档能力，并提供友好的四步向导体验。

## 1. 功能定位与流程概览

- **主打方案**：分区切换（`ALTER TABLE ... SWITCH PARTITION`）；仅在源库与目标库同实例、结构完全对齐时启用。
- **备用方案**：BCP/BulkCopy 作为占位选项；当实例不同或预检未通过时，提示用户后续版本支持。
- **统一流程**：
  1. **目标确认**：选择目标服务器/数据库，判断是否与源实例一致。
  2. **预检校验**：检测分区切换所需全部条件，输出阻塞项/可自动修复项/警告项。
  3. **自动补齐**：对可修复项生成补齐计划（创建空表、同步分区结构/索引、清理残留数据等），执行成功后落地切换。
  4. **执行归档**：在单事务内执行切换；生成日志快照并回写后台任务。

## 2. 技术设计要点

### 2.1 预检规则（`PartitionSwitchInspectionService`）
- 确认源/目标同实例；若不同则阻断切换。
- 比对分区函数/方案名称、边界值、Boundary On Right 设置。
- 校验目标表存在且为空、无触发器、无外键依赖。
- 对齐索引：聚簇/非聚簇、包含列、索引选项（填充因子、压缩等）。
- 比对约束（CHECK、DEFAULT、UNIQUE）、行 GUID、IDENTITY、统计信息。
- 检查文件组是否可写、剩余空间、锁冲突、权限（ALTER/CONTROL）。
- 分类产出：
  - **Blocker**：必须手动处理后才能继续（结构差异、跨实例等）。
  - **AutoFix**：系统可自动补齐（创建分区函数/方案/表、同步索引、清理数据）。
  - **Warning**：风险提示（缺少备份、统计过期等）。

> 进度（2025-10-28）：**P0 后端能力全部完成** ✅
>
> - **检查服务增强**: 完成索引、约束、权限、锁、分区对齐等全方位预检。
> - **补齐计划结构**: `PartitionSwitchPlan` 统一输出 Blocker/AutoFix/Warning 计划骨架。
> - **AutoFix 执行器**: 实现 6 种自动补齐步骤（表创建/分区对象/索引/约束/数据清理/统计刷新），均支持失败回滚。
> - **切换执行增强**: 三阶段日志输出 + SQL 异常分类 + 错误详情脚本附加。
> - **API 层完整**: 预检/自动补齐/执行端点已存在且 DTO 支持目标数据库字段。
>
> **后端核心能力已闭环，可进入前端向导开发阶段。**

### 2.2 自动补齐策略（`PartitionSwitchPlan`）
- **对象创建**：
  - 复制源表结构至目标（含分区列、主键、列属性）。
  - 同步分区函数、分区方案，并映射到目标文件组。
  - 批量创建索引、约束、统计，保持 PERSISTED/NOT FOR REPLICATION 等属性一致。
- **环境准备**：
  - 清空目标表残留数据。
  - 如需，自动更新统计、重建索引。
- **事务控制**：全部 DDL 在一个事务中执行，启用 `XACT_ABORT ON`；失败即回滚。
- **日志记录**：补齐计划与实际执行 SQL 写入 Markdown 日志供审计。

### 2.3 后端执行链路
- `IPartitionSwitchAppService`
  - `InspectAsync`：返回预检报告与补齐计划。
  - `AutoFixAsync`：根据用户勾选执行补齐，返回执行日志。
  - `SwitchAsync` / `ExecuteSwitchAsync`：完成最终切换并持久化任务快照。
- `BackgroundTask` 记录：
  - `OperationType = ArchiveSwitch`
  - 快照中保存预检报告、补齐计划、执行 SQL、结果状态。
  - 日志以 Markdown 形式呈现，便于前端渲染。

> 进度（2025-10-28）：`SqlPartitionCommandExecutor.ExecuteSwitchWithTransactionAsync` 已输出分阶段信息（准备/事务/执行）并对常见 `SqlException.Number` 做中文分类描述，同时在失败详情中附带切换脚本，便于后台任务日志直接引用。完整支持跨库切换的三限定名格式。

### 2.4 前端向导（`PartitionArchiveWizard.razor`）
1. **方案选择**（Step 1）：仅允许勾选分区切换，其余方案灰显并提示“规划中”。
2. **参数配置**（Step 2）：
   - 选择源数据源/表、分区编号；
   - 选择目标数据库/表（提示是否同实例）；
   - 勾选“已完成备份”前仅做提醒，不阻断流程。
3. **预检结果**（Step 3）：
   - 展示 Blocker/AutoFix/Warning 列表；
   - Blocker 显示原因与处理建议；
   - AutoFix 提供“系统自动处理”勾选项；
   - Warning 要求用户确认风险。
4. **执行确认**（Step 4）：
   - 显示补齐脚本与最终 SWITCH SQL（Markdown）；
   - 勾选“确认已备份且知晓风险”后可提交；
   - 调用 API 触发后台任务并跳转任务详情页。

## 3. TODO 列表（按优先级）

### P0（立即启动）✅ **已完成**
1. **需求整理** ✅
   - [x] 归档向导 Step 文案与交互原型确定。
   - [x] SQL Server 分区切换强制条件脚本库整理。
   - [x] 自动补齐策略与可操作范围确认。

2. **后端开发** ✅
   - [x] 完成 `PartitionSwitchInspectionService` 规则补齐（结构/索引/约束/权限/锁等）。
     - [x] 支持目标数据库解析、跨实例判定，并在缺失目标表时追加自动补齐建议。
     - [x] 拦截目标表触发器与外键依赖，提供阻塞提示。
     - [x] 检查源/目标表 ALTER 权限与源表锁占用，输出阻塞或提示信息。
     - [x] 覆盖索引、约束、权限、锁冲突等 Blocker 逻辑，并输出详细处理建议。
  - [x] 定义 `PartitionSwitchPlan` 数据结构，区分 Blocker/AutoFix/Warning（含自动补齐执行脚本与影响面描述）。
  - [x] 扩展 `IPartitionSwitchAppService`：`InspectAsync`、`AutoFixAsync`、`ExecuteSwitchAsync`。
    - [x] `InspectAsync` 支持目标库字段映射并返回 `AutoFixSteps`。
    - [x] `AutoFixAsync` 串联自动补齐执行，返回执行明细与汇总日志。
    - [x] `ExecuteSwitchAsync` 关于自动补齐回滚记录及日志输出已完善。
  - [x] 基础设施实现自动补齐执行器（创建表/分区结构/索引/约束、清理数据、同步统计）。
     - [x] `CreateTargetTable` - 创建目标表并支持回滚删除
     - [x] `SyncPartitionObjects` - 同步分区函数与分区方案
     - [x] `SyncIndexes` - 同步非聚集索引并支持回滚删除
     - [x] `SyncConstraints` - 同步 CHECK/DEFAULT 约束并支持回滚删除
     - [x] `CleanupResidualData` - 清空目标表残留数据
     - [x] `RefreshStatistics` - 刷新统计信息
   - [x] 强化 `SqlPartitionCommandExecutor.ExecuteSwitchWithTransactionAsync`，支持多阶段日志与异常回滚。
     - [x] SWITCH 脚本生成支持 `[database].[schema].[table]` 全限定名。
     - [x] 增补执行链路的阶段日志与异常分类，记录到后台任务日志。
   - [x] API 端点已确认（`/api/v1/partition-archive/switch/autofix`）。

3. **前端开发**
   - [ ] 创建 `PartitionArchiveWizard.razor` 组件框架（Steps + Drawer + 状态管理）。
   - [ ] Step 1：方案选择（禁用 BCP/BulkCopy，提示原因）。
   - [ ] Step 2：参数配置表单（源/目标选择、校验提示）。
   - [ ] Step 3：预检结果页面（分级展示 + 勾选自动补齐 + 警告确认）。
   - [ ] Step 4：执行确认（Markdown 预览 + 勾选确认 + 提交 API）。

### P1（P0 完成后立即跟进）
- [ ] 集成测试：成功切换、结构不匹配、索引缺失、目标表非空、跨实例等场景。
- [ ] 任务监控页面增强：展示归档目标、预检结果快照、自动补齐执行详情。
- [ ] 更新文档：
  - `分区边界值明细管理功能规划设计.md` 增补归档流程细节；
  - 新增用户操作手册章节（含截图）；
  - FAQ 汇总常见失败原因与处理方式。

### P2（规划阶段）
- [ ] 记录用户选择 BCP/BulkCopy 的需求参数，为后续方案提供数据。
- [ ] 设计切换失败后的回滚脚本生成能力（撤回目标、恢复源）。
- [ ] 评估归档执行的性能监控与告警需求。

---

## 4. 当前进度回顾（2025-10-28）

**P0 后端能力已全部完成** ✅

- **目标库解析**: 应用服务、DTO、执行器已支持显式 `TargetDatabase`，并在检查阶段校验跨实例场景。
- **自动补齐建议**: 当目标表缺失时返回 `CreateTargetTable` AutoFixStep，并完整实现 6 种自动补齐步骤。
- **AutoFix 执行器**: 完整实现索引、约束、分区对象、表创建、数据清理、统计刷新等全链路自动补齐。
- **回滚机制**: 栈式补偿设计，支持表创建、索引同步、约束同步的失败回滚，确保数据库状态可恢复。
- **SQL 生成**: `SwitchOut.sql` 模板与执行器支持输出跨库全限定名的 SWITCH 脚本。
- **错误分类**: `ExecuteSwitchWithTransactionAsync` 实现三阶段日志输出，并对常见 SQL 错误码(1205/1222/2627/547/4981)进行中文分类描述。
- **测试覆盖**: `PartitionSwitchAppServiceTests`、`PartitionCommandTests` 已更新通过，验证目标库字段与脚本渲染逻辑。
- **API 端点**: 已确认 `/api/v1/partition-archive/switch/{inspect,autofix}` 端点存在且 DTO 包含 `TargetDatabase` 字段。

> 下一步优先事项：启动前端向导开发（Step 1-4），实现预检结果展示与 AutoFix 勾选交互，并补充集成测试覆盖。