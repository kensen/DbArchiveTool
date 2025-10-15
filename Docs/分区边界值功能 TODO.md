# 分区边界值管理功能 TODO 列表

> 依据《分区边界值明细管理功能规划设计》拆解的最小执行任务。建议按顺序完成，跨模块任务可并行推进。完成后请在对应条目前打勾并记录提交哈希。

## 0. 基础准备
- [x] 创建后端/前端共享的操作类型枚举与 DTO 扩展（`PartitionExecutionOperationType` 等）
- [x] 迁移脚本：为既有 `PartitionExecutionTask` 补充 `OperationType` 默认值
    - [ ] 更新任务调度平台 API 契约文档（若有独立仓库请同步 PR）

## 1. 后端 - 配置边界操作
- [x] `IPartitionConfigurationAppService` 新增 Add/Split/Merge 方法定义
- [x] `PartitionConfigurationAppService` 实现新增方法，调用领域逻辑并持久化
- [x] 基础设施层编写 `SplitPartitionCommand`、`MergePartitionCommand` 执行器
- [x] API 控制器新增 `POST /boundaries/add|split|merge`
- [ ] 审计日志写入 `PartitionAuditLog`（或新增）并写调度任务
- [ ] 单元测试覆盖：边界顺序校验、文件组验证、异常路径

## 2. 后端 - 数据归档（分区切换方案）
- [ ] 新建 `PartitionSwitchInspectionService`，实现结构/索引/约束检查
- [ ] 扩展 `PartitionExecutionTask`：字段 `OperationType`、归档目标信息
- [ ] `PartitionSwitchAppService`：`InspectAsync` + `ArchiveBySwitchAsync`
- [ ] API：新增 `POST /archive/inspect`、`POST /archive/switch`
- [ ] 执行器：复用/扩展 `SqlPartitionCommandExecutor` 支持 `SwitchPartitionCommand`
- [ ] 更新任务调度平台日志写入逻辑，支持 Archive 类别
- [ ] 集成测试：成功切换、结构不一致失败、索引不齐失败

## 3. 后端 - BCP/BulkCopy 预留
- [x] 定义接口 `PlanArchiveWithBcpAsync`、`PlanArchiveWithBulkCopyAsync`（暂返回规划中）
- [x] API 返回占位响应并记录用户需求参数
- [ ] 在任务调度平台记录 `Planned` 状态的归档任务

## 4. 前端 - 公共支持
- [x] 扩展 `PartitionExecutionTaskSummaryModel`、`PartitionExecutionTaskDetailModel` 新字段
- [x] 任务调度页面 UI：新增“操作类型”“归档方案”“目标实例/库”列
- [ ] 日志视图新增过滤选项及新的类别渲染
- [ ] 迁移旧数据展示：`Unknown` 类型兼容显示

## 5. 前端 - 分区管理子控件
- [ ] `PartitionBoundaryAddDialog` 组件 + 表单验证 + API 对接
- [ ] `PartitionSplitWizard` 组件（Step UI、预览、执行调用）
- [ ] `PartitionMergeWizard` 组件
- [ ] `PartitionArchiveWizard` 组件（方案选择 + 分区切换实现 + BCP/BulkCopy 占位）
- [ ] 主页面整合按钮、刷新逻辑、操作权限控制
- [ ] 对应的 UI 单元/集成测试（BUnit / Playwright）

## 6. 测试与文档
- [ ] 更新 `分区边界值明细管理功能规划设计.md` 实际完成情况
- [ ] 编写操作手册或 README 附录说明新功能
- [ ] 新增/更新 CI 管道脚本（若需）确保新测试运行

## 里程碑建议
1. **Milestone A**：后端 Add/Split/Merge API + 审计日志 + 任务记录
2. **Milestone B**：前端三个边界操作子控件上线
3. **Milestone C**：数据归档（分区切换）闭环 + 任务调度平台 UI 扩展
4. **Milestone D**：BCP/BulkCopy 占位与后续调研记录
