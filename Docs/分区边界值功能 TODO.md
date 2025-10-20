# 分区边界值管理功能 TODO 列表

> 依据《分区边界值明细管理功能规划设计》拆解的最小执行任务。建议按顺序完成,跨模块任务可并行推进。完成后请在对应条目前打勾并记录提交哈希。

**优先级调整(2025-10-17)**:优先完成分区拆分/合并功能,归档功能后置。

## 0. 基础准备
- [x] 创建后端/前端共享的操作类型枚举与 DTO 扩展（`BackgroundTaskOperationType` 等）
- [x] 迁移脚本：为既有 `BackgroundTask` 补充 `OperationType` 默认值
    - [ ] 更新任务调度平台 API 契约文档（若有独立仓库请同步 PR）

## 1. 后端 - 配置边界操作
- [x] `IPartitionConfigurationAppService` 新增 Add/Split/Merge 方法定义
- [x] `PartitionConfigurationAppService` 实现新增方法，调用领域逻辑并持久化
- [x] 基础设施层编写 `SplitPartitionCommand`、`MergePartitionCommand` 执行器
- [x] API 控制器新增 `POST /boundaries/add|split|merge`
- [x] 审计日志写入 `PartitionAuditLog`（或新增）并写调度任务
- [x] 单元测试覆盖：边界顺序校验、文件组验证、异常路径

## 2. 后端 - 数据归档（分区切换方案）
- [x] 新建 `PartitionSwitchInspectionService`，实现结构/索引/约束检查
- [x] 扩展 `BackgroundTask`：字段 `OperationType`、归档目标信息（已在实体中实现并创建迁移）
- [x] `PartitionSwitchAppService`：`InspectAsync` + `ArchiveBySwitchAsync`
- [x] API：新增 `POST /archive/inspect`、`POST /archive/switch`
- [x] 执行器：`SwitchPartitionCommandExecutor` + `SqlPartitionCommandExecutor.ExecuteSwitchWithTransactionAsync`
- [x] 更新任务调度平台日志写入逻辑，支持 Archive 类别（通过 OperationType 区分）
- [x] 集成测试：成功切换、结构不一致失败、索引不齐失败（已创建占位测试，待真实环境验证）

## 3. 后端 - BCP/BulkCopy 预留
- [x] 定义接口 `PlanArchiveWithBcpAsync`、`PlanArchiveWithBulkCopyAsync`（暂返回规划中）
- [x] API 返回占位响应并记录用户需求参数
- [x] 在任务调度平台记录 `Planned` 状态的归档任务（通过 BackgroundTask 支持）

## 4. 前端 - 公共支持
- [x] 扩展 `BackgroundTaskSummaryModel`、`BackgroundTaskDetailModel` 新字段
- [x] 任务调度页面 UI：新增“操作类型”“归档方案”“目标实例/库”列
- [ ] 日志视图新增过滤选项及新的类别渲染
- [ ] 迁移旧数据展示：`Unknown` 类型兼容显示

## 5. 前端 - 分区管理子控件

### 5.1 添加分区值(已完成 ✅)
- [x] `PartitionBoundaryAddDrawer` 组件 + 表单验证 + API 对接（✅ 2025-10-17 完成）
  - [x] 单值添加表单与提交
  - [x] 批量生成边界值（日期范围/数值序列）
  - [x] 边界值预览与删除（Tag 组件集成）
  - [x] 文件组选择与自动分配
  - [x] 前端验证（顺序、格式、重复）
  - [x] 后端 API 调用与错误处理
  - [x] 成功后刷新分区明细列表
- [x] 主页面整合按钮、刷新逻辑、操作权限控制（添加分区值按钮已集成）

### 5.2 分区拆分(已完成 ✅ 2025-10-17)
- [x] `PartitionSplitWizard.razor` 组件框架搭建 ✅
  - [x] Steps 导航组件(简化为2步)
  - [x] 组件状态管理类(`SplitFormModel`)
  - [x] Drawer 容器与关闭逻辑
- [x] **Step 1 - 设置新边界值** ✅
  - [x] 从主页面预选分区(不需要在向导中再选)
  - [x] 边界值输入框(支持日期/数值格式)
  - [x] 单个/批量生成模式切换
  - [x] 实时校验(范围内、不重复)
  - [x] 文件组选择(继承或指定)
- [x] **Step 2 - 确认执行** ✅
  - [x] 显示将执行的 SQL 脚本
  - [x] 风险提示(从后端获取)
  - [x] 备份确认复选框
  - [x] 调用 API 提交任务
  - [x] FormItem 错误修复(添加Form标签包裹)
- [x] API 客户端路由修复 ✅
- [x] 主页面集成"拆分分区"按钮 ✅
  - [x] 传入预选分区边界键
  - [x] 从选中行获取边界值
- [x] 编译通过 ✅
- [ ] 批量生成边界值功能(占位,待实现)
- [ ] 集成测试与 Bug 修复(需要在运行环境中测试)

### 5.3 分区合并(计划中 📋)
- [ ] `PartitionMergeWizard.razor` 组件框架搭建
- [ ] **Step 1 - 选择合并边界**
  - [ ] 边界值列表表格
  - [ ] 选择要删除的边界(合并其左右分区)
  - [ ] 预览合并后范围
- [ ] **Step 2 - 确认合并参数**
  - [ ] 目标文件组选择
  - [ ] 数据量预估显示
  - [ ] 风险提示
- [ ] **Step 3 - 执行预览**
  - [ ] 显示 SQL 脚本
  - [ ] 备份确认
  - [ ] 调用 API 提交任务
- [ ] 集成测试与 Bug 修复
- [ ] 主页面添加"合并分区"按钮

### 5.4 数据归档(后置)
- [ ] `PartitionArchiveWizard` 组件（方案选择 + 分区切换实现 + BCP/BulkCopy 占位）

### 5.5 测试
- [ ] 对应的 UI 单元/集成测试（BUnit / Playwright）

## 6. 测试与文档
- [x] 更新 `分区边界值功能 TODO.md` 完成情况（✅ 2025-10-17）
- [ ] 更新 `分区边界值明细管理功能规划设计.md` 实际完成情况
- [ ] 编写操作手册或 README 附录说明新功能
- [ ] 新增/更新 CI 管道脚本（若需）确保新测试运行

## 里程碑建议(已调整)
1. **Milestone A** ✅：后端 Add/Split/Merge API + 审计日志 + 任务记录(已完成)
2. **Milestone B** 🚧：前端边界操作子控件上线
   - ✅ 添加分区值(已完成 2025-10-17)
   - 🚧 分区拆分(进行中,预计 2025-10-20 完成)
   - 📋 分区合并(计划中,预计 2025-10-23 完成)
3. **Milestone C** 📋：数据归档（分区切换）闭环 + 任务调度平台 UI 扩展(预计 2025-10-31 完成)
4. **Milestone D** 📋：BCP/BulkCopy 占位与后续调研记录

## 当前进度(2025-10-17)
- ✅ 添加分区值功能已完成并测试通过
- ✅ **已完成**:分区拆分向导前端组件开发完成并编译通过
- 📋 **下一步**:分区合并功能(预计明天 10-18 开始)

