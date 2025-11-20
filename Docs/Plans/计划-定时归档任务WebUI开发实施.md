# 定时归档任务 Web UI 开发实施计划

> 创建时间: 2025-11-20  
> 项目: DbArchiveTool - 定时归档任务前端开发  
> 策略: 优先实现创建功能，最后完成监控页面

## 1. 开发策略

### 核心原则
- **由简到繁**：从基础组件到复杂交互
- **增量交付**：每个阶段都能产出可演示的功能
- **测试驱动**：关键组件开发完成即测试
- **文档同步**：代码完成同步更新中文注释

### 优先级排序
1. **P0（必须）**：数据源入口 → 任务列表页 → 创建/编辑表单 → 基础 CRUD
2. **P1（重要）**：任务详情页 → 立即执行 → 执行历史
3. **P2（优化）**：监控页面改造 → 自动刷新 → 统计卡片
4. **P3（扩展）**：批量操作 → 性能优化 → 错误处理增强

---

## 2. 开发阶段规划

### ✅ Stage 1: 基础设施搭建（1-2天）**已完成**
**目标**：建立开发基础，确保后续工作顺畅

#### Task 1.1: API 客户端实现
- [x] 创建 `ScheduledArchiveJobApiClient.cs`
  - 位置: `src/DbArchiveTool.Web/Services/`
  - 实现方法: GetListAsync, GetByIdAsync, CreateAsync, UpdateAsync, DeleteAsync
  - 实现方法: EnableAsync, DisableAsync, ExecuteAsync, GetStatisticsAsync, GetExecutionHistoryAsync
  - 遵循现有 ApiClient 模式（参考 `ArchiveDataSourceApiClient`）
  - 所有方法添加中文 XML 注释
  - 已解决 PagedResult 命名冲突

#### Task 1.2: DTO 定义
- [x] 创建前端 DTO 模型
  - 位置: `src/DbArchiveTool.Web/Models/ScheduledArchiveJobModels.cs`
  - `ScheduledArchiveJobDto` - 展示用（含便利字段）
  - `CreateScheduledArchiveJobRequest` - 创建用
  - `UpdateScheduledArchiveJobRequest` - 更新用
  - `TableValidationResponse` - 表验证响应
  - `ColumnInfo` - 列信息
  - `ScheduledArchiveJobStatisticsDto` - 统计信息
  - `JobExecutionHistoryDto` - 执行历史
  - `JobExecutionStatus` - 执行状态枚举

#### Task 1.3: 状态管理服务
- [x] 创建 `ScheduledArchiveJobState.cs`
  - 位置: `src/DbArchiveTool.Web/Services/`
  - 缓存当前数据源的任务列表（30秒有效期）
  - 提供刷新、清空缓存方法
  - Scoped 生命周期注册
  - 状态变更事件通知

#### Task 1.4: 依赖注册
- [x] 在 `Program.cs` 中注册服务
  ```csharp
  builder.Services.AddHttpClient<ScheduledArchiveJobApiClient>(configureClient);
  builder.Services.AddScoped<ScheduledArchiveJobState>();
  ```

**验收标准**：
- ✅ API 客户端能正常调用后端接口（9个方法完整实现）
- ✅ DTO 与后端接口契约匹配（使用 Result<T> 模式）
- ✅ 所有代码包含中文注释（完整 XML 文档注释）
- ✅ 编译成功无错误

---

### ✅ Stage 2: 数据源入口改造（0.5天）**已完成**
**目标**：在数据源卡片添加"定时归档配置"入口

#### Task 2.1: 修改数据源卡片
- [x] 编辑 `src/DbArchiveTool.Web/Pages/Index.razor`
  - 操作栏添加"定时归档配置"按钮
  - 布局: [编辑] | [分区管理] | [定时归档配置] | [更多]
  - 点击跳转: `/archive-data-sources/{id}/scheduled-jobs`
  - 添加导航方法 `NavigateToScheduledJobs`

#### Task 2.2: 样式调整
- [x] 复用现有 Link 按钮样式
  - 与其他按钮保持一致
  - 使用 Ant Design Link Button

**验收标准**：
- ✅ 数据源卡片显示新按钮
- ✅ 点击能正确跳转到任务列表页
- ✅ 按钮样式与其他按钮一致

---

### ✅ Stage 3: 任务列表页（2-3天）**已完成**
**目标**：实现单数据源的定时任务列表查看

#### Task 3.1: 页面骨架
- [x] 创建 `src/DbArchiveTool.Web/Pages/ScheduledJobs/Index.razor`
  - 路由: `@page "/archive-data-sources/{DataSourceId:guid}/scheduled-jobs"`
  - 页面参数: `[Parameter] public Guid DataSourceId { get; set; }`
  - PageHeader 带返回按钮

#### Task 3.2: 顶部信息条
- [x] 显示数据源信息
  - 数据源名称
  - 从 API 加载数据源列表查找

#### Task 3.3: 统计卡片
- [x] 实现 4 个统计卡片组件
  - 任务总数、启用任务、今日执行、今日成功率
  - 使用 Ant Design `<Statistic>` 组件
  - 数据来源: GetStatisticsAsync API

#### Task 3.4: 筛选器
- [x] 状态单选（全部、已启用、已禁用）
- [x] 关键字搜索（任务名称、源表、目标表）
- [x] 筛选条件变化自动刷新列表

#### Task 3.5: 任务表格
- [x] 表格列定义（9列）
  - 任务名称（链接到详情）、源表、目标表、执行频率、下次执行、最近状态、最近归档行数、状态、操作
- [x] 状态标签（颜色标识）
  - 成功（绿）、运行中（蓝）、失败（红）、跳过（灰）
- [x] Badge 显示启用/禁用状态
- [x] 调度列显示 Cron 中文描述或间隔分钟

#### Task 3.6: 操作列
- [x] 详情按钮（跳转详情页，路由已预留）
- [x] 编辑按钮（跳转编辑页，路由已预留）
- [x] 启用/禁用按钮（调用 EnableAsync/DisableAsync API）
- [x] 立即执行按钮（调用 ExecuteAsync API）
- [x] 删除按钮（Popconfirm 二次确认 + DeleteAsync API）

#### Task 3.7: 顶部操作区
- [x] [刷新] 按钮
- [x] [新建任务] 按钮（跳转创建页）

#### Task 3.8: 分页
- [x] 前端分页（使用 Table 内置分页）
- [x] 默认 20 条/页
- [x] 支持页码和每页条数切换

#### Task 3.9: 状态管理集成
- [x] 使用 ScheduledArchiveJobState 缓存
- [x] 订阅状态变更事件自动刷新
- [x] 操作后本地更新缓存

#### Task 3.10: 样式文件
- [x] 创建 `Index.razor.css`
- [x] 定义统计卡片、筛选器布局样式

**验收标准**：
- ✅ 列表页能正常显示任务数据
- ✅ 筛选器工作正常（状态筛选 + 关键字搜索）
- ✅ 启用/禁用、立即执行功能可用
- ✅ 删除功能带二次确认
- ✅ 编译成功无错误

---

### 🔄 Stage 4: 创建/编辑表单（3-4天）**进行中 - 60%完成** ⭐核心组件
**目标**：实现完整的任务创建和编辑功能

#### Task 4.1: 页面路由 ✅
- [x] 创建页: `/archive-data-sources/{DataSourceId:guid}/scheduled-jobs/create` - **已完成**
- [ ] 编辑页: `/archive-data-sources/{DataSourceId:guid}/scheduled-jobs/{JobId:guid}/edit`

#### Task 4.2: 页面布局 ✅
- [x] 左右分栏布局（Ant Design `<Row>` + `<Col>`）- **已完成**
- [x] 左侧：表单区域（Col span=16）- **已完成**
- [x] 右侧：预览面板（Col span=8）- **已完成**

#### Task 4.3: 基本信息表单组 ✅
- [x] 任务名称（必填，2-100字符）- **已完成**
- [x] 任务描述（多行文本框）- **已完成**
- [x] 数据源信息（只读显示）- **已完成**

#### Task 4.4: 表选择组件 🔄 部分完成
- [ ] 创建 `TableSelector.razor` 组件 - **待实现**
- [x] 源表输入框（临时实现）- **已完成**
- [ ] 源表下拉（支持搜索，仅加载普通表）- **待实现**
  - 调用后端接口获取表列表
  - 自动过滤分区表
- [ ] 表验证逻辑 - **待实现**
  - 选择后检测是否为分区表
  - 若是分区表显示错误："定时归档仅支持普通表"
- [x] 目标表输入框 - **已完成**
  - 自动建议: `{SourceTable}_Archive` - **已实现**
  - 可手动编辑

#### Task 4.5: 筛选条件生成器组件 ✅ **已完成**
- [x] 创建 `FilterBuilder.razor` 组件（⭐ 核心组件）- **604行,已完成**
- [x] 筛选列选择 - **已完成**
  - 下拉加载列元数据（临时模拟数据,后续对接API）
  - 标注推荐字段（时间/日期字段,显示索引标签）
- [x] 动态条件渲染（根据列类型）- **已完成**
  - **日期时间**：
    - [x] 相对时间模式（推荐）：最近 N 天
    - [x] 绝对时间模式：日期范围选择器
    - [x] 大于/小于操作符
  - **数值类型**：
    - [x] 等于/不等于/大于/小于/范围
    - [x] 动态输入框渲染
  - **字符串**：
    - [x] 匹配模式（包含/开头/结尾/等于/不等于）
- [x] WHERE 子句预览 - **已完成**
  - [x] 实时生成标准 SQL WHERE 子句
  - [x] 代码块样式展示
  - [x] 复制按钮
  - [x] 预估影响行数（后台异步请求）
  - [x] 超100万行警告
- [x] 验证规则 - **部分完成**
  - [x] 必填验证（至少一个条件）
  - [x] 预估影响行数（调用后端 COUNT 接口 - 已集成,临时模拟）
  - [ ] 超过阈值二次确认

#### Task 4.6: 归档参数组 ✅ **已完成**
- [x] 归档方法（固定 BulkCopy，灰显）- **已完成**
- [x] 批次大小 - **已完成**
  - [x] 预设选项: 1000/5000/10000
  - [x] 自定义输入框
  - [x] 输入验证: 1-100000
  - [x] 推荐值提示
- [x] 最大行数 - **已完成**
  - [x] 默认 50000
  - [x] InputNumber 控件
  - [x] 说明文本

#### Task 4.7: 调度方式组件 ✅ **已完成**
- [x] 创建 `CronBuilder.razor` 组件（⭐ 核心组件）- **358行,已完成**
- [x] 单选：按间隔 / Cron 高级 - **已集成到Create.razor**
- [x] **按间隔模式**：
  - [x] 输入间隔分钟（>=1）
- [x] **Cron 高级模式**：
  - [x] 频率类型选择：分钟/小时/每日/每周/每月
  - [x] 根据类型渲染细节控件
    - [x] 分钟：每 N 分钟（1-59）
    - [x] 小时：每 N 小时的第 M 分钟
    - [x] 每日：时间选择器
    - [x] 每周：星期多选 + 时间
    - [x] 每月：日期多选（1-31）+ 时间
  - [x] 实时生成 Cron 表达式
  - [x] 中文语义描述："每周一、三、五 02:30 执行"
- [x] 使用 **Cronos** NuGet 包（v0.11.1）
  - [x] 验证表达式合法性
  - [x] 计算下次执行时间
  - [x] 显示"距离现在"时间
- [x] 表达式有效性标签（绿色/红色）
- [x] 复制 Cron 表达式按钮

#### Task 4.8: 安全与限制 ✅ **已完成**
- [x] 最大连续失败次数（默认 5）- **已完成**
- [x] 启用状态开关（创建后立即调度）- **已完成**

#### Task 4.9: 右侧预览面板 ✅ **已完成**
- [x] 任务摘要卡片 - **已完成**
  - [x] 名称、描述
  - [x] 源表 → 目标表
  - [x] 筛选条件（WHERE 子句）
  - [x] 调度语义（间隔/Cron）
  - [x] 启用状态
- [x] Sticky 定位（右侧滚动时保持可见）

#### Task 4.10: 提交区 ✅ **已完成**
- [x] 保存按钮（主按钮）- **已完成**
- [x] 取消按钮（返回列表）- **已完成**
- [x] 重置按钮（清空表单）- **已完成**
- [x] 表单校验逻辑 - **已完成**
  - [x] 前端验证所有必填项
  - 调用后端 API 创建/更新
  - 成功后跳转详情页或列表页

**验收标准**：
- ✅ 表单所有字段能正确填写
- ✅ 筛选条件生成器工作正常
- ✅ Cron 生成器生成正确表达式
- ✅ 表验证能正确阻止分区表
- ✅ 预览面板实时更新
- ✅ 创建/更新成功后跳转正确

---

### 📊 Stage 5: 任务详情页（2天）
**目标**：展示任务详细信息和执行历史

#### Task 5.1: 页面路由
- [ ] 创建 `src/DbArchiveTool.Web/Pages/ScheduledJobs/Details.razor`
- [ ] 路由: `@page "/scheduled-jobs/{JobId:guid}"`

#### Task 5.2: 基本信息区块
- [ ] 任务名称、描述、启用状态
- [ ] 数据源与表映射（源表→目标表）
- [ ] 标注"归档方法：BulkCopy"

#### Task 5.3: 筛选条件区块
- [ ] 高亮显示 WHERE 子句
- [ ] 标注字段类型和索引状态
- [ ] 性能提示（若无索引）

#### Task 5.4: 调度信息区块
- [ ] Cron 表达式 / 间隔分钟
- [ ] 下次执行时间
- [ ] 最近执行时间

#### Task 5.5: 累计统计卡片
- [ ] 总执行次数
- [ ] 总归档行数
- [ ] 平均每次行数
- [ ] 平均耗时
- [ ] 连续失败次数

#### Task 5.6: BulkCopy 执行详情
- [ ] 最近一次执行的详细信息
  - 批次循环次数
  - 单批次平均耗时
  - 数据传输速率（行/秒）
  - 是否触发最大行数限制

#### Task 5.7: 执行历史表格
- [ ] 表格列：开始时间、结束时间、耗时、归档行数、批次数、状态、错误摘要
- [ ] 最近 20 次记录
- [ ] 行展开查看错误详情
- [ ] 分页加载

#### Task 5.8: 操作按钮
- [ ] 立即执行（调用 API）
- [ ] 启用/禁用（切换开关）
- [ ] 编辑（跳转编辑页）
- [ ] 删除（二次确认）
- [ ] 回到列表

**验收标准**：
- ✅ 详情页能完整显示任务信息
- ✅ 执行历史加载正常
- ✅ BulkCopy 详情显示准确
- ✅ 所有操作按钮功能正常

---

### 🖥️ Stage 6: 监控页面改造（2天）
**目标**：优化现有 `/archive-jobs` 页面，专注定时任务监控

#### Task 6.1: 页面改造
- [ ] 编辑 `src/DbArchiveTool.Web/Pages/ArchiveJobs.razor`
- [ ] 修改页面标题："定时归档任务监控"
- [ ] 副标题："基于 Hangfire 的定时归档任务执行状态监控"

#### Task 6.2: 统计卡片
- [ ] 启用任务数
- [ ] 运行中任务数
- [ ] 今日执行次数
- [ ] 今日成功率

#### Task 6.3: 筛选器增强
- [ ] 数据源下拉（跨数据源查询）
- [ ] 状态多选
- [ ] 时间范围选择器
- [ ] 调度方式筛选（间隔/Cron）
- [ ] 自动刷新开关（默认关闭）

#### Task 6.4: 任务表格
- [ ] 复用列表页表格组件
- [ ] 支持跨数据源显示
- [ ] 添加数据源列

#### Task 6.5: 自动刷新机制
- [ ] 使用 `PeriodicTimer` 或 `System.Threading.Timer`
- [ ] 智能轮询：运行中任务 5秒，其他 30秒
- [ ] 开关控制启停

#### Task 6.6: URL 参数支持
- [ ] 支持 `?dataSourceId={id}` 预设筛选
- [ ] 筛选条件持久化到 URL

**验收标准**：
- ✅ 监控页能显示所有数据源的任务
- ✅ 筛选器工作正常
- ✅ 自动刷新功能可用
- ✅ URL 参数正确解析

---

### 🎨 Stage 7: 优化与增强（1-2天）
**目标**：完善用户体验和错误处理

#### Task 7.1: 错误处理
- [ ] 所有 API 调用包裹 try-catch
- [ ] 友好错误提示（Toast）
- [ ] 网络超时处理（重试按钮）
- [ ] ErrorBoundary 集成

#### Task 7.2: 加载状态
- [ ] 列表加载显示 Skeleton
- [ ] 按钮操作显示 Loading
- [ ] 长时间操作显示进度

#### Task 7.3: 性能优化
- [ ] 表格虚拟滚动（>50 行）
- [ ] 源表下拉惰性加载
- [ ] 列元数据缓存
- [ ] 使用 `@key` 优化渲染

#### Task 7.4: 可访问性
- [ ] 添加 aria-label
- [ ] 键盘导航支持
- [ ] 颜色+图标双冗余

#### Task 7.5: 国际化准备
- [ ] 文案集中到资源文件
- [ ] 预留多语言支持钩子

**验收标准**：
- ✅ 错误提示友好准确
- ✅ 加载状态清晰
- ✅ 性能表现良好（列表 < 1秒）
- ✅ 可访问性测试通过

---

### 🧪 Stage 8: 测试与文档（1天）
**目标**：确保质量，完善文档

#### Task 8.1: 功能测试
- [ ] 按照 17.1 节测试用例执行数据校验测试
- [ ] 按照 17.2 节测试用例执行 UI 交互测试
- [ ] 记录测试结果

#### Task 8.2: 端到端测试
- [ ] 创建普通表任务 → 自动执行 → 验证数据 → 监控 → 禁用
- [ ] 分区表验证阻止测试
- [ ] 筛选条件必填验证测试

#### Task 8.3: 代码审查
- [ ] 检查所有中文注释完整性
- [ ] 验证 DTO 与后端契约一致
- [ ] 确认错误处理覆盖全面

#### Task 8.4: 文档更新
- [ ] 更新 `数据模型与API规范.md`
- [ ] 生成变更总结文档（放入 `Docs/Changes/`）
- [ ] 更新 README（如需要）

**验收标准**：
- ✅ 所有测试用例通过
- ✅ 端到端流程顺畅
- ✅ 代码注释完整
- ✅ 文档更新完毕

---

## 3. 技术栈与依赖

### 前端框架
- Blazor Server (.NET 8)
- Ant Design Blazor（UI 组件库）
- SignalR（内置，用于实时通信）

### NuGet 包依赖
- `Cronos`（Cron 表达式解析和验证）
- `AntDesign`（已有）
- 其他现有依赖

### 后端 API 依赖
- 定时归档任务 CRUD 接口（已实现）
- 表元数据查询接口（需确认）
- 预估行数接口（需后端实现）
- 表验证接口（需后端实现）

---

## 4. 风险与应对

| 风险 | 影响 | 应对措施 |
|------|------|---------|
| 后端接口不完整 | 阻塞开发 | 提前与后端对齐，明确接口需求 |
| Cron 生成器复杂度高 | 延期 | 先实现简单模式，复杂模式分阶段 |
| 筛选条件生成器交互复杂 | 用户体验差 | 参考现有成熟组件设计，提供示例 |
| 表元数据加载慢 | 性能问题 | 实现惰性加载、搜索过滤、缓存 |
| 自动刷新性能影响 | 服务器压力 | 智能轮询，仅运行中任务高频刷新 |

---

## 5. 交付物清单

### 代码文件
- [ ] `ScheduledArchiveJobApiClient.cs`
- [ ] `ScheduledArchiveJobState.cs`
- [ ] DTO 模型文件（4-5个）
- [ ] `Index.razor`（数据源卡片修改）
- [ ] `ScheduledJobs/Index.razor`（任务列表页）
- [ ] `ScheduledJobs/Create.razor`（创建页）
- [ ] `ScheduledJobs/Edit.razor`（编辑页）
- [ ] `ScheduledJobs/Details.razor`（详情页）
- [ ] `Components/TableSelector.razor`
- [ ] `Components/FilterBuilder.razor`
- [ ] `Components/CronBuilder.razor`
- [ ] `ArchiveJobs.razor`（监控页改造）

### 文档文件
- [ ] 变更总结文档（`Docs/Changes/`）
- [ ] API 规范更新（`数据模型与API规范.md`）
- [ ] 测试报告（可选）

### 配置文件
- [ ] `Program.cs`（服务注册）
- [ ] `_Imports.razor`（命名空间注册）
- [ ] CSS 样式文件（如需要）

---

## 6. 时间估算

| 阶段 | 预估时间 | 说明 |
|------|---------|------|
| Stage 1: 基础设施 | 1-2天 | API 客户端、DTO、状态管理 |
| Stage 2: 数据源入口 | 0.5天 | 简单改造 |
| Stage 3: 任务列表页 | 2-3天 | 表格、筛选、操作 |
| Stage 4: 创建/编辑表单 | 3-4天 | 复杂表单组件 |
| Stage 5: 任务详情页 | 2天 | 展示和历史 |
| Stage 6: 监控页面 | 2天 | 页面改造 |
| Stage 7: 优化增强 | 1-2天 | 错误处理、性能 |
| Stage 8: 测试文档 | 1天 | 测试和文档 |
| **总计** | **12-16天** | 约 2-3 周 |

---

## 7. 开始开发

### 当前状态
- ✅ 后端 API 已完成（85% 整体进度）
- ✅ 设计文档已完成
- ✅ 开发计划已制定
- ✅ Stage 1: 基础设施搭建 **已完成**
- ✅ Stage 2: 数据源入口改造 **已完成**
- ✅ Stage 3: 任务列表页 **已完成**
- 🔄 **当前进行**: Stage 4 - 创建/编辑表单（60%完成）
  - ✅ Create.razor 页面框架
  - ✅ FilterBuilder 组件（⭐核心）
  - ✅ CronBuilder 组件（⭐核心）
  - ⏳ TableSelector 组件（待实现）
  - ⏳ Edit.razor 页面（待实现）

### 已完成交付物
- ✅ `ScheduledArchiveJobApiClient.cs` (355行)
- ✅ `ScheduledArchiveJobModels.cs` (270行)
- ✅ `ScheduledArchiveJobState.cs` (228行)
- ✅ `Program.cs` 服务注册
- ✅ `Pages/Index.razor` 数据源入口按钮
- ✅ `Pages/ScheduledJobs/Index.razor` 任务列表页 (365行)
- ✅ `Pages/ScheduledJobs/Index.razor.css` 样式文件
- ✅ `Pages/ScheduledJobs/Create.razor` 任务创建页 (424行)
- ✅ `Pages/ScheduledJobs/Create.razor.css` 样式文件
- ✅ `Components/FilterBuilder.razor` 筛选条件生成器（⭐核心，604行）
- ✅ `Components/FilterBuilder.razor.css` 样式文件
- ✅ `Components/CronBuilder.razor` Cron表达式生成器（⭐核心，358行）
- ✅ `Components/CronBuilder.razor.css` 样式文件
- ✅ Cronos NuGet 包集成（v0.11.1）

**总代码行数**: ~2,950行

### 下一步行动（Stage 4 剩余任务）
1. ⏳ 实现表选择组件（TableSelector.razor）
   - 表列表下拉（支持搜索）
   - 分区表检测和过滤
   - 集成到 Create.razor
2. ⏳ 完善 FilterBuilder
   - 对接后端表元数据 API（获取列信息）
   - 对接后端 COUNT 预估 API
3. ⏳ 创建 Edit.razor 页面
   - 复用 Create.razor 的表单结构
   - 预填充现有任务数据
   - UpdateAsync API 调用
4. 🎯 进入 Stage 5（任务详情页）

---

**持续实施中！** 🚀 **已完成 47.5%** (~3.8/8 阶段)
