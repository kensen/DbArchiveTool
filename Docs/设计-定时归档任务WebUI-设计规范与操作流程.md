# 定时归档任务 Web UI 设计规范与操作流程

> 版本: v1.0  \| 最近更新: 2025-11-20  \| 模块: ScheduledArchiveJob (定时归档任务)  \| 撰写: GitHub Copilot

## 1. 背景与目标
定时归档功能已在后端完成核心能力 (ScheduledArchiveJob 实体 + 调度/执行服务 + API)。当前缺失 Web 前端界面以支持用户可视化创建、管理、监控与优化归档任务。

**功能定位**：
- **适用场景**：仅针对**普通表**（非分区表）的自动化归档，代替传统数据库定时作业（SQL Server Job）。
- **归档方式**：固定使用 **BulkCopy** 方法执行批量数据迁移和删除。
- **执行模式**：长期运行的后台任务，按固定间隔（分钟/小时/天）持续归档符合条件的数据。
- **典型应用**：日志表、流水表、历史订单表等高频写入且需定期清理的业务表。

本文档定义前端信息架构、交互流程、组件规范、状态反馈与技术实现建议，确保：
1. 入口清晰：用户从"归档数据源"首页卡片直接进入当前数据源的定时任务配置界面。
2. 操作简单：不要求用户掌握 Cron 语法，通过结构化表单自动生成 Cron 表达式。
3. 筛选明确：必须基于时间/日期字段或业务主键字段定义归档范围，避免全表扫描。
4. 可观测性强：任务执行状态、归档行数、下次运行时间、失败统计等均可被快速浏览与筛选。
5. 一致性：遵循现有 UI 设计语言、命名与领域术语（归档、批次、任务）。

## 2. 术语与定义
- **归档任务（ScheduledArchiveJob）**：周期性执行归档的作业定义，仅适用于普通表。
- **普通表**：不使用分区的标准数据库表，通过 WHERE 条件筛选待归档数据。
- **BulkCopy 归档**：使用 SqlBulkCopy API 批量插入目标表后，再批量删除源表数据的归档方式。
- **批次大小（BatchSize）**：单次循环归档处理的最大行数，建议 1000-10000 行，避免长事务锁表。
- **最大行数（MaxRowsPerExecution）**：一次任务触发周期内累计归档的行数上限，防止单次执行时间过长。
- **归档筛选条件**：必须包含明确的 WHERE 子句，通常基于时间字段（如 CreateDate < DATEADD(day, -30, GETDATE())）或业务主键范围。
- **间隔分钟（IntervalMinutes）**：基于简单时间间隔的调度方式，与 Cron 二选一。
- **Cron 表达式**：高级调度方式，由表单帮用户生成，显示为只读预览可复制。
- **状态（Status）**：NotStarted / Running / Success / Failed / Skipped / Disabled。

**限制说明**：
- ⚠️ 定时归档不支持分区表（分区表归档请使用手动归档向导的分区切换功能）。
- ⚠️ 归档方法固定为 BulkCopy，不支持 BCP 或 PartitionSwitch。
- ⚠️ 必须提供有效的筛选条件，禁止全表归档（防止误操作）。

## 3. 信息架构 & 导航
```
首页 (归档数据源列表)
  └─ 数据源卡片 (新增入口: 定时归档配置)
       └─ 定时归档任务列表页 (当前数据源上下文)
             ├─ 新建任务按钮 → 新建/编辑任务页
             ├─ 任务行操作: 启用/禁用/立即执行/详情
             └─ [查看全部任务] → 跳转到全局监控页

定时归档任务监控页 (/archive-jobs) - 优化现有 Hangfire 监控页面
  └─ 仅展示定时归档任务 (ScheduledArchiveJob，基于 Hangfire)

手动归档任务 (ArchiveTask，基于 BackgroundTask)
  └─ 通过手动归档向导页面查看执行状态

其他后台任务 (BackgroundTask)
  └─ 通过各自功能页面查看（如分区维护页面）
```
导航方式：
1. **数据源卡片入口**：
   - 新增操作项"定时归档配置"（与"编辑""分区管理"并列放置）
   - 操作栏布局：[编辑] | [分区管理] | [定时归档配置]
   - 点击后跳转：`/archive-data-sources/{id}/scheduled-jobs`（当前数据源的定时任务列表）

2. **顶部导航菜单**：
   - 归档管理 → 归档任务 → `/archive-jobs`（默认显示"定时归档任务" Tab）
   - 从任何页面都可以快速访问全局任务监控

3. **URL 规范**（统一命名风格）：
   - 当前数据源任务列表：`/archive-data-sources/{id}/scheduled-jobs`
   - 新建任务：`/archive-data-sources/{id}/scheduled-jobs/create`
   - 编辑任务：`/archive-data-sources/{id}/scheduled-jobs/{jobId}/edit`
   - 任务详情：`/scheduled-jobs/{jobId}`
   - 全局监控（带Tab）：`/archive-jobs?tab=scheduled`（定时任务）
   - 全局监控（带筛选）：`/archive-jobs?tab=scheduled&dataSourceId={id}`

4. **面包屑导航**：
   - 数据源列表页：`归档数据源`
   - 定时任务列表页：`归档数据源 > {DataSourceName} > 定时归档任务`
   - 新建任务页：`归档数据源 > {DataSourceName} > 定时归档任务 > 新建任务`
   - 任务详情页：`归档任务 > {TaskName}`
   - 全局监控页：`归档管理 > 归档任务`

## 4. 页面概览
### 4.1 数据源卡片修改
- “更多”替换为文字操作项“定时归档配置”。
- 交互：悬停下划线，点击进入列表页。禁用条件：数据源未启用或连接测试失败（显示 Tooltip：请先启用并测试连接）。

### 4.2 定时归档任务列表页（数据源上下文）
**页面定位**：从数据源卡片"定时归档配置"进入，仅显示当前数据源的定时归档任务。

**页面结构**：
- **顶部信息条**：显示当前数据源连接信息（服务器、数据库、认证方式、连接状态）。
- **统计卡片**（简化版）：
  - 当前数据源任务总数
  - 启用任务数
  - 今日执行次数
  - 今日成功率（成功次数/总次数）
- **操作区**：
  - 左侧：筛选器（状态多选、目标表关键字搜索）
  - 右侧：[查看全部任务] 按钮（跳转到 `/archive-jobs?tab=scheduled&dataSourceId={id}`） | [刷新] | [新建任务]
- **任务表格**：
  | 名称 | 状态 | 源表→目标表 | 筛选条件（摘要） | 批次大小 | 调度 | 下次执行 | 最后归档行数 | 操作 |
  - 操作列：详情 | 编辑 | 启用/禁用 | 立即执行 | 删除
  - 筛选条件列：显示前50字符，鼠标悬停显示完整 WHERE 子句
  - 调度列：显示 Cron 描述或"每N分钟"
- **分页**：后端分页，默认 20 条/页。
- **状态展示**：使用颜色标签 + 图标
  - 🟢 启用运行中（绿色，动画 spinner）
  - 🟡 启用待执行（黄色）
  - ✅ 最近成功（绿色）
  - 🔴 失败（红色，显示失败次数 badge）
  - ⚫ 已禁用（灰色）

**与全局监控页的区别**：
- 列表页：单数据源上下文，有"新建任务"按钮，操作快捷
- 监控页：跨数据源聚合，无新建入口，侧重监控和故障排查

**与其他任务的区别**：
- 定时归档任务（ScheduledArchiveJob）：基于 **Hangfire** 实现，长期运行，周期性触发
- 手动归档任务（ArchiveTask）：基于 **BackgroundTask** 实现，一次性执行，通过手动归档向导创建
- 后台任务（如分区维护）：基于 **BackgroundTask** 实现，在各自功能页面管理

### 4.3 新建 / 编辑定时归档任务页
分区布局：左主表单 + 右侧即时预览面板。
表单分组：
1. 基本信息：任务名称、描述（多行）。
2. 数据源上下文：只读显示当前数据源信息。
3. 表选择：
   - 源表下拉（支持搜索，加载数据源的**所有普通表**，自动过滤分区表）。
   - 表验证：选择后自动检测是否为分区表，若是则显示错误提示："定时归档仅支持普通表，分区表请使用手动归档向导"。
   - 目标表自动建议：`{SourceTable}_archive` 可编辑。
   - 目标服务器：当前数据源（同数据源，后期支持跨源归档）。
4. 筛选条件生成器（⚠️ 必填，定时归档必须提供明确条件）：
   - **推荐方式**：优先使用时间/日期字段（如 CreateDate, UpdatedAt, OrderDate）定义归档范围。
   - 选择筛选列（下拉列名，显示类型，**标注推荐字段**）。
   - 根据类型动态渲染：
     - **日期时间**（推荐）：
       - 相对时间模式（推荐）：保留最近 N 天/小时的数据，归档更早的数据。
         - 示例：`CreateDate < DATEADD(day, -30, GETDATE())` → 归档30天前的数据
       - 绝对时间模式：指定起始、结束时间范围（日期+时间选择器）。
       - 快捷按钮：过去7天 / 过去30天 / 过去90天。
     - 数值类型：最小值、最大值（任一留空表示单边条件）；选择运算模式（范围 / 大于 / 小于）。
       - 示例：`Id < 1000000` → 归档ID小于100万的历史数据
     - 字符串：匹配模式（包含 / 开头 / 结尾 / 等于），输入框（较少使用）。
   - 预览 WHERE 子句（只读 + 复制按钮）。
   - **验证规则**：
     - 必须提供至少一个筛选条件，禁止留空（防止全表归档）。
     - 时间字段推荐使用相对时间表达式（如 DATEADD），确保持续有效。
     - 提交前预估影响行数（调用后端 `SELECT COUNT(*) WHERE ...`），超过阈值需二次确认。
   - 注：正则表达式支持列入扩展阶段(见第16节)，首版仅支持简单运算符。
5. 归档参数：
   - 归档方法：**固定为 BulkCopy**（灰显不可选，显示说明："定时归档统一使用 BulkCopy 方式"）。
   - 删除源数据：固定勾选（定时归档目的是清理历史数据，必须删除源表数据）。
   - 批次大小：输入或选择预设（**1000** / **5000** / **10000** / 自定义）。
     - 说明：单次循环处理的行数，过大会导致长事务锁表，过小会降低效率。
     - 推荐值：普通业务表 5000 行，高并发表（如日志表）1000-2000 行。
   - 最大行数：单次执行累计上限（默认 50000，可调整）。
     - 说明：防止单次执行时间过长，建议设置为批次大小的 5-20 倍。
     - 示例：批次5000行 × 10次循环 = 单次最多归档50000行。
6. 调度方式：单选"按间隔"或"Cron 高级"。
   - 间隔：输入间隔分钟（>=1），自动生成 Cron 表达式预览（例如 5 → `*/5 * * * *`）。
   - Cron 高级：
     - 频率类型：分钟 / 小时 / 每日 / 每周 / 每月。
     - 细节：
       - 分钟：每 N 分钟。
       - 小时：每 N 小时的第 M 分钟。
       - 每日：每天 HH:mm。
       - 每周：选择星期集合 + 时间。
       - 每月：选择日期（1-31或"最后一天"）+ 时间。
     - 实时生成 Cron + 语义说明："每周一、三、五 02:30 执行"。
   - 技术实现：使用 **Cronos** NuGet 包验证和计算下次执行时间：
     ```csharp
     using Cronos;
     // 验证表达式
     var expression = CronExpression.Parse(cronString, CronFormat.Standard);
     // 计算下次执行时间
     var nextOccurrence = expression.GetNextOccurrence(DateTime.UtcNow, TimeZoneInfo.Local);
     ```
7. 安全与限制：最大连续失败次数（默认 5，可配置）、启用状态（勾选表示创建后立即调度）。
8. 提交区：保存、取消、重置、测试表达式（前端模拟下一次执行时间）。
右侧预览：显示当前组合的任务摘要（名称、源→目标、筛选 WHERE、调度语义、预计下次执行时间）。
   - **归档范围预估**：调用后端接口 `SELECT COUNT(*) FROM {源表} WHERE {筛选条件}` 显示当前符合条件的行数。
   - **风险提示**：若预估行数 > 100万，显示警告："数据量较大，建议缩小归档范围或增加执行频率"。

### 4.4 任务详情页
内容区块：
1. 基本信息（名称、描述、启用状态）。
2. 数据源与表映射（源表→目标表，标注"归档方法：BulkCopy"）。
3. 筛选条件（高亮 WHERE 子句，标注字段类型和索引状态）。
   - 性能提示：若筛选字段无索引，显示警告："建议在 {列名} 上创建索引以提升归档性能"。
4. 调度信息（Cron、间隔、下次执行、最近执行）。
5. 累计统计：总执行次数、总归档行数、平均每次行数、平均耗时、连续失败次数。
6. **BulkCopy 执行详情**（最近一次执行）：
   - 批次循环次数（已执行 N 个批次）
   - 单批次平均耗时
   - 数据传输速率（行/秒）
   - 是否触发最大行数限制（若是，提示："本周期达到最大行数限制，剩余数据将在下次执行时继续归档"）
7. 最近执行记录表格（最近 20 次）：开始时间、结束时间、耗时、归档行数、批次数、状态、错误摘要（展开查看）。
8. 操作：立即执行、启用/禁用、编辑、删除、回到列表。
9. 扩展：后续可加入图表（执行频率折线、行数直方图、失败趋势）。

### 4.5 归档任务监控页（优化现有 /archive-jobs）
`/archive-jobs` 页面原本展示 Hangfire 后台任务监控。**定时归档任务基于 Hangfire 实现**，而手动归档任务和其他后台任务基于 `BackgroundTask` 实现（与 Hangfire 无关）。

**明确职责分工**：
- `/archive-jobs` → **仅展示 Hangfire 定时归档任务**（ScheduledArchiveJob）
- 手动归档任务（ArchiveTask）→ 通过手动归档向导页面查看状态
- 其他后台任务（BackgroundTask）→ 通过各自功能页面查看（如分区维护页面）

**页面改造策略**：

1. **单一职责**（推荐方案）：
   ```razor
   <!-- /archive-jobs 页面专注于定时归档任务 -->
   <PageHeader Title="定时归档任务监控" 
               SubTitle="基于 Hangfire 的定时归档任务执行状态监控" />
   
   <Space Direction="SpaceDirection.Vertical" Size="large">
       <!-- 统计卡片 -->
       <StatisticCards />
       
       <!-- 筛选器 -->
       <Filters>
           <DataSourceSelect />
           <StatusSelect />
           <TimeRangeSelect />
           <AutoRefreshSwitch />
       </Filters>
       
       <!-- 任务列表 -->
       <ScheduledJobsTable />
   </Space>
   ```

2. **统计卡片**（定时归档任务专属）：
   ```
   ┌─────────────┬─────────────┬─────────────┬─────────────┐
   │ 启用任务数  │  运行中任务 │ 今日执行次数│  今日成功率 │
   │    15       │      3      │     128     │    95.3%    │
   └─────────────┴─────────────┴─────────────┴─────────────┘
   ```

3. **筛选器**：
   - 数据源下拉（支持跨数据源查询，全部/按数据源筛选）
   - 状态多选（启用、禁用、运行中、失败）
   - 时间范围（最近执行时间、创建时间）
   - 调度方式（间隔/Cron）
   - 刷新按钮 + 自动刷新开关（默认关闭，开启后每30秒刷新）

4. **表格列定义**：
   | 任务名称 | 数据源 | 源表→目标表 | 筛选条件（摘要） | 批次大小 | 调度 | 状态 | 下次执行 | 最后执行 | 最后归档行数 | 操作 |
   
   - **调度列**：显示 Cron 中文描述（"每5分钟"、"每天凌晨2点"）或间隔分钟
   - **状态列**：
     - 🟢 启用运行中（绿色 + 动画 spinner）
     - 🟡 启用待执行（黄色）
     - ✅ 最近成功（绿色）
     - 🔴 失败（红色 + 失败次数 badge："连续失败 3 次"）
     - ⚫ 已禁用（灰色）
   - **下次执行列**：相对时间（"2小时后"）或绝对时间（"明天 14:30"）

5. **操作列**：
   - 详情（查看执行历史）
   - 启用/禁用（切换开关）
   - 立即执行（次要按钮，执行中禁用）
   - 编辑（跳转到表单页）
   - 删除（二次确认）

6. **导航入口**：
   - **从数据源卡片进入**：
     - 点击"定时归档配置" → `/archive-data-sources/{id}/scheduled-jobs`（单数据源列表）
     - 点击"查看全部任务" → `/archive-jobs?dataSourceId={id}`（带筛选的监控页）
   - **从顶部菜单进入**：
     - 归档管理 → 定时归档任务 → `/archive-jobs`（全局监控）

7. **技术实现要点**：
   - 使用 `ScheduledArchiveJobApiClient` 获取任务列表
   - 支持 URL 参数 `?dataSourceId={id}` 预设数据源筛选
   - 自动刷新使用 `PeriodicTimer` 或 `System.Threading.Timer`
   - 表格支持虚拟滚动（任务数 > 50 时启用）
   - 状态变更（启用/禁用）使用乐观更新 + 失败回滚

8. **与其他页面的关系**：
   - **手动归档任务**：通过"手动归档向导"页面查看执行状态（基于 BackgroundTask）
   - **后台任务**（分区维护等）：通过各自功能页面查看（基于 BackgroundTask）
   - **Hangfire Dashboard**：可选提供链接到 Hangfire 原生监控界面（管理员用）

**优势**：
- ✅ 职责单一，专注于定时归档任务监控
- ✅ 复用 Hangfire 基础设施，无需额外调度器
- ✅ 避免与 BackgroundTask 任务混淆
- ✅ 清晰的任务分类：定时任务（Hangfire）vs 即时任务（BackgroundTask）

## 5. 组件设计规范
| 组件 | 说明 | 交互要点 |
|------|------|---------|
| 数据源卡片操作项 | 文本按钮“定时归档配置” | Hover 下划线，禁用时浅灰+Tooltip |
| 任务状态标签 | 彩色 Pill | 颜色 + 图标（成功✅ 运行中⏱ 失败❌ 禁用🚫 跳过➖） |
| 筛选条件生成器 | 动态表单 | 列类型驱动显示；即时 WHERE 预览自动更新 |
| Cron 生成器 | 多步骤表单 | 修改任一参数立即重算；语义描述区同步 |
| WHERE 预览框 | 只读文本域 | 复制按钮；长度>200折叠展开 |
| 执行历史表格 | 虚拟滚动（后续） | 行点击展开错误详情 |
| 分页器 | 通用 | 保持与现有样式统一 |
| 立即执行按钮 | 次要强调 | 执行中禁用并显示 loading spinner |

## 6. 关键交互流程
### 6.1 入口
用户在首页看到数据源卡片 → 点击“定时归档配置” → 后端以 DataSourceId 查询任务列表 → 渲染。
### 6.2 创建任务
1. 点击“新建任务”
2. 选择源表（触发加载列元数据）
3. 配置目标表（默认建议名，检测是否存在冲突）
4. 选择筛选列 + 条件（预览 WHERE）
5. 设置批次大小、最大行数
6. 选择调度方式（间隔或 Cron）
7. 设置最大连续失败数 & 启用状态
8. 提交：前端校验 → 调用 `POST /api/v1/scheduled-archive-jobs`
9. 成功：通知 + 跳转详情或返回列表（用户可配置）。
### 6.3 启停任务
列表页点击“启用/禁用” → PUT 更新状态 → 刷新行。
### 6.4 立即执行
调用 `POST /{id}/execute` → 行显示“运行中” + spinner → 完成后更新统计与状态。
### 6.5 查看详情
从列表页或监控页点击"详情" → 加载实体与执行历史（两个并行请求）→ 渲染页面。
### 6.6 跨页面导航
- 从数据源列表页点击"定时归档配置" → 进入单数据源任务列表页
- 从任务列表页点击"查看全部任务" → 进入全局监控页（带数据源筛选）
- 从全局监控页点击数据源筛选器 → 快速切换不同数据源的任务
- 手动归档任务和后台任务通过各自页面独立查看（不在 `/archive-jobs` 混合展示）

## 7. 表单字段与校验
| 字段 | 校验规则 | 错误反馈 |
|------|----------|---------|
| 名称 | 必填，长度 2-100，不允许仅数字 | 显示"请输入有效任务名称" |
| 源表 | 必选，且必须是普通表（非分区表） | "请选择普通表，分区表不支持定时归档" |
| 目标表 | 必填，长度 2-128，合法标识符 | "目标表名称不合法或过长" |
| 筛选条件 | **必填**，至少包含一个有效条件 | "定时归档必须提供筛选条件，禁止全表归档" |
| 筛选列 | 推荐使用时间/日期字段 | 提示："建议使用时间字段（如 CreateDate）定义归档范围" |
| 数值范围 | min <= max；均为空则忽略 | “请输入合法范围” |
| 日期范围 | start < end；UTC 转换成功 | “起止时间不合法” |
| 字符串关键词 | 长度 <=128；正则时尝试编译验证 | “关键字或正则无效” |
| BatchSize | >0 且 <= 100000 | “批次大小范围 1-100000” |
| MaxRowsPerExecution | >= BatchSize | “最大行数需 ≥ 批次大小” |
| IntervalMinutes | >=1（仅间隔模式） | “间隔需 ≥1 分钟” |
| CronExpression | Cron 解析库验证通过 | “Cron 表达式不合法” |
| MaxConsecutiveFailures | 1-20 | “请输入 1-20 之间的数字” |

前端可使用：正则 + 轻量解析（或利用后端校验返回详细错误）。

## 8. 状态与反馈规范
- 成功保存：绿色 Toast "任务已创建"。
- 运行中：状态标签 + 行 Loading；详情页顶部 Banner。
- 无数据归档：状态显示"跳过"，行数 0；提示气泡"本周期未匹配到数据"。
- 连续失败触发禁用：红色 Banner："任务已自动禁用，连续失败次数达到阈值"。
- 删除确认：二次弹窗（名称回显），输入名称确认（防误删）。
- 列表加载中：显示 Ant Design Skeleton 骨架屏（至少3行）。
- 立即执行提交中：按钮 Loading 状态 + "正在提交..." 文本。
- 网络超时：Toast "请求超时，请检查网络连接" + 重试按钮（3秒后自动消失）。
- 长时间运行任务：若后端支持进度推送，显示进度百分比；否则显示 Spin "正在执行..."。

## 9. 权限与安全
- **首版实现**：跳过权限控制，所有登录用户可访问定时归档功能。
- **后续扩展**（见第16节）：集成 RBAC 权限系统
  - 仅具备"归档管理"角色的用户展示入口（接口返回权限标记）。
  - 删除 / 立即执行需具备高级权限。按钮按权限隐藏或禁用 Tooltip。
- 防止批量执行滥用：单任务立即执行冷却时间（例如 30 秒）前端灰显（可选实现）。

## 10. 性能与加载策略
- 源表下拉：惰性加载 + 搜索过滤（限制前 200 条，继续滚动请求下一页）。
- 列元数据：选择源表时单次请求缓存。
- 任务列表：分页 API；支持状态筛选参数减少返回量。
- 执行历史：详情页单独请求，允许"加载更多"。
- 监控页：可选自动刷新（定时轮询，使用指数退避避免高峰）。
- **Blazor Server 特定优化**：
  - 列表数据使用 `@bind-Value` 减少不必要的重新渲染。
  - 大表格（>50行）使用虚拟化组件：`<Virtualize Items="@jobs" Context="job">`。
  - 避免在 `@foreach` 循环中调用 `StateHasChanged()`，改为批量更新后统一调用。
  - 监控页自动刷新使用 `System.Threading.Timer` 而非轮询 API（减少 SignalR 连接压力）。
  - 长列表使用 `@key` 指令优化 Diff 算法：`@foreach (var job in jobs) { <div @key="job.Id">...</div> }`。

## 11. 可访问性 & 国际化
- 所有交互按钮添加 aria-label（例如“立即执行定时归档任务 {Name}”）。
- 颜色+图标双冗余表达状态（保证色盲可用）。
- Tooltip 提供键盘触发（焦点 + Enter）。
- 文案集中在资源文件，后续支持多语言；暂保留中文默认。

## 12. 错误与异常处理
类型划分：
1. 表单校验错误：就地红色提示，阻止提交。
2. 后端业务错误：Toast + 保留用户输入，不清空表单。
3. 网络异常：展示重试按钮，自动记录失败时间。
4. Cron 生成失败：标注源字段红框，给出修复建议。
统一错误结构：`{ code, message, details }`。前端映射友好提示，未知错误显示"操作失败，请稍后再试，并联系管理员"。

**Blazor ErrorBoundary 集成**：
每个主页面包裹 `<ErrorBoundary>` 组件防止整体崩溃：
```razor
<ErrorBoundary>
    <ChildContent>
        <!-- 实际页面内容 -->
    </ChildContent>
    <ErrorContent Context="ex">
        <Alert Type="AlertType.Error" 
               Message="页面加载失败" 
               Description="@ex.Message" 
               ShowIcon />
        <Button OnClick="@(() => Navigation.NavigateTo("/"))">返回首页</Button>
    </ErrorContent>
</ErrorBoundary>
```

## 13. 技术实现建议（Blazor Server）
### 13.1 组件划分
- `ScheduledJobsList.razor` - 任务列表页
- `ScheduledJobForm.razor` - 新建/编辑表单
- `CronBuilder.razor` - Cron表达式生成器
- `FilterBuilder.razor` - 筛选条件生成器
- `JobDetails.razor` - 任务详情页
- `JobsMonitorDashboard.razor` - 监控总览页

### 13.2 复用现有组件
- **MarkdownViewer.razor**：复用于渲染任务执行日志的 Markdown 格式内容（执行历史错误详情）。
- **数据源卡片样式**：复用 `Index.razor` 的 `.data-source-card` CSS 类及布局结构。
- **BackgroundTaskApiClient**：参考其轮询任务状态的逻辑，用于"立即执行"后的状态跟踪。
- **通用分页组件**：保持与现有 `ArchiveJobs.razor` 一致的分页器样式。
- **表元数据加载逻辑**：复用 `PartitionArchiveWizard.razor` 的表结构查询和列类型判断逻辑。
- **分区表检测**：复用现有分区配置相关接口判断表是否为分区表，用于源表选择验证。

### 13.3 状态管理与API
- **状态管理**：
  - 列表页：使用 Scoped 服务 `ScheduledArchiveJobState` 缓存当前数据源的任务列表
  - 监控页：使用独立的 `ScheduledJobsMonitorState` 管理筛选条件和自动刷新状态
  - 页面切换时清空缓存，重新加载
- **API 调用**：
  - 定时归档任务：`ScheduledArchiveJobApiClient`（List / Create / Update / Execute / Enable / Disable / Delete / GetExecutionHistory / GetStatistics）
  - 手动归档任务：通过手动归档向导页面独立管理（基于 BackgroundTask）
  - 其他后台任务：通过各自功能页面独立管理（基于 BackgroundTask）
- **Cron 生成**：使用 **Cronos** NuGet 包；提交前再次校验后端。
- **并发控制**：立即执行与启停加互斥锁（前端按钮防重复点击 + `_isSubmitting` 状态标志）。
- **监控页优化**：
  - URL 参数解析：`NavigationManager.Uri` 提取 `dataSourceId` 参数预设筛选
  - 筛选条件持久化到 URL（支持收藏和分享链接）
  - 自动刷新使用 `PeriodicTimer`（.NET 6+）或 `System.Threading.Timer`
  - 状态轮询：仅在有运行中任务时启用高频刷新（5秒），否则降为30秒

## 14. API 端点映射
| 前端操作 | API | 方法 | 说明 |
|----------|-----|------|------|
| 列表查询 | `/api/v1/scheduled-archive-jobs?dataSourceId=...` | GET | 支持状态过滤 |
| 创建任务 | `/api/v1/scheduled-archive-jobs` | POST | 表单提交 |
| 更新任务 | `/api/v1/scheduled-archive-jobs/{id}` | PUT | 编辑保存 |
| 启用任务 | `/api/v1/scheduled-archive-jobs/{id}/enable` | POST | ✅ 后端已实现 |
| 禁用任务 | `/api/v1/scheduled-archive-jobs/{id}/disable` | POST | ✅ 后端已实现 |
| 启用/禁用切换 | `/api/v1/scheduled-archive-jobs/{id}/toggle` | POST | ⚠️ 待确认后端是否实现，若无则前端调用enable/disable |
| 立即执行 | `/api/v1/scheduled-archive-jobs/{id}/execute` | POST | ✅ 异步执行，后端已实现 |
| 单个详情 | `/api/v1/scheduled-archive-jobs/{id}` | GET | ✅ 基本信息 |
| 执行历史 | `/api/v1/scheduled-archive-jobs/{id}/executions` | GET | ⚠️ 待确认，可能需后端扩展 |
| 统计概览 | `/api/v1/scheduled-archive-jobs/{id}/statistics` | GET | ✅ 累计数据 |
| 删除任务 | `/api/v1/scheduled-archive-jobs/{id}` | DELETE | ✅ 软删除 |

**说明**：
- ✅ 标记为后端已实现（参考 Stage3 实现总结）
- ⚠️ 标记为需确认或扩展的端点，开发前需核实后端支持情况
- 若后端未实现 toggle 端点，前端逻辑：根据当前 `IsEnabled` 状态调用 enable 或 disable

## 15. 前端 DTO 建议
```csharp
/// <summary>
/// 定时归档任务DTO（用于列表和详情展示，仅支持普通表+BulkCopy）
/// </summary>
public record ScheduledArchiveJobDto(
    Guid Id,
    string Name,
    string? Description,
    Guid DataSourceId,
    string DataSourceName,           // 🆕 用于列表显示，避免二次查询
    string SourceSchemaName,
    string SourceTableName,
    string FullSourceTable,          // 🆕 格式化为 "dbo.Orders"，便于显示
    string TargetSchemaName,
    string TargetTableName,
    string FullTargetTable,          // 🆕 格式化为 "dbo.Orders_Archive"
    string ArchiveFilterColumn,
    string ArchiveFilterCondition,
    // ArchiveMethod 固定为 BulkCopy，前端无需此字段
    int BatchSize,
    int MaxRowsPerExecution,
    int? IntervalMinutes,
    string? CronExpression,
    string? CronDescription,         // 🆕 中文描述: "每5分钟执行一次" / "每天凌晨2点执行"
    bool IsEnabled,
    DateTime? NextExecutionAtUtc,
    string? NextExecutionDisplay,    // 🆕 本地化显示: "2小时后" / "明天 02:00"
    DateTime? LastExecutionAtUtc,
    JobExecutionStatus LastExecutionStatus,
    string LastExecutionStatusDisplay, // 🆕 "运行中" / "成功" / "失败"
    long? LastArchivedRowCount,
    long TotalExecutionCount,
    long TotalArchivedRowCount,
    int ConsecutiveFailureCount,
    int MaxConsecutiveFailures,
    DateTime CreatedAtUtc,
    DateTime UpdatedAtUtc
);

/// <summary>
/// 创建定时归档任务请求（仅支持普通表 + BulkCopy）
/// </summary>
public record CreateScheduledArchiveJobRequest(
    string Name,
    string? Description,
    Guid DataSourceId,
    string SourceSchemaName,
    string SourceTableName,
    string TargetSchemaName,
    string TargetTableName,
    string ArchiveFilterColumn,          // 筛选列名，推荐时间字段
    string ArchiveFilterCondition,       // 筛选条件（WHERE 子句），必填
    // ArchiveMethod 固定为 BulkCopy，前端无需传递
    // DeleteSourceDataAfterArchive 固定为 true，前端无需传递
    int BatchSize,                       // 批次大小，推荐 1000-10000
    int MaxRowsPerExecution,             // 单次执行最大行数，推荐 50000
    int? IntervalMinutes,                // 间隔分钟（简单模式）
    string? CronExpression,              // Cron表达式（高级模式）
    bool IsEnabled,                      // 是否立即启用
    int MaxConsecutiveFailures           // 最大连续失败次数，默认 5
);

/// <summary>
/// 表验证响应（用于验证是否为普通表）
/// </summary>
public record TableValidationResponse(
    bool IsValid,                        // 是否为有效的普通表
    bool IsPartitionedTable,             // 是否为分区表
    string? ErrorMessage,                // 错误提示（若 IsValid=false）
    List<ColumnInfo>? Columns            // 表列信息（含类型、是否索引）
);

public record ColumnInfo(
    string Name,
    string DataType,
    bool IsIndexed,
    bool IsRecommendedForFilter          // 是否推荐用于筛选（时间字段、主键等）
);
```

**字段说明**：
- 🆕 标记为新增的便利字段，用于前端显示，减少二次计算和查询
- `CronDescription` 由后端根据表达式生成，或前端使用 Cronos 库本地生成
- `NextExecutionDisplay` 使用相对时间（"2小时后"）或绝对时间（"明天 14:30"）

## 16. 扩展留钩
### 16.1 当前版本不支持（明确排除）
- ❌ 分区表归档：定时归档仅支持普通表，分区表请使用手动归档向导。
- ❌ BCP 归档方式：定时归档固定使用 BulkCopy，BCP 方式仅在手动归档中支持。
- ❌ 不删除源数据：定时归档目的是清理历史数据，必须删除源表数据。
- ❌ 跨数据源归档：当前版本仅支持同数据源内归档（源库和目标库相同）。

### 16.2 后续扩展功能
- **正则表达式筛选**：字符串筛选条件支持正则表达式模式（需前端校验 + 后端安全验证）。
- **权限系统集成**：实现基于角色的访问控制（RBAC），区分"查看""创建""执行""删除"权限。
- **跨数据源归档**：支持将数据归档到独立的归档数据库服务器（需配置目标连接）。
- **归档前后钩子**：支持在归档前后执行自定义 SQL 脚本（如更新统计信息、重建索引）。
- **智能调度**：根据表增长速率自动调整执行频率和批次大小。
- **归档数据压缩**：目标表自动启用数据压缩（SQL Server 企业版特性）。
- 标签系统：任务可打标签（高优、冷热数据类别）。
- 实时推送：SignalR 推送执行状态变化，减少轮询。
- **批量操作**：支持批量启用/禁用/删除任务（勾选多行 + 操作按钮）。
- **任务克隆**：快速复制现有任务配置创建新任务。

## 17. 测试用例参考（前端）

### 17.1 数据校验测试
| 场景 | 期望 |
|------|------|
| 选择分区表作为源表 | 显示错误提示："定时归档仅支持普通表，分区表不支持" |
| 创建任务不填筛选条件 | 阻止提交，显示："定时归档必须提供筛选条件，禁止全表归档" |
| 使用时间字段相对条件 | WHERE 正确生成 `CreateDate < DATEADD(day, -30, GETDATE())` |
| 数值范围仅最小值 | WHERE 仅包含 `>=` 条件 |
| 日期快捷"过去30天" | 生成相对时间表达式，而非绝对时间 |
| 批次大小超过10万 | 显示警告："批次过大可能导致长事务锁表" |
| 最大行数小于批次大小 | 阻止提交："最大行数需 ≥ 批次大小" |
| Cron 周期每周多选 | 表达式与语义描述一致 |
| 连续失败模拟达阈值 | 自动禁用状态 + Banner |
| 列表筛选状态=失败 | 仅显示失败任务 |
| 删除任务确认错误名称 | 阻止删除 |

### 17.2 UI 交互测试
| 场景 | 期望 |
|------|------|
| 点击"新建任务"按钮 | 打开表单Modal，默认值正确填充（BatchSize=5000等） |
| 选择源表后切换到其他表 | 列元数据重新加载，筛选列下拉更新 |
| 修改间隔分钟为非法值（0或负数） | 即时显示红色错误提示，保存按钮禁用 |
| 保存成功后 | 关闭表单Modal，列表自动刷新，显示Toast"任务已创建" |
| 立即执行运行中任务再次点击 | 第二次点击被阻止（按钮禁用 + Tooltip提示） |
| 列表页长时间加载 | 显示Skeleton骨架屏，不阻塞页面交互 |
| 网络超时 | Toast提示 + 重试按钮，点击重试重新请求 |
| 任务详情页加载执行历史 | 分页加载，"加载更多"按钮正常工作 |
| 切换调度方式（间隔↔Cron） | 相关字段显示/隐藏，Cron预览实时更新 |

## 18. 里程碑与实施顺序
1. 入口与列表页骨架
2. API 客户端 & DTO 集成
3. 新建/编辑表单（基础字段 → 筛选构建器 → Cron 构建器）
4. 任务详情页 + 执行历史
5. 监控总览页改造
6. 权限/错误处理/可访问性增强
7. 优化性能与自动刷新
8. 编写前端测试（单元 + 少量集成）
9. 文档更新与用户培训

## 19. 成功判定标准
- 用户无需编写 Cron 即可完成调度设置（≥90% 任务采用表单生成）。
- 用户无需了解 BulkCopy 原理即可配置归档任务（归档方法固定，参数简化）。
- **表验证准确**：选择分区表时 100% 阻止并提示正确信息。
- **筛选条件必填**：无筛选条件的任务 100% 无法创建（防止全表误操作）。
- 列表页在 1 秒内加载（≤20 条记录）。
- 任务详情页含最近 20 次执行记录加载 ≤1.5 秒。
- 错误操作提示准确（≥10 个常见错误场景覆盖）。
- **BulkCopy 性能**：单批次 5000 行的归档耗时 ≤3 秒（普通业务表）。
- 端到端测试脚本通过：创建普通表任务 → 自动执行 → 验证数据迁移和删除 → 监控 → 禁用。

## 20. 风险与缓解
| 风险 | 说明 | 缓解 |
|------|------|------|
| **无筛选条件全表归档** | 误操作导致全表数据被归档删除 | 强制要求筛选条件 + 预估行数二次确认 |
| **分区表被误选** | 用户选择分区表创建定时任务导致失败 | 前端强验证 + 后端拒绝分区表任务 |
| **BulkCopy 长事务锁表** | 批次过大导致源表被长时间锁定 | 限制批次大小上限（≤10万行） + 警告提示 |
| **筛选字段无索引** | WHERE 条件查询慢影响归档性能 | 详情页检测并提示创建索引 |
| 表元数据加载慢 | 大库表多 | 增加搜索 + 分页 + 缓存 + 仅加载普通表 |
| Cron 语义复杂 | 用户不理解 | 语义描述 + 示例 + 预览下一次执行时间 |
| 立即执行频繁 | 影响服务器负载 | 限流 + 冷却时间 + 后端并发控制 |
| 执行历史数据膨胀 | 表膨胀与查询慢 | 后端分页 + 定期归档历史表 |

## 21. 开发注意事项

### 21.1 代码注释规范（严格遵循《开发规范与项目结构.md》第5.6节）
- **Razor 组件文件**：在 `@code` 块顶部添加中文注释块，说明组件职责、依赖服务、交互流程。
  ```razor
  @* 
   * 定时归档任务列表页
   * 职责：展示当前数据源的所有定时归档任务，支持筛选、启停、立即执行等操作
   * 依赖：ScheduledArchiveJobApiClient、MessageService、NavigationManager
   * 交互：点击"新建任务" → 跳转表单页；点击"详情" → 跳转详情页
   *@
  @code { ... }
  ```
- **ApiClient 方法**：必须添加中文 XML 文档注释（`/// <summary>`），说明用途、参数、返回值、异常情况。
  ```csharp
  /// <summary>
  /// 获取指定数据源的定时归档任务列表
  /// </summary>
  /// <param name="dataSourceId">数据源ID</param>
  /// <param name="isEnabled">是否仅查询启用任务，null表示查询全部</param>
  /// <returns>任务列表，失败时返回错误信息</returns>
  public async Task<Result<List<ScheduledArchiveJobDto>>> GetListAsync(Guid dataSourceId, bool? isEnabled = null)
  ```
- **表单 Model 类**：每个属性添加中文注释，包含业务含义、校验规则、示例值。
  ```csharp
  /// <summary>任务名称，长度2-100字符，不允许仅数字，示例："订单表每日归档"</summary>
  public string Name { get; set; } = string.Empty;
  
  /// <summary>批次大小，范围1-100000，推荐5000，表示单次循环处理的最大行数</summary>
  [Range(1, 100000)]
  public int BatchSize { get; set; } = 5000;
  ```
- **关键业务逻辑**：在复杂算法（如 Cron 计算、WHERE 拼接）前添加行内中文注释说明意图。
  ```csharp
  // 根据间隔分钟数生成 Cron 表达式：每 N 分钟执行一次
  // 示例：5分钟 → "*/5 * * * *"，60分钟 → "0 * * * *"
  var cronExpression = intervalMinutes < 60 
      ? $"*/{intervalMinutes} * * * *" 
      : $"0 */{intervalMinutes / 60} * * *";
  ```

### 21.2 其他注意事项
- 更新本设计后需同步 `数据模型与API规范.md` 中的前端交互说明。
- 不得在非 Docs 目录创建新文档，后续变更总结放入 `Docs/Changes/`。
- 所有新增组件需在 `_Imports.razor` 中注册命名空间。
- API 调用统一使用 `try-catch` 捕获异常并转换为友好错误消息。

---
本规范作为定时归档任务 Web UI 初始实现的依据；如实施过程中有新需求或后端接口变更，应在此文件增补版本并记录更新日期与变更摘要。
