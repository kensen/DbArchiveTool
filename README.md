# DBArchiveTool

> 面向企业 SQL Server 归档治理场景的工具集，基于 .NET 8 打造的统一网关与运维平台。

## 项目简介
- 以 SQL Server 为核心，提供数据源管理、分区治理、归档任务编排等统一能力。
- 后端采用分层架构（Domain / Application / Infrastructure / Api），前端以 Blazor Server + Ant Design Blazor 构建交互界面。
- 通过共享的 Result/PagedResult 契约、密码加密与脚本模板库，保障调用一致性与敏感信息安全。

## 功能规划
| 模块 | 目标 | 当前状态 |
| --- | --- | --- |
| 管理员与安全体系 | 管理员注册、登录、密码保护与会话管理 | ✅ 已上线注册/登录 API 及 Blazor 表单，密码加密和会话状态持久化；角色授权规划中 |
| 数据源管理 | 数据源/目标库配置、连接校验、历史追踪 | ✅ API 与 Web 端完成 CRUD、连接测试、目标库配置，密码统一加密存储 |
| 分区治理模块 | 分区规划、边界维护、文件组策略 | ✅ 分区元数据查询、添加分区值、拆分与合并功能全链路完成；支持自动创建文件组/数据文件及差量边界同步 |
| 归档任务调度 | 归档任务编排、状态转移、执行链路 | ✅ 支持基于 BackgroundTask 的任务编排、排队与后台 HostedService 执行；分区切换归档功能已上线，含预检/补齐/执行全流程 |
| 运维控制 | 后台命令队列、执行托管、运行监控 | ✅ 命令队列与 HostedService 框架落地，执行日志可回放（Markdown 格式），任务监控看板已完成；告警规则待补全 |
| 归档执行引擎 | 执行模板、脚本渲染、Dapper 执行封装 | ✅ 模板仓库与脚本生成器上线，分区切换执行器已完成；BCP/BulkCopy 方案技术调研中 |
| 自动化运营 | 巡检、容量阈值告警、计划任务 | 🐣 需求梳理阶段 |
| 执行日志与审计 | 行为追踪、审计报表、追责闭环 | 🧩 任务执行日志已落地（详细 Markdown 格式），审计报表与追责流程待完善 |

## 当前进度

### 核心功能已完成 ✅
- **分层架构**：完成 Domain / Application / Infrastructure / Api / Web 分层搭建，`ArchiveDbContext`、仓储实现及依赖注入配置齐备。
- **安全体系**：管理员注册/登录流程全链路贯通，使用 ASP.NET Identity 哈希和 `PasswordEncryptionService` 保证敏感字段安全，Blazor 端已接入会话状态。
- **数据源管理**：归档数据源支持新建、编辑、连接测试与目标库配置，前后端复用统一 DTO，提供命令行加密迁移工具（`tools/EncryptPasswords`）。

### 分区治理功能完成 🎉
- **元数据查询**：实现 `SqlPartitionQueryService` 与 `PartitionInfoController` API，Blazor 页面可展示表/分区明细并测试目标连接。
- **添加分区值** (2025-10-17)：支持单值添加与批量生成（日期范围/数值序列），含前端验证、文件组选择、预览删除等完整交互。
- **分区拆分** (2025-10-24)：2步向导（设置边界值 → 确认执行），支持批量生成、文件组继承/指定、DDL 脚本预览，后台任务执行含详细 Markdown 日志。
- **分区合并** (2025-10-27)：2步向导（选择删除边界 → 确认执行），智能提示合并范围（左/右边界），端到端测试通过。
- **执行引擎**：分区执行处理器支持自动创建文件组与数据文件、差量边界同步、记录分区函数/方案与拆分步骤的详细耗时日志，无新增边界时自动跳过。

### 数据归档功能完成 🎉 (2025-10-30)
- **归档向导**：4步向导（方案选择 → 参数配置 → 预检结果 → 执行确认），支持单分区/批量分区归档。
- **预检服务**：`PartitionSwitchInspectionService` 完成结构/索引/约束/权限/锁检查，分级输出 Blocker/AutoFix/Warning。
- **自动补齐**：`PartitionSwitchAutoFixExecutor` 支持6种修复步骤（创建表/同步分区/索引/约束/统计），失败自动回滚。
- **执行流程**：`BackgroundTaskProcessor.ExecuteArchiveSwitchAsync` 实现8阶段流程（预检 → 补齐 → 切换 → 验证 → 清理），含详细 Markdown 日志。
- **任务编排**：基于 `BackgroundTask` 的任务队列、排队与 HostedService 执行，支持失败草稿重新发起，任务监控看板已完成。
- **端到端验证**：单分区/批量归档测试通过，索引对齐检测与修复验证，Markdown 日志渲染正常。

### 测试与文档
- **单元测试**：覆盖分区命令校验（`tests/DbArchiveTool.UnitTests/Partitions/PartitionCommandTests.cs`）。
- **集成测试**：工程已搭建（`BackgroundTaskContextEndpointTests`），覆盖正常切换、结构不匹配、索引对齐等场景。
- **技术文档**：完成《分区边界值明细管理功能规划设计》《数据归档功能开发 Todos》等设计文档更新。

### 待完善事项 📋
- **无分区配置草稿场景**：需补充默认配置加载与归档向导交互逻辑（已记录在 TODO）。
- **用户操作手册**：编写分区管理、归档操作等用户手册与 FAQ。
- **BCP/BulkCopy 方案**：技术调研与接口占位（Milestone D）。
- **监控告警**：任务监控看板已完成，告警规则与失败重试入口待补全。

## 系统架构
- **后端**：.NET 8、ASP.NET Core Web API、EF Core 8、Dapper、Microsoft.Extensions.*。
- **前端**：Blazor Server、Ant Design Blazor；计划结合 SignalR 实现实时反馈。
- **数据层**：SQL Server（主库与元数据），脚本模板位于 `Sql/Partitioning` 目录，可扩展 Redis/文件存储作为缓存与中间结果。
- **测试**：xUnit、Moq，分别用于单元、集成与 E2E 项目（位于 `tests/`）。

项目目录示例：
```
DbArchiveTool/
  src/
    DbArchiveTool.Api/              # REST API 层
    DbArchiveTool.Web/              # Blazor Server 界面
    DbArchiveTool.Application/      # 用例服务与 DTO
    DbArchiveTool.Domain/           # 领域模型与聚合
    DbArchiveTool.Infrastructure/   # EF Core、Dapper、执行器实现
    DbArchiveTool.Shared/           # 共享类型与 Result 契约
  tests/                            # 单元 / 集成 / E2E 测试项目
  Docs/                             # 设计方案与架构文档
  Sql/                              # SQL 模板与示例脚本
  tools/EncryptPasswords/           # 密码加密迁移工具
```

## 快速开始
### 环境准备
1. 安装 [.NET 8 SDK](https://dotnet.microsoft.com/) 与 SQL Server（或确保可访问目标实例）。
2. 配置 `src/DbArchiveTool.Api/appsettings.Development.json` 中的 `ConnectionStrings:ArchiveDatabase`，并在 `src/DbArchiveTool.Web/appsettings.Development.json` 中指向本地 API 地址。
3. 若需使用 `dotnet ef`，请先执行 `dotnet tool restore` 或 `dotnet tool install --global dotnet-ef`。

### 初始化数据库
```powershell
cd DBManageTool
dotnet ef database update --project src/DbArchiveTool.Infrastructure --startup-project src/DbArchiveTool.Api
```
> 如需使用自定义连接字符串，可覆盖 `--connection` 参数或临时修改配置文件。

### 运行与调试
```powershell
cd DBManageTool
dotnet restore
dotnet build DbArchiveTool.sln
dotnet run --project src/DbArchiveTool.Api          # API: http://localhost:5083 (Swagger 已启用)
dotnet run --project src/DbArchiveTool.Web          # Blazor 前端: http://localhost:5000
```
开发阶段推荐启用 `dotnet watch`（如 `dotnet watch --project src/DbArchiveTool.Web run`）获取热加载体验。

### 主要功能入口
1. **数据源管理**：`/DataSources` - 配置归档数据源与目标库连接。
2. **分区管理**：`/PartitionManagement` - 查询分区元数据、添加/拆分/合并分区边界值。
3. **归档任务**：`/PartitionManagement` → "切换分区"按钮 - 启动归档向导，选择方案、配置参数、预检与执行。
4. **任务监控**：`/BackgroundTasks` - 查看任务执行状态、日志回放（Markdown 格式）。

## 测试
```powershell
cd DBManageTool
dotnet test
```
按模块迭代时，可使用 `dotnet test --filter FullyQualifiedName~Partitions` 聚焦分区相关测试；持续集成阶段可改用 `dotnet watch test` 获得快速反馈。

### 测试覆盖范围
- **单元测试** (`DbArchiveTool.UnitTests`)：分区命令校验、领域模型逻辑、服务层单元测试。
- **集成测试** (`DbArchiveTool.IntegrationTests`)：API 端点测试、后台任务执行流程、数据库交互场景。
- **端到端测试** (`DbArchiveTool.E2ETests`)：完整业务流程验证（目前部分测试标记为 Skip，待真实环境验证）。

## 工具与文档

### 设计文档（`Docs/`）
- **总体架构**：《总体设计文档》《开发规范与项目结构》
- **功能设计**：《分区边界值明细管理功能规划设计》《数据归档功能开发 Todos》
- **实现总结**：《密码加密功能实现总结》《重构完成总结-BackgroundTask》
- **进度追踪**：《分区边界值功能 TODO》（已完成 Milestone C，分区管理与归档功能全链路贯通）

### 实用工具
- **密码加密迁移**：`tools/EncryptPasswords` - 批量加密历史密码记录，部署前必须执行。
- **SQL 模板库**：`Sql/Partitioning/Commands` - 分区命令生成模板与权限检查脚本。
- **测试数据**：`Sql/PartitionArchive_*.sql` - 元数据表结构定义（与逆向工程遗留结构对齐）。

### 里程碑
- ✅ **Milestone A** (2025-10-17)：后端 Add/Split/Merge API + 审计日志 + 任务记录
- ✅ **Milestone B** (2025-10-27)：前端边界操作子控件（添加/拆分/合并）全部上线
- ✅ **Milestone C** (2025-10-30)：数据归档（分区切换）闭环 + 任务调度平台 UI 扩展
- 📋 **Milestone D** (计划中)：BCP/BulkCopy 方案技术调研与实现

## 许可证
本项目遵循 [LICENSE](LICENSE) 文件所述的开源协议。
