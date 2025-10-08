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
| 归档任务调度 | 归档任务编排、状态转移、执行链路 | 🧩 支持任务创建与分页查询，域模型/仓储完善；后台执行器与调度策略开发中 |
| 分区治理模块 | 分区规划、边界维护、文件组策略 | 🚧 SQL 元数据查询已打通（Dapper + API + UI）；命令排队与脚本执行串联中 |
| 运维控制 | 后台命令队列、执行托管、运行监控 | 🛠️ 命令队列与 HostedService 框架落地，执行记录与监控仪表板待补全 |
| 归档执行引擎 | 执行模板、脚本渲染、Dapper 执行封装 | 🧪 模板仓库与脚本生成器上线，执行器与安全校验联调中 |
| 自动化运营 | 巡检、容量阈值告警、计划任务 | 🐣 需求梳理阶段 |
| 执行日志与审计 | 行为追踪、审计报表、追责闭环 | 📝 Web 端提供占位页，服务与存储尚未实现 |

## 当前进度
- 完成分层项目结构与基础设施搭建，`ArchiveDbContext`、仓储实现及依赖注入配置齐备。
- 管理员注册/登录流程全链路贯通，使用 ASP.NET Identity 哈希和 `PasswordEncryptionService` 保证敏感字段安全，Blazor 端已接入会话状态。
- 归档数据源管理支持新建、编辑、连接测试与目标库配置，前后端复用统一 DTO，并提供命令行加密迁移工具（`tools/EncryptPasswords`）。
- 分区治理实现 SQL 元数据查询服务 `SqlPartitionQueryService` 与 `PartitionInfoController` API，Blazor 页面可展示表/分区明细并测试目标连接。
- 分区命令域模型、脚本模板渲染器及后台队列/宿主服务实现完毕，为后续执行器与权限校验提供扩展点。
- 测试层覆盖分区命令校验（`tests/DbArchiveTool.UnitTests/Partitions/PartitionCommandTests.cs`），集成测试工程已搭建，待接入端到端流程。

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
cd DBManageTool/DBManageTool
dotnet ef database update --project src/DbArchiveTool.Infrastructure --startup-project src/DbArchiveTool.Api
```
> 如需使用自定义连接字符串，可覆盖 `--connection` 参数或临时修改配置文件。

### 运行与调试
```powershell
cd DBManageTool/DBManageTool
dotnet restore
dotnet build DbArchiveTool.sln
dotnet run --project src/DbArchiveTool.Api          # API: http://localhost:5083 / https://localhost:5001
dotnet run --project src/DbArchiveTool.Web          # Blazor 前端
```
开发阶段推荐启用 `dotnet watch`（如 `dotnet watch --project src/DbArchiveTool.Web run`）获取热加载体验。

## 测试
```powershell
cd DBManageTool/DBManageTool
dotnet test
```
按模块迭代时，可使用 `dotnet test --filter FullyQualifiedName~Namespaces.SubjectTests` 聚焦某个测试类；持续集成阶段可改用 `dotnet watch test` 获得快速反馈。

## 工具与文档
- 设计与实现细节见 `Docs/`（例如《总体架构设计》《密码加密功能实现总结》《分区管理功能设计》等）。
- `tools/EncryptPasswords` 提供历史密码加密迁移脚本，可在部署前批量更新已有记录。
- SQL 模板与权限检查脚本存放于 `Sql/Partitioning/Commands`，用于分区命令生成与安全校验。

## 许可证
本项目遵循 [LICENSE](LICENSE) 文件所述的开源协议。
