# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## 项目概览

DBArchiveTool 是一个面向企业 SQL Server 归档治理场景的统一工具平台，基于 .NET 8 构建，采用分层架构设计。

**核心功能**:
- 数据源管理: 配置归档数据源与目标库连接，支持密码加密存储
- 分区治理: 分区元数据查询、边界值管理（添加/拆分/合并）、文件组策略
- 数据归档: 支持分区切换归档和 BCP/BulkCopy 跨库归档
- 后台任务调度: 基于 BackgroundTask 的统一任务编排与执行
- 定时归档: 集成 Hangfire 实现定时归档任务调度

**技术栈**:
- 后端: .NET 8, ASP.NET Core, EF Core 8, Dapper, Hangfire
- 前端: Blazor Server, Ant Design Blazor
- 数据库: SQL Server (工具元数据 + 业务数据库)
- 日志: Serilog

---

## 项目结构

### 解决方案分层

```
DbArchiveTool.sln
├── src/
│   ├── DbArchiveTool.Web/              # Blazor Server 前端应用
│   ├── DbArchiveTool.Api/              # ASP.NET Core Web API
│   ├── DbArchiveTool.Application/      # 应用服务层 (业务编排)
│   ├── DbArchiveTool.Domain/           # 领域层 (实体、值对象、仓储接口)
│   ├── DbArchiveTool.Infrastructure/   # 基础设施层 (EF Core、Dapper、执行器)
│   └── DbArchiveTool.Shared/           # 共享类型 (Result、枚举)
├── tests/                              # 测试项目
├── tools/EncryptPasswords/             # 密码加密迁移工具
└── Docs/                               # 设计文档与实现总结
```

### 关键目录说明

**DbArchiveTool.Domain/**: 核心领域模型，不依赖任何基础设施
- `ArchiveTasks/`: 归档任务聚合
- `DataSources/`: 数据源聚合
- `Partitions/`: 分区配置、边界值、后台任务
- `AdminUsers/`: 管理员用户聚合

**DbArchiveTool.Application/**: 业务用例编排
- `ArchiveTasks/`: 归档任务应用服务
- `DataSources/`: 数据源管理服务
- `Partitions/`: 分区管理应用服务 (配置、执行、归档)
- `Services/BackgroundTasks/`: 后台任务处理器
- `Services/ScheduledArchiveJobs/`: 定时归档任务调度器

**DbArchiveTool.Infrastructure/**: 技术实现
- `Persistence/`: EF Core 仓储实现、DbContext、迁移
- `Executors/`: 后台任务执行器、HostedService
- `Partitions/`: 分区元数据查询、脚本生成器
- `Security/`: 密码加密服务
- `Archives/`: BCP/BulkCopy 执行器

**DbArchiveTool.Web/**: Blazor Server 前端
- `Pages/`: Razor 页面组件 (数据源、分区管理、任务监控)
- `Components/`: 可复用组件 (向导、表单)
- `Services/`: API 客户端 (调用后端 API)
- `Core/`: 状态管理服务

---


## Build, Test, and Development Commands
Restore dependencies with `dotnet restore DbArchiveTool.sln`, then compile via `dotnet build DbArchiveTool.sln`. Run the API and Blazor front end with `dotnet run --project src/DbArchiveTool.Api` and `dotnet run --project src/DbArchiveTool.Web`; switch to `dotnet watch --project src/DbArchiveTool.Web run` for live reload. Sync the database before testing: `dotnet ef database update --project src/DbArchiveTool.Infrastructure --startup-project src/DbArchiveTool.Api`. Execute the suite using `dotnet test`, narrowing with `dotnet test --filter FullyQualifiedName~Partitions` while iterating.

## Coding Style & Naming Conventions
Adhere to .NET defaults: four-space indentation, PascalCase for types, camelCase for locals, and `Async` suffixes on asynchronous methods. Interfaces keep the `I` prefix (for example, `IPartitionMetadataRepository`). Maintain XML doc comments on public APIs, and update shared DTOs in `DbArchiveTool.Shared` whenever contracts change so both services and tests stay aligned.

## Testing Guidelines
xUnit powers the suite, with classes ending in `Tests` (see `tests/DbArchiveTool.UnitTests/Partitions/PartitionCommandTests.cs`). Mirror production namespaces to keep discovery predictable. Favor Arrange/Act/Assert blocks and Moq for collaborators; new fakes belong in the `Fixtures` folders. Guard regressions by asserting both `Result.IsSuccess` and error messages, because the UI surfaces localized strings. Call out coverage gaps in the PR description if they remain.

## Commit & Pull Request Guidelines
The history follows Conventional Commits (`feat:`, `fix:`, `docs:`). Keep subjects ≤72 characters and link issue IDs in the body when available. PRs should explain intent, note schema or infrastructure touchpoints, list manual/automated test evidence, and attach screenshots or console snippets for UI changes. Update `Docs/` or `README.md` when behavior shifts, and flag reviewers if a migration impacts deployed databases.

## Security & Configuration Tips
Never hard-code credentials; store them in `appsettings.Development.json` locally and environment secrets elsewhere. `DbArchiveTool.Api` reads the `ConnectionStrings:ArchiveDatabase` value, so keep overrides per environment. Redact customer identifiers when sharing logs, and rotate credentials after running utilities in `tools/EncryptPasswords/` on shared infrastructure.

## 其他规则
- 遵循《开发规范与项目结构.md》中规定的所有编码和架构标准。
- 遵循《总体设计文档.md》中规定的所有设计原则和模式。
- 对程序修改后不必每次都输出修复说明文档，在对话中输出信息给我就可以，除非我让你输出总结性的修复说明文档。
- 对程序的重构、修改、bug修复等工作输出的变更总结说明文档请放在 `DBManageTool/Docs/Changes` 目录下，文件命名格式为 `重构完成总结-模块名称.md`。
- 对于设计文档、方案文档、需求文档等请放在 `DBManageTool/Docs` 目录下，文件命名格式为 `设计-模块名称-简要描述.md`。
- 对于开发计划、任务分解等请放在 `DBManageTool/Docs/Plans` 目录下，文件命名格式为 `计划-模块名称-简要描述.md`。
- 严禁在其他目录下创建新的文档文件。
- PartitionCommand 已重构为 BackgroundTask，请参考 `Docs/重构完成总结-BackgroundTask.md` 中的内容进行相关开发工作。后续所有相关功能都使用 BackgroundTask 进行开发。0 