# DBArchiveTool

> 企业级 SQL Server 数据库归档与分区治理工具，围绕现代 .NET 8 技术栈重构，面向高可靠、高性能的数据生命周期管理场景。

## 项目概述
- 以 SQL Server 为核心目标库，提供数据源管理、分区建模、归档执行与自动化调度的端到端能力。
- 前端采用 Blazor Server + Ant Design Blazor，后端拆分为 Web API、应用服务、领域模型与基础设施四层，既保留灵活性也便于扩展。
- 在全新架构下提供既有数据库结构的兼容与迁移能力，确保现有生产环境可平滑切换。

## 功能规划
| 模块 | 核心目标 | 当前状态 |
| --- | --- | --- |
| 管理员与安全基线 | 管理员注册、登录、密码安全、基础权限控制 | ✅ 已实现首个管理员注册/登录闭环 |
| 归档任务队列 | 统一的归档任务聚合根、入队 API、状态流转 | ✅ 已提供任务入队接口与待处理查询 |
| 数据源管理 | 多来源/目标库配置、连接校验、兼容历史配置 | 📝 规划中 |
| 分区建模 | 分区方案设计、文件组规划、索引兼容分析 | 📝 规划中 |
| 分区维护 | 分区增删、空间分析、优化建议 | 📝 规划中 |
| 归档执行引擎 | 分区切换、BCP 导入导出、事务安全与回滚 | 📝 规划中 |
| 自动化调度 | 调度策略、任务队列、重试与告警 | 📝 规划中 |
| 执行日志与监控 | 操作轨迹、指标监测、差异报告 | 📝 规划中 |

## 当前进度
- ✅ 完成解决方案分层搭建（Web、Api、Application、Domain、Infrastructure、Shared）。
- ✅ 管理员账户领域模型、密码哈希与 API（注册/登录/存在性查询）可用，Blazor 端提供对应页面与状态管理。
- ✅ 归档任务聚合根、EF Core 仓储、基础 API（入队、查询待处理任务）已经落地，单元测试覆盖核心行为。
- ✅ 首个数据库迁移脚本创建 `AdminUser` 与 `ArchiveTask` 表，`ArchiveDbContext` 映射完备。
- 🚧 其余模块正按《新数据库归档工具-核心功能需求清单》推进。

## 技术架构
- **后端**：.NET 8、ASP.NET Core Web API、EF Core 8、Dapper、Microsoft.Extensions.*。
- **前端**：Blazor Server、Ant Design Blazor、SignalR（计划用于实时推送）。
- **数据库**：SQL Server（配置与元数据库），Redis、文件系统等扩展能力预留。
- **测试体系**：xUnit 单元测试已启用，集成/E2E 测试骨架已创建并可按需补充。

目录速览：
```
DbArchiveTool/
├── src/
│   ├── DbArchiveTool.Web           # Blazor Server 前端
│   ├── DbArchiveTool.Api           # REST API 层
│   ├── DbArchiveTool.Application   # 应用服务与 DTO
│   ├── DbArchiveTool.Domain        # 领域模型
│   ├── DbArchiveTool.Infrastructure# EF Core + Dapper 实现
│   └── DbArchiveTool.Shared        # 通用结果与工具
├── tests/                          # 单元/集成/E2E 测试项目
├── Docs/                           # 架构、规范与需求文档
└── Sql/                            # 历史数据库脚本参考
```

## 快速开始
### 环境准备
1. 安装 [.NET 8 SDK](https://dotnet.microsoft.com/) 与 SQL Server（或兼容实例）。
2. 配置 `src/DbArchiveTool.Api/appsettings.json` 的 `ConnectionStrings:ArchiveDatabase` 指向目标数据库；Blazor 端调用 API 的基地址位于 `src/DbArchiveTool.Web/appsettings.json`。

### 初始化数据库
使用迁移脚本创建项目所需表结构：
```powershell
cd DBManageTool
dotnet ef database update --project src/DbArchiveTool.Infrastructure --startup-project src/DbArchiveTool.Api
```
> 如需自定义连接字符串，请在执行前修改 `appsettings.json` 或通过环境变量覆盖。

### 构建与运行
```powershell
cd DBManageTool
dotnet restore
dotnet build
# 启动 API（默认 http://localhost:5083/ 或 https://localhost:5001/）
dotnet run --project src/DbArchiveTool.Api
# 在新的终端启动 Blazor 前端
dotnet run --project src/DbArchiveTool.Web
```

### 测试
```powershell
cd DBManageTool
dotnet test
```
集成与 E2E 测试已设置骨架，部分用例因依赖完整后端流程而暂时 `Skip`，可在相关模块实现后启用。

## 许可证
本项目遵循 [LICENSE](LICENSE) 中的开源许可协议。