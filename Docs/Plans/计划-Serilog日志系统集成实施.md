# Serilog 日志系统集成实施计划

> **版本**: 1.0  
> **日期**: 2025-11-17  
> **预计耗时**: 60-70 分钟  
> **依赖文档**: [设计-Serilog日志系统集成方案.md](../设计-Serilog日志系统集成方案.md)

---

## 任务总览

本计划将 Serilog 集成工作拆解为 **19 个最小颗粒度任务**，按依赖关系分为 **6 个阶段**执行。

### 进度仪表盘

| 阶段 | 任务数 | 预计耗时 | 状态 |
|------|--------|----------|------|
| 阶段 1: 基础设施准备 | 4 | 10 分钟 | ⏳ 待开始 |
| 阶段 2: 配置集成 | 4 | 15 分钟 | ⏳ 待开始 |
| 阶段 3: 启动代码修改 | 3 | 10 分钟 | ⏳ 待开始 |
| 阶段 4: 自定义增强器 | 3 | 15 分钟 | ⏳ 待开始 |
| 阶段 5: 代码清理 | 1 | 5 分钟 | ⏳ 待开始 |
| 阶段 6: 测试验证 | 4 | 10 分钟 | ⏳ 待开始 |
| **总计** | **19** | **65 分钟** | **0%** |

---

## 阶段 1: 基础设施准备

### Task 1.1: 为 Api 项目安装 Serilog NuGet 包

**目标**: 安装 5 个核心 Serilog 包到 `DbArchiveTool.Api.csproj`

**命令**:
```powershell
cd f:\tmp\数据归档工具\DBManageTool\src\DbArchiveTool.Api
dotnet add package Serilog.AspNetCore --version 8.0.1
dotnet add package Serilog.Sinks.Async --version 1.5.0
dotnet add package Serilog.Enrichers.Environment --version 2.3.0
dotnet add package Serilog.Enrichers.Thread --version 3.1.0
dotnet add package Serilog.Exceptions --version 8.4.0
```

**验收标准**:
- [ ] 命令执行成功，无错误
- [ ] `DbArchiveTool.Api.csproj` 文件包含上述 5 个 PackageReference

**预计耗时**: 2 分钟

---

### Task 1.2: 为 Web 项目安装 Serilog NuGet 包

**目标**: 安装 5 个核心 Serilog 包到 `DbArchiveTool.Web.csproj`

**命令**:
```powershell
cd f:\tmp\数据归档工具\DBManageTool\src\DbArchiveTool.Web
dotnet add package Serilog.AspNetCore --version 8.0.1
dotnet add package Serilog.Sinks.Async --version 1.5.0
dotnet add package Serilog.Enrichers.Environment --version 2.3.0
dotnet add package Serilog.Enrichers.Thread --version 3.1.0
dotnet add package Serilog.Exceptions --version 8.4.0
```

**验收标准**:
- [ ] 命令执行成功，无错误
- [ ] `DbArchiveTool.Web.csproj` 文件包含上述 5 个 PackageReference

**预计耗时**: 2 分钟

---

### Task 1.3: 创建 appsettings.Development.json（Api 项目）

**目标**: 为 Api 项目创建开发环境专用配置文件

**文件路径**: `src/DbArchiveTool.Api/appsettings.Development.json`

**内容**:
```json
{
  "Serilog": {
    "MinimumLevel": {
      "Default": "Debug"
    }
  }
}
```

**验收标准**:
- [ ] 文件创建成功
- [ ] JSON 格式正确，无语法错误

**预计耗时**: 3 分钟

---

### Task 1.4: 创建 appsettings.Development.json（Web 项目）

**目标**: 为 Web 项目创建开发环境专用配置文件

**文件路径**: `src/DbArchiveTool.Web/appsettings.Development.json`

**内容**:
```json
{
  "Serilog": {
    "MinimumLevel": {
      "Default": "Debug"
    }
  }
}
```

**验收标准**:
- [ ] 文件创建成功
- [ ] JSON 格式正确，无语法错误

**预计耗时**: 3 分钟

---

## 阶段 2: 配置集成

### Task 2.1: 配置 appsettings.json Serilog 段（Api 项目）

**目标**: 在 Api 项目的主配置文件中添加 Serilog 完整配置

**文件路径**: `src/DbArchiveTool.Api/appsettings.json`

**操作**:
1. 删除原有 `Logging` 配置段
2. 添加完整 `Serilog` 配置段（参考设计文档第 3.1 节）
3. 日志路径设置为 `logs/api-.log`

**关键配置点**:
- `MinimumLevel.Default`: `Information`
- `Override`: Microsoft.AspNetCore=Warning, EF=Warning, Hangfire=Information
- `Enrich`: FromLogContext, WithMachineName, WithThreadId, WithExceptionDetails
- `WriteTo`: Async Console + Async File

**验收标准**:
- [ ] JSON 格式正确
- [ ] 包含所有必需的 Using 声明
- [ ] 输出模板符合设计规范

**预计耗时**: 4 分钟

---

### Task 2.2: 配置 appsettings.json Serilog 段（Web 项目）

**目标**: 在 Web 项目的主配置文件中添加 Serilog 完整配置

**文件路径**: `src/DbArchiveTool.Web/appsettings.json`

**操作**:
1. 删除原有 `Logging` 配置段
2. 添加完整 `Serilog` 配置段
3. 日志路径设置为 `logs/web-.log`

**与 Api 项目的差异**:
- 日志文件路径不同（`web-` vs `api-`）
- 其余配置完全相同

**验收标准**:
- [ ] JSON 格式正确
- [ ] 日志路径为 `logs/web-.log`
- [ ] 保留原有 `ArchiveApi` 和 `ConnectionStrings` 配置段

**预计耗时**: 4 分钟

---

### Task 2.3: 配置 appsettings.Development.json 差异化设置（Api）

**目标**: 完善开发环境配置，添加控制台彩色输出

**文件路径**: `src/DbArchiveTool.Api/appsettings.Development.json`

**操作**: 扩展 Task 1.3 创建的文件，添加完整开发环境配置

**内容**:
```json
{
  "Serilog": {
    "MinimumLevel": {
      "Default": "Debug"
    },
    "WriteTo": [
      {
        "Name": "Async",
        "Args": {
          "configure": [
            {
              "Name": "Console",
              "Args": {
                "theme": "Serilog.Sinks.SystemConsole.Themes.AnsiConsoleTheme::Code, Serilog.Sinks.Console"
              }
            }
          ]
        }
      }
    ]
  }
}
```

**验收标准**:
- [ ] 配置覆盖生产环境的 `MinimumLevel`
- [ ] 保留控制台彩色输出
- [ ] JSON 格式正确

**预计耗时**: 3 分钟

---

### Task 2.4: 配置 appsettings.Development.json 差异化设置（Web）

**目标**: 完善 Web 项目开发环境配置

**文件路径**: `src/DbArchiveTool.Web/appsettings.Development.json`

**内容**: 与 Task 2.3 相同

**验收标准**:
- [ ] 配置与 Api 项目开发环境一致
- [ ] JSON 格式正确

**预计耗时**: 4 分钟

---

## 阶段 3: 启动代码修改

### Task 3.1: 修改 Program.cs 启用 Serilog（Api 项目）

**目标**: 在 Api 项目启动代码中集成 Serilog

**文件路径**: `src/DbArchiveTool.Api/Program.cs`

**操作**:
1. 在文件顶部添加 `using Serilog;`
2. 在 `var builder = WebApplication.CreateBuilder(args);` 之后立即添加 Serilog 配置
3. 确保在 `app.Run();` 之前配置

**关键代码**:
```csharp
using Serilog;

var builder = WebApplication.CreateBuilder(args);

// ⭐ 配置 Serilog
builder.Host.UseSerilog((context, services, configuration) => configuration
    .ReadFrom.Configuration(context.Configuration)
    .ReadFrom.Services(services)
    .Enrich.WithProperty("Application", "DbArchiveTool.Api")
    .Enrich.WithProperty("Environment", context.HostingEnvironment.EnvironmentName)
);

// ... 原有代码
```

**验收标准**:
- [ ] 代码编译成功
- [ ] `UseSerilog` 调用位置正确（在服务注册之前）
- [ ] 添加了 Application 和 Environment 属性

**预计耗时**: 4 分钟

---

### Task 3.2: 添加 HTTP 请求日志中间件（Api 项目）

**目标**: 为 Api 项目添加自动的 HTTP 请求日志记录

**文件路径**: `src/DbArchiveTool.Api/Program.cs`

**操作**: 在 `app.UseHttpsRedirection();` 之前添加中间件

**关键代码**:
```csharp
var app = builder.Build();

// ... Swagger 配置

// ⭐ 添加 Serilog HTTP 请求日志
app.UseSerilogRequestLogging(options =>
{
    options.MessageTemplate = "HTTP {RequestMethod} {RequestPath} responded {StatusCode} in {Elapsed:0.0000} ms";
    options.EnrichDiagnosticContext = (diagnosticContext, httpContext) =>
    {
        diagnosticContext.Set("RequestHost", httpContext.Request.Host.Value);
        diagnosticContext.Set("UserAgent", httpContext.Request.Headers["User-Agent"].ToString());
    };
});

app.UseHttpsRedirection();
// ... 其余中间件
```

**验收标准**:
- [ ] 代码编译成功
- [ ] 中间件位置正确（在 UseHttpsRedirection 之前）
- [ ] 包含 RequestHost 和 UserAgent 上下文

**预计耗时**: 3 分钟

---

### Task 3.3: 修改 Program.cs 启用 Serilog（Web 项目）

**目标**: 在 Web 项目启动代码中集成 Serilog

**文件路径**: `src/DbArchiveTool.Web/Program.cs`

**操作**: 与 Task 3.1 相同，但 Application 属性改为 `"DbArchiveTool.Web"`

**关键代码**:
```csharp
using Serilog;

var builder = WebApplication.CreateBuilder(args);

// ⭐ 配置 Serilog
builder.Host.UseSerilog((context, services, configuration) => configuration
    .ReadFrom.Configuration(context.Configuration)
    .ReadFrom.Services(services)
    .Enrich.WithProperty("Application", "DbArchiveTool.Web")
    .Enrich.WithProperty("Environment", context.HostingEnvironment.EnvironmentName)
);

// ... 原有代码
```

**验收标准**:
- [ ] 代码编译成功
- [ ] Application 属性为 "DbArchiveTool.Web"
- [ ] 不添加 HTTP 请求日志中间件（Blazor Server 不需要）

**预计耗时**: 3 分钟

---

## 阶段 4: 自定义增强器

### Task 4.1: 创建 TaskContextEnricher.cs

**目标**: 实现自定义 Enricher，自动注入业务上下文字段

**文件路径**: `src/DbArchiveTool.Infrastructure/Logging/TaskContextEnricher.cs`

**内容**:
```csharp
using Microsoft.AspNetCore.Http;
using Serilog.Core;
using Serilog.Events;

namespace DbArchiveTool.Infrastructure.Logging;

/// <summary>
/// 任务上下文富化器
/// 自动从 HttpContext.Items 中读取 TaskId、DataSourceId 等业务上下文字段并注入到日志事件中
/// </summary>
public class TaskContextEnricher : ILogEventEnricher
{
    private readonly IHttpContextAccessor? _httpContextAccessor;

    public TaskContextEnricher(IHttpContextAccessor? httpContextAccessor = null)
    {
        _httpContextAccessor = httpContextAccessor;
    }

    public void Enrich(LogEvent logEvent, ILogEventPropertyFactory propertyFactory)
    {
        var httpContext = _httpContextAccessor?.HttpContext;
        if (httpContext == null) return;

        // 从 HttpContext.Items 读取业务上下文
        if (httpContext.Items.TryGetValue("TaskId", out var taskId))
        {
            logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("TaskId", taskId));
        }

        if (httpContext.Items.TryGetValue("DataSourceId", out var dsId))
        {
            logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("DataSourceId", dsId));
        }

        if (httpContext.Items.TryGetValue("UserId", out var userId))
        {
            logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("UserId", userId));
        }
    }
}
```

**验收标准**:
- [ ] 文件创建成功，目录 `Logging` 自动创建
- [ ] 类包含完整中文 XML 注释
- [ ] 实现 `ILogEventEnricher` 接口
- [ ] 代码编译成功

**预计耗时**: 6 分钟

---

### Task 4.2: 在 DependencyInjection.cs 注册 Enricher

**目标**: 将自定义 Enricher 注册到 DI 容器

**文件路径**: `src/DbArchiveTool.Infrastructure/DependencyInjection.cs`

**操作**: 在 `AddInfrastructureLayer` 方法中添加注册

**关键代码**:
```csharp
using DbArchiveTool.Infrastructure.Logging;
using Serilog.Core;

public static IServiceCollection AddInfrastructureLayer(
    this IServiceCollection services, 
    IConfiguration configuration)
{
    // ... 原有代码
    
    // ⭐ 注册 Serilog 自定义 Enricher
    services.AddSingleton<ILogEventEnricher, TaskContextEnricher>();
    
    return services;
}
```

**验收标准**:
- [ ] 添加了 `using Serilog.Core;` 引用
- [ ] 注册代码位置合理（建议在方法末尾）
- [ ] 代码编译成功

**预计耗时**: 4 分钟

---

### Task 4.3: 添加 IHttpContextAccessor 注册

**目标**: 确保 HttpContextAccessor 在 Infrastructure 层可用

**文件路径**: `src/DbArchiveTool.Infrastructure/DependencyInjection.cs`

**操作**: 检查是否已注册，如未注册则添加

**关键代码**:
```csharp
// ⭐ 注册 IHttpContextAccessor（如果未注册）
services.AddHttpContextAccessor();
```

**验收标准**:
- [ ] 确认 `AddHttpContextAccessor()` 已调用
- [ ] 如果 Api/Web 项目已注册，无需重复注册（多次注册无害）
- [ ] 代码编译成功

**预计耗时**: 5 分钟

---

## 阶段 5: 代码清理

### Task 5.1: 替换 PartitionTaskWizard.razor.cs 中的 Console.WriteLine

**目标**: 清理遗留的控制台输出，使用 ILogger 代替

**文件路径**: `src/DbArchiveTool.Web/Pages/BackgroundTaskManagement/PartitionTaskWizard.razor.cs`

**操作**: 替换 3 处 `Console.WriteLine`

**位置和替换**:

1. **第 576 行附近**:
```csharp
// 原代码
catch (Exception ex)
{
    Console.WriteLine($"关闭向导标签页失败: {ex.Message}");
}

// 替换为
catch (Exception ex)
{
    _logger.LogError(ex, "关闭向导标签页失败");
}
```

2. **第 606 行附近**:
```csharp
// 原代码
catch (Exception ex)
{
    Console.WriteLine($"打开新标签页失败: {ex.Message}");
}

// 替换为
catch (Exception ex)
{
    _logger.LogError(ex, "打开新标签页失败");
}
```

3. **第 641 行附近**:
```csharp
// 原代码
catch (Exception ex)
{
    Console.WriteLine($"导航到任务详情页失败: {ex.Message}");
}

// 替换为
catch (Exception ex)
{
    _logger.LogError(ex, "导航到任务详情页失败");
}
```

**验收标准**:
- [ ] 3 处 `Console.WriteLine` 全部替换为 `_logger.LogError`
- [ ] 异常对象作为第一个参数传递
- [ ] 代码编译成功

**预计耗时**: 5 分钟

---

## 阶段 6: 测试验证

### Task 6.1: 启动 Api 项目，验证日志文件生成

**目标**: 确认 Api 项目日志系统正常工作

**操作**:
```powershell
cd f:\tmp\数据归档工具\DBManageTool\src\DbArchiveTool.Api
dotnet run
```

**验证步骤**:
1. 观察控制台输出是否带彩色主题
2. 检查 `logs/` 目录是否自动创建
3. 检查 `logs/api-20251117.log` 文件是否生成
4. 打开日志文件，验证格式是否符合设计
5. 使用 Swagger 调用一个 API，检查日志是否包含 HTTP 请求记录

**验收标准**:
- [ ] 控制台显示彩色日志
- [ ] 日志文件自动创建
- [ ] 日志格式包含时间戳、级别、MachineName、ThreadId、SourceContext
- [ ] HTTP 请求日志包含方法、路径、状态码、耗时

**预计耗时**: 3 分钟

---

### Task 6.2: 启动 Web 项目，验证日志文件生成

**目标**: 确认 Web 项目日志系统正常工作

**操作**:
```powershell
cd f:\tmp\数据归档工具\DBManageTool\src\DbArchiveTool.Web
dotnet run
```

**验证步骤**:
1. 观察控制台输出
2. 检查 `logs/web-20251117.log` 文件是否生成
3. 访问几个页面，检查日志输出

**验收标准**:
- [ ] 控制台显示彩色日志
- [ ] 日志文件自动创建
- [ ] 日志格式与 Api 项目一致

**预计耗时**: 2 分钟

---

### Task 6.3: 触发后台任务，验证上下文字段

**目标**: 验证 LogContext 和自定义 Enricher 是否正常工作

**操作**:
1. 通过 API 创建一个分区执行任务
2. 等待 Hangfire 执行任务
3. 查看日志文件中的后台任务日志

**验证点**:
```bash
# 在日志文件中搜索后台任务日志
cd f:\tmp\数据归档工具\DBManageTool\src\DbArchiveTool.Api
Select-String -Path "logs\*.log" -Pattern "BackgroundTaskId" -Context 0,2
```

**验收标准**:
- [ ] 日志包含 `BackgroundTaskId` 字段
- [ ] 日志包含 `TaskType` 字段
- [ ] 日志包含 `DataSourceId` 字段（如果任务有数据源）
- [ ] 上下文字段以 JSON 格式输出在 `Properties` 部分

**预计耗时**: 3 分钟

---

### Task 6.4: 验证日志级别过滤

**目标**: 确认日志级别过滤生效

**操作**:
1. 检查日志文件中是否有大量 EF Core 查询日志
2. 检查日志文件中是否有 Microsoft.AspNetCore 的 Debug 日志

**验收标准**:
- [ ] 日志文件中无 EF Core 的查询 SQL（应被过滤为 Warning）
- [ ] 日志文件中无 ASP.NET Core 的 Debug/Info 级别日志
- [ ] 业务逻辑的 Information 级别日志正常输出

**预计耗时**: 2 分钟

---

## 实施顺序建议

### 推荐顺序（按依赖关系）

1. **串行执行阶段 1-3**: 基础设施 → 配置 → 启动代码
2. **可选执行阶段 4**: 自定义 Enricher（可后续添加）
3. **快速执行阶段 5**: 代码清理
4. **最后执行阶段 6**: 验证

### 快速通道（仅核心功能，45 分钟）

如果时间紧张，可跳过以下任务：
- Task 1.3/1.4: Development 配置（使用默认配置）
- Task 3.2: HTTP 请求日志中间件（可选功能）
- Task 4.1-4.3: 自定义 Enricher（后续添加）

**核心任务列表**（9 个任务，45 分钟）:
1. Task 1.1-1.2: 安装 NuGet 包（必需）
2. Task 2.1-2.2: 配置 appsettings.json（必需）
3. Task 3.1: Api Program.cs（必需）
4. Task 3.3: Web Program.cs（必需）
5. Task 5.1: 代码清理（必需）
6. Task 6.1-6.2: 基础验证（必需）

---

## 故障排查指南

### 问题 1: 编译错误 "找不到 Serilog 命名空间"

**原因**: NuGet 包未正确安装

**解决**:
```powershell
dotnet restore
dotnet build
```

---

### 问题 2: 日志文件未生成

**原因**: 可能是配置错误或权限问题

**解决**:
1. 检查 `appsettings.json` 中 Serilog 配置段是否正确
2. 手动创建 `logs/` 目录
3. 检查控制台输出是否有 Serilog 初始化错误

---

### 问题 3: 控制台日志无彩色输出

**原因**: 终端不支持 ANSI 或配置错误

**解决**:
1. 使用 Windows Terminal 或 VS Code 集成终端
2. 检查 `theme` 配置是否正确

---

### 问题 4: 自定义 Enricher 不生效

**原因**: 未注册或 HttpContext 为空

**解决**:
1. 确认 `DependencyInjection.cs` 中已注册 `ILogEventEnricher`
2. 确认已调用 `AddHttpContextAccessor()`
3. 检查是否在非 HTTP 请求上下文中（如后台任务）

**后台任务解决方案**: 使用 `LogContext.PushProperty` 手动添加上下文

---

## 回滚检查清单

如果需要回滚，按以下顺序执行：

1. [ ] 恢复 `Program.cs` 文件（移除 `UseSerilog` 调用）
2. [ ] 恢复 `appsettings.json` 文件（恢复 `Logging` 配置段）
3. [ ] 移除 `TaskContextEnricher.cs` 文件
4. [ ] 移除 `DependencyInjection.cs` 中的 Enricher 注册
5. [ ] 卸载 Serilog NuGet 包
6. [ ] 重新编译项目

**回滚命令**:
```powershell
# 卸载 NuGet 包
dotnet remove package Serilog.AspNetCore
dotnet remove package Serilog.Sinks.Async
dotnet remove package Serilog.Enrichers.Environment
dotnet remove package Serilog.Enrichers.Thread
dotnet remove package Serilog.Exceptions
```

---

## 附录: 快速命令参考

### 一键安装所有包（Api 项目）

```powershell
cd f:\tmp\数据归档工具\DBManageTool\src\DbArchiveTool.Api
dotnet add package Serilog.AspNetCore --version 8.0.1; `
dotnet add package Serilog.Sinks.Async --version 1.5.0; `
dotnet add package Serilog.Enrichers.Environment --version 2.3.0; `
dotnet add package Serilog.Enrichers.Thread --version 3.1.0; `
dotnet add package Serilog.Exceptions --version 8.4.0
```

### 一键安装所有包（Web 项目）

```powershell
cd f:\tmp\数据归档工具\DBManageTool\src\DbArchiveTool.Web
dotnet add package Serilog.AspNetCore --version 8.0.1; `
dotnet add package Serilog.Sinks.Async --version 1.5.0; `
dotnet add package Serilog.Enrichers.Environment --version 2.3.0; `
dotnet add package Serilog.Enrichers.Thread --version 3.1.0; `
dotnet add package Serilog.Exceptions --version 8.4.0
```

### 验证日志输出

```powershell
# 实时查看日志
Get-Content -Path "src\DbArchiveTool.Api\logs\api-*.log" -Wait -Tail 20

# 搜索错误日志
Select-String -Path "src\DbArchiveTool.Api\logs\*.log" -Pattern "\[ERR\]" -Context 1,3
```

---

**文档维护者**: AI Assistant  
**最后更新**: 2025-11-17  
**实施状态**: 待开始
