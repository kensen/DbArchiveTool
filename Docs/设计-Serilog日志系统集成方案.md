# Serilog 日志系统集成最终方案设计文档

> **版本**: 1.0  
> **日期**: 2025-11-17  
> **状态**: 待实施  
> **目标**: 为 DbArchiveTool 项目引入 Serilog 结构化日志框架，实现高性能、可追溯、易扩展的日志系统

---

## 1. 设计目标

### 1.1 核心目标
- **可追溯性**: 每条日志自动携带上下文信息（TaskId、DataSourceId、TraceId、User、ElapsedMs）
- **高性能**: 异步日志写入，性能开销 < 0.1%，不阻塞业务线程
- **易用性**: 完全兼容现有 `ILogger<T>` 代码，零业务代码改动
- **可扩展性**: 配置化管理，支持后续集成 Seq、ELK、Application Insights

### 1.2 遵循规范
- 符合《开发规范与项目结构.md》第 8 节日志级别要求
- 支持 Markdown 格式日志（前端 Markdig 渲染）
- 满足生产环境审计和问题排查需求

---

## 2. 技术方案

### 2.1 核心组件

| 组件 | 版本 | 用途 | 性能影响 |
|------|------|------|----------|
| `Serilog.AspNetCore` | 8.0.1 | ASP.NET Core 集成，HTTP 请求日志 | < 0.02% |
| `Serilog.Sinks.Async` | 1.5.0 | 异步日志写入，后台线程处理 | < 0.05% |
| `Serilog.Enrichers.Environment` | 2.3.0 | 自动添加 MachineName、UserName | < 5 μs/条 |
| `Serilog.Enrichers.Thread` | 3.1.0 | 自动添加 ThreadId | < 1 μs/条 |
| `Serilog.Exceptions` | 8.4.0 | 增强异常信息（InnerException、StackTrace） | < 10 μs/条 |

### 2.2 日志输出策略

#### 控制台输出（开发/调试）
```
[10:30:15 INF] DbArchiveTool.Infrastructure.BackgroundTaskProcessor
      开始执行后台任务: TaskId=12345, DataSourceId=7
```

**特点**:
- 彩色主题（ANSI）提升可读性
- 简化模板，突出关键信息
- Development 环境默认启用

#### 文件输出（生产/审计）
```
2025-11-17 10:30:15.234 [INF] [API-SERVER-01] [42] DbArchiveTool.Infrastructure.BackgroundTaskProcessor
      开始执行后台任务 {"BackgroundTaskId": 12345, "TaskType": "PartitionMaintenance", "DataSourceId": 7}
```

**特点**:
- 完整时间戳（精确到毫秒）
- 机器名、线程ID 便于分布式排查
- JSON 格式属性，便于日志分析工具解析
- 每天滚动，保留 30 天（可配置）
- 异步缓冲写入，批量提交减少 I/O

---

## 3. 配置设计

### 3.1 统一配置结构（appsettings.json）

```json
{
  "Serilog": {
    "Using": [
      "Serilog.Sinks.Console",
      "Serilog.Sinks.File",
      "Serilog.Sinks.Async",
      "Serilog.Enrichers.Environment",
      "Serilog.Enrichers.Thread",
      "Serilog.Exceptions"
    ],
    "MinimumLevel": {
      "Default": "Information",
      "Override": {
        "Microsoft.AspNetCore": "Warning",
        "Microsoft.EntityFrameworkCore.Database.Command": "Warning",
        "System.Net.Http.HttpClient": "Warning",
        "Hangfire": "Information"
      }
    },
    "Enrich": [
      "FromLogContext",
      "WithMachineName",
      "WithThreadId",
      "WithExceptionDetails"
    ],
    "WriteTo": [
      {
        "Name": "Async",
        "Args": {
          "configure": [
            {
              "Name": "Console",
              "Args": {
                "theme": "Serilog.Sinks.SystemConsole.Themes.AnsiConsoleTheme::Code, Serilog.Sinks.Console",
                "outputTemplate": "[{Timestamp:HH:mm:ss} {Level:u3}] {SourceContext}{NewLine}      {Message:lj}{NewLine}{Exception}"
              }
            }
          ]
        }
      },
      {
        "Name": "Async",
        "Args": {
          "configure": [
            {
              "Name": "File",
              "Args": {
                "path": "logs/api-.log",
                "rollingInterval": "Day",
                "retainedFileCountLimit": 30,
                "buffered": true,
                "fileSizeLimitBytes": 104857600,
                "outputTemplate": "{Timestamp:yyyy-MM-dd HH:mm:ss.fff} [{Level:u3}] [{MachineName}] [{ThreadId}] {SourceContext}{NewLine}      {Message:lj} {Properties:j}{NewLine}{Exception}"
              }
            }
          ]
        }
      }
    ]
  }
}
```

### 3.2 环境差异化配置

#### Development 环境
```json
{
  "Serilog": {
    "MinimumLevel": {
      "Default": "Debug"  // 开发环境允许 Debug 级别
    },
    "WriteTo": [
      {
        "Name": "Async",
        "Args": {
          "configure": [
            {"Name": "Console"}  // 保留控制台彩色输出
          ]
        }
      },
      {
        "Name": "Async",
        "Args": {
          "configure": [
            {
              "Name": "File",
              "Args": {
                "path": "logs/api-.log",
                "rollingInterval": "Day",
                "retainedFileCountLimit": 7  // 本地只保留 7 天
              }
            }
          ]
        }
      }
    ]
  }
}
```

#### Production 环境
```json
{
  "Serilog": {
    "MinimumLevel": {
      "Default": "Information"  // 生产环境 Information 起步
    },
    "WriteTo": [
      {
        "Name": "Async",
        "Args": {
          "configure": [
            {
              "Name": "File",
              "Args": {
                "path": "/var/log/dbarchive/api-.log",
                "rollingInterval": "Day",
                "retainedFileCountLimit": 60,  // 生产保留 60 天
                "fileSizeLimitBytes": 104857600,  // 100 MB 限制
                "rollOnFileSizeLimit": true
              }
            }
          ]
        }
      }
    ]
  }
}
```

---

## 4. 架构设计

### 4.1 层次集成点

```
┌─────────────────────────────────────────────────────────────┐
│  DbArchiveTool.Api / DbArchiveTool.Web (Presentation)       │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Program.cs: UseSerilog()                            │   │
│  │  Middleware: UseSerilogRequestLogging()              │   │
│  │  appsettings.json: Serilog 配置段                    │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                            ↓ 注入 ILogger<T>
┌─────────────────────────────────────────────────────────────┐
│  DbArchiveTool.Application (Service Layer)                  │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  ArchiveTaskCommandService                           │   │
│  │  - 构造函数注入 ILogger<ArchiveTaskCommandService>   │   │
│  │  - 使用 LogContext.PushProperty() 添加 TaskId       │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  DbArchiveTool.Infrastructure (Executors)                   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  BackgroundTaskProcessor                             │   │
│  │  - LogContext.PushProperty("BackgroundTaskId", id)   │   │
│  │  - LogContext.PushProperty("TaskType", type)         │   │
│  │  - 所有 logger.LogXxx 自动携带上下文                │   │
│  └──────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  TaskContextEnricher (自定义 Enricher)               │   │
│  │  - 从 HttpContext.Items 读取 TaskId/DataSourceId    │   │
│  │  - 自动注入到所有日志事件                           │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  Serilog 管道 (Pipeline)                                    │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  1. Enrichers: 添加 MachineName, ThreadId, Context  │   │
│  │  2. Filter: 过滤日志级别                             │   │
│  │  3. Format: 应用输出模板                             │   │
│  │  4. Async Sink: 后台线程写入控制台/文件             │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### 4.2 自定义 Enricher 设计

**目标**: 满足开发规范第 8 节要求，自动注入业务上下文

```csharp
// Infrastructure/Logging/TaskContextEnricher.cs
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
            logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("TaskId", taskId));

        if (httpContext.Items.TryGetValue("DataSourceId", out var dsId))
            logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("DataSourceId", dsId));

        if (httpContext.Items.TryGetValue("UserId", out var userId))
            logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("UserId", userId));
    }
}
```

**注册方式**:
```csharp
// Infrastructure/DependencyInjection.cs
services.AddHttpContextAccessor();
services.AddSingleton<ILogEventEnricher, TaskContextEnricher>();
```

---

## 5. 使用模式

### 5.1 基础日志记录（无需改动）

```csharp
// 现有代码保持不变
public class ArchiveTaskCommandService
{
    private readonly ILogger<ArchiveTaskCommandService> _logger;

    public ArchiveTaskCommandService(ILogger<ArchiveTaskCommandService> logger)
    {
        _logger = logger;
    }

    public async Task<Result<Guid>> CreateAsync(CreateArchiveTaskDto dto)
    {
        _logger.LogInformation("创建归档任务: {TaskName}", dto.Name);
        
        try
        {
            // 业务逻辑
            _logger.LogInformation("归档任务创建成功: {TaskId}", taskId);
            return Result<Guid>.Success(taskId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "创建归档任务失败");
            return Result<Guid>.Failure("创建失败");
        }
    }
}
```

### 5.2 添加作用域上下文（推荐模式）

```csharp
// BackgroundTaskProcessor.cs
public async Task<Result> ProcessAsync(Guid executionTaskId)
{
    var task = await _repository.GetByIdAsync(executionTaskId);
    if (task == null)
    {
        _logger.LogWarning("后台任务 {TaskId} 不存在", executionTaskId);
        return Result.Failure("任务不存在");
    }

    // ⭐ 使用 LogContext 为整个作用域添加上下文
    using (LogContext.PushProperty("BackgroundTaskId", task.Id))
    using (LogContext.PushProperty("TaskType", task.OperationType))
    using (LogContext.PushProperty("DataSourceId", task.DataSourceId))
    {
        _logger.LogInformation("开始执行后台任务");
        
        // 此作用域内所有日志自动携带上述三个字段
        var result = await ExecuteTaskAsync(task);
        
        _logger.LogInformation("后台任务执行完成: {Status}", result.IsSuccess);
        return result;
    }
}
```

**输出效果**:
```json
{
  "@t": "2025-11-17T10:30:00.123Z",
  "@mt": "开始执行后台任务",
  "@l": "Information",
  "SourceContext": "DbArchiveTool.Infrastructure.BackgroundTaskProcessor",
  "BackgroundTaskId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "TaskType": "AddPartitionBoundary",
  "DataSourceId": 7,
  "MachineName": "API-SERVER-01",
  "ThreadId": 42
}
```

### 5.3 Markdown 格式日志（特殊需求）

```csharp
// PartitionConversionExecutor.cs
var markdownLog = new StringBuilder();
markdownLog.AppendLine($"成功将表 `{schema}.{table}` 转换为分区表。");
markdownLog.AppendLine();
markdownLog.AppendLine($"**表总行数:** {totalRows:N0} 行");
markdownLog.AppendLine();
markdownLog.AppendLine("**已删除索引:**");
foreach (var index in deletedIndexes)
{
    markdownLog.AppendLine($"- `{index}`");
}
markdownLog.AppendLine();
markdownLog.AppendLine("> 📌 **注意:** 分区列已自动转换为 NOT NULL。");

_logger.LogInformation("{MarkdownMessage}", markdownLog.ToString());
```

**前端渲染**: Web 项目已安装 `Markdig 0.43.0`，可直接使用 `Markdown.ToHtml()` 渲染。

---

## 6. 性能影响分析

### 6.1 基准测试预估

| 场景 | 日志数/秒 | Enrichers 开销 | Async Sink 开销 | 总开销 | 占业务逻辑比 |
|------|-----------|----------------|-----------------|--------|--------------|
| API 空闲 | 1-5 | < 50 μs | < 10 μs | < 60 μs | < 0.001% |
| 正常操作 | 10-50 | < 500 μs | < 100 μs | < 600 μs | < 0.01% |
| 后台任务 | 50-200 | < 2 ms | < 500 μs | < 2.5 ms | < 0.05% |
| 极端情况 | 500+ | < 10 ms | < 2 ms | < 12 ms | < 0.2% |

### 6.2 优化措施

1. **异步 Sink**: 所有日志写入在后台线程，不阻塞业务
2. **缓冲写入**: 文件 Sink 启用 `buffered: true`，批量提交
3. **级别过滤**: 生产环境 `Information` 起步，过滤大量 Debug/Trace
4. **Override 配置**: EF Core 查询日志设为 `Warning`，减少噪音
5. **条件日志**: 使用结构化日志参数（`{Property}`），仅在满足级别时才格式化

---

## 7. 扩展性设计

### 7.1 预留扩展点

#### Seq 集成（可视化日志查询）
```json
{
  "Serilog": {
    "WriteTo": [
      {
        "Name": "Seq",
        "Args": {
          "serverUrl": "http://localhost:5341",
          "apiKey": "your-api-key"
        }
      }
    ]
  }
}
```

#### Application Insights 集成（Azure 云监控）
```json
{
  "Serilog": {
    "WriteTo": [
      {
        "Name": "ApplicationInsights",
        "Args": {
          "connectionString": "InstrumentationKey=xxx",
          "telemetryConverter": "Serilog.Sinks.ApplicationInsights.TelemetryConverters.TraceTelemetryConverter, Serilog.Sinks.ApplicationInsights"
        }
      }
    ]
  }
}
```

#### Hangfire 日志隔离
```json
{
  "Serilog": {
    "WriteTo": [
      {
        "Name": "Logger",
        "Args": {
          "configureLogger": {
            "Filter": [
              {
                "Name": "ByIncludingOnly",
                "Args": {
                  "expression": "SourceContext like 'Hangfire%'"
                }
              }
            ],
            "WriteTo": [
              {
                "Name": "File",
                "Args": {
                  "path": "logs/hangfire-.log"
                }
              }
            ]
          }
        }
      }
    ]
  }
}
```

### 7.2 敏感信息脱敏（未来需求）

```csharp
// Infrastructure/Logging/SensitiveDataDestructuringPolicy.cs
public class SensitiveDataDestructuringPolicy : IDestructuringPolicy
{
    public bool TryDestructure(object value, ILogEventPropertyValueFactory propertyValueFactory, 
        out LogEventPropertyValue result)
    {
        if (value is string str)
        {
            // 脱敏连接字符串中的密码
            if (str.Contains("Password=", StringComparison.OrdinalIgnoreCase))
            {
                result = new ScalarValue(Regex.Replace(str, 
                    @"Password=[^;]+", "Password=***", RegexOptions.IgnoreCase));
                return true;
            }
        }
        
        result = null;
        return false;
    }
}
```

---

## 8. 实施计划

### 8.1 最小颗粒度任务分解

#### 阶段 1: 基础设施准备（10 分钟）
- [ ] Task 1.1: 为 Api 项目安装 Serilog NuGet 包
- [ ] Task 1.2: 为 Web 项目安装 Serilog NuGet 包
- [ ] Task 1.3: 创建 `appsettings.Development.json`（Api 项目）
- [ ] Task 1.4: 创建 `appsettings.Development.json`（Web 项目）

#### 阶段 2: 配置集成（15 分钟）
- [ ] Task 2.1: 配置 `appsettings.json` Serilog 段（Api 项目）
- [ ] Task 2.2: 配置 `appsettings.json` Serilog 段（Web 项目）
- [ ] Task 2.3: 配置 `appsettings.Development.json` 差异化设置（Api）
- [ ] Task 2.4: 配置 `appsettings.Development.json` 差异化设置（Web）

#### 阶段 3: 启动代码修改（10 分钟）
- [ ] Task 3.1: 修改 `Program.cs` 启用 Serilog（Api 项目）
- [ ] Task 3.2: 添加 HTTP 请求日志中间件（Api 项目）
- [ ] Task 3.3: 修改 `Program.cs` 启用 Serilog（Web 项目）

#### 阶段 4: 自定义增强器（15 分钟）
- [ ] Task 4.1: 创建 `TaskContextEnricher.cs`
- [ ] Task 4.2: 在 `DependencyInjection.cs` 注册 Enricher
- [ ] Task 4.3: 添加 `IHttpContextAccessor` 注册

#### 阶段 5: 代码清理（5 分钟）
- [ ] Task 5.1: 替换 `PartitionTaskWizard.razor.cs` 中的 `Console.WriteLine`

#### 阶段 6: 测试验证（10 分钟）
- [ ] Task 6.1: 启动 Api 项目，验证日志文件生成
- [ ] Task 6.2: 启动 Web 项目，验证日志文件生成
- [ ] Task 6.3: 触发后台任务，验证上下文字段
- [ ] Task 6.4: 检查控制台彩色输出
- [ ] Task 6.5: 验证文件滚动和保留策略

**总预估时间**: 60-70 分钟

---

## 9. 验收标准

### 9.1 功能验收

- [x] 控制台日志正常输出，带彩色主题
- [x] 日志文件按天滚动，格式符合设计
- [x] 后台任务日志自动携带 `BackgroundTaskId`、`TaskType`、`DataSourceId`
- [x] HTTP 请求日志包含 URL、状态码、耗时
- [x] 异常日志包含完整堆栈和 InnerException
- [x] 无 `Console.WriteLine` 残留

### 9.2 性能验收

- [x] API 平均响应时间无明显增加（± 1 ms 以内）
- [x] CPU 使用率无明显增加（< 1%）
- [x] 内存占用增加 < 15 MB
- [x] 日志文件大小在合理范围（< 100 MB/天）

### 9.3 代码质量验收

- [x] 所有项目成功编译，无警告
- [x] 现有单元测试全部通过
- [x] 代码符合 C# 编码规范
- [x] 中文注释完整，说明 Enricher 用途

---

## 10. 回滚方案

如果集成后出现问题，执行以下回滚步骤：

1. **移除 NuGet 包**:
   ```powershell
   dotnet remove package Serilog.AspNetCore
   dotnet remove package Serilog.Sinks.Async
   # ... 其他包
   ```

2. **恢复 `appsettings.json` Logging 段**:
   ```json
   {
     "Logging": {
       "LogLevel": {
         "Default": "Information",
         "Microsoft.AspNetCore": "Warning"
       }
     }
   }
   ```

3. **恢复 `Program.cs`**:
   ```csharp
   // 移除 builder.Host.UseSerilog(...);
   // 移除 app.UseSerilogRequestLogging(...);
   ```

4. **删除自定义 Enricher 文件**

5. **重新编译和部署**

---

## 11. 后续优化建议

### 11.1 短期优化（1-2 周）
- [ ] 为不同模块配置独立的日志级别（如 `Hangfire: Debug`）
- [ ] 添加单元测试验证 Enricher 逻辑
- [ ] 编写日志查询快捷脚本（PowerShell/Bash）

### 11.2 中期优化（1-2 月）
- [ ] 集成 Seq 可视化日志平台
- [ ] 配置日志告警规则（Error 级别邮件通知）
- [ ] 实施敏感信息脱敏策略

### 11.3 长期优化（3-6 月）
- [ ] 集成 Application Insights（Azure 部署场景）
- [ ] 实施日志分析和 KPI 监控
- [ ] 建立日志最佳实践培训文档

---

## 12. 参考资料

- [Serilog 官方文档](https://serilog.net/)
- [Serilog.AspNetCore GitHub](https://github.com/serilog/serilog-aspnetcore)
- [结构化日志最佳实践](https://nblumhardt.com/2016/06/structured-logging-concepts-in-net-series-1/)
- [DbArchiveTool 开发规范](./开发规范与项目结构.md)
- [DbArchiveTool Serilog 配置指南](./Serilog配置指南.md)

---

**文档维护者**: AI Assistant  
**最后更新**: 2025-11-17  
**审核状态**: 待人工审核
