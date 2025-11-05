# Hangfire 配置修复 - Web 项目

## 问题描述

访问 `/archive-jobs` 页面时出现以下错误:

```
System.InvalidOperationException: Current JobStorage instance has not been initialized yet. 
You must set it before using Hangfire Client or Server API.
```

## 根本原因

Web 项目中的 `HangfireMonitorService` 使用了 `JobStorage.Current` 来访问 Hangfire 数据,但 Web 项目本身没有初始化 Hangfire 存储。

## 解决方案

在 Web 项目中配置 Hangfire 存储(只读模式,不启动 Hangfire Server)。

### 1. 添加 NuGet 包

**文件**: `src/DbArchiveTool.Web/DbArchiveTool.Web.csproj`

```xml
<PackageReference Include="Hangfire.SqlServer" Version="1.8.21" />
```

### 2. 配置数据库连接

**文件**: `src/DbArchiveTool.Web/appsettings.json`

```json
{
  "ConnectionStrings": {
    "HangfireDatabase": "Server=localhost;Database=DbArchiveTool;Trusted_Connection=True;TrustServerCertificate=True"
  }
}
```

注意:使用与 API 项目相同的数据库和 Hangfire Schema。

### 3. 在 Program.cs 中配置 Hangfire

**文件**: `src/DbArchiveTool.Web/Program.cs`

添加以下代码:

```csharp
using Hangfire;
using Hangfire.SqlServer;

// ... 其他代码 ...

// 配置 Hangfire 存储(只读模式,用于监控)
var hangfireConnectionString = builder.Configuration.GetConnectionString("HangfireDatabase");
if (string.IsNullOrWhiteSpace(hangfireConnectionString))
{
    throw new InvalidOperationException("未在配置中找到 ConnectionStrings:HangfireDatabase。");
}

builder.Services.AddHangfire(config => config
    .SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
    .UseSimpleAssemblyNameTypeSerializer()
    .UseRecommendedSerializerSettings()
    .UseSqlServerStorage(hangfireConnectionString, new SqlServerStorageOptions
    {
        CommandBatchMaxTimeout = TimeSpan.FromMinutes(5),
        SlidingInvisibilityTimeout = TimeSpan.FromMinutes(5),
        QueuePollInterval = TimeSpan.Zero,
        UseRecommendedIsolationLevel = true,
        DisableGlobalLocks = true,
        SchemaName = "Hangfire"  // 必须与 API 项目一致
    }));

// 注册 Hangfire 监控服务
builder.Services.AddScoped<IHangfireMonitorService, HangfireMonitorService>();
```

### 4. 初始化 JobStorage.Current (关键步骤!)

**文件**: `src/DbArchiveTool.Web/Program.cs`

在 `var app = builder.Build();` 之后,添加以下代码来初始化全局 JobStorage:

```csharp
var app = builder.Build();

// 初始化 Hangfire JobStorage (不启动 Server,仅用于监控)
GlobalConfiguration.Configuration.UseSqlServerStorage(
    hangfireConnectionString,
    new SqlServerStorageOptions
    {
        CommandBatchMaxTimeout = TimeSpan.FromMinutes(5),
        SlidingInvisibilityTimeout = TimeSpan.FromMinutes(5),
        QueuePollInterval = TimeSpan.Zero,
        UseRecommendedIsolationLevel = true,
        DisableGlobalLocks = true,
        SchemaName = "Hangfire"
    });

// ... 其他中间件配置 ...
```

**重要说明**: 这一步是必须的!`AddHangfire` 只是注册了服务到 DI 容器,但 `JobStorage.Current` 需要通过 `GlobalConfiguration.Configuration.UseSqlServerStorage` 来初始化。

## 重要说明

### Web 项目 vs API 项目的 Hangfire 配置

| 配置项 | API 项目 | Web 项目 |
|--------|---------|---------|
| **Hangfire 存储** | ✅ 配置 | ✅ 配置(相同数据库) |
| **Hangfire Server** | ✅ 启动 | ❌ 不启动 |
| **Dashboard** | ✅ 启用(`/hangfire`) | ❌ 不启用 |
| **Job 执行** | ✅ 执行任务 | ❌ 只读取数据 |
| **用途** | 任务调度和执行 | 任务监控展示 |

### 为什么 Web 项目不启动 Hangfire Server?

1. **职责分离**: API 项目负责执行任务,Web 项目只负责展示
2. **避免冲突**: 同一个任务不应该被多个 Server 同时处理
3. **性能优化**: Web 项目不需要占用资源执行后台任务
4. **架构清晰**: 前端(展示) 和 后端(执行) 分离

### Schema 名称必须一致

```csharp
// API 和 Web 项目都必须使用相同的 Schema
SchemaName = "Hangfire"
```

如果 Schema 不一致,Web 项目将无法读取 API 项目创建的任务数据。

## 架构说明

```
┌──────────────────────────┐
│   DbArchiveTool.Api      │
│  (Hangfire Server)       │
├──────────────────────────┤
│ • 启动 Hangfire Server   │
│ • 执行后台任务           │
│ • 创建/管理任务          │
│ • Dashboard (/hangfire)  │
└────────┬─────────────────┘
         │
         │ 写入/读取
         ↓
┌────────────────────────────┐
│   SQL Server Database      │
│   Hangfire Schema          │
├────────────────────────────┤
│ • Job 表                   │
│ • State 表                 │
│ • Server 表                │
│ • 等...                    │
└────────┬───────────────────┘
         │
         │ 只读取
         ↓
┌──────────────────────────┐
│   DbArchiveTool.Web      │
│  (Hangfire Client Only)  │
├──────────────────────────┤
│ • 不启动 Server          │
│ • 只读取任务数据         │
│ • 自定义监控页面         │
│ • /archive-jobs          │
└──────────────────────────┘
```

## 验证

1. **检查配置**: 确保 `appsettings.json` 中的连接字符串正确

2. **启动 API 项目**: 先启动 API 项目以确保 Hangfire 表结构存在
   ```bash
   cd src/DbArchiveTool.Api
   dotnet run
   ```

3. **启动 Web 项目**: 然后启动 Web 项目
   ```bash
   cd src/DbArchiveTool.Web
   dotnet run
   ```

4. **访问页面**: 打开浏览器,访问 `/archive-jobs` 页面

5. **检查功能**:
   - ✅ 统计卡片显示数据
   - ✅ 任务列表加载成功
   - ✅ 定时任务列表显示
   - ✅ 无 JobStorage 错误

## 常见问题

### Q1: 为什么不使用 HttpClient 调用 API 获取数据?

**A**: 直接访问数据库更高效,避免了额外的网络开销和 API 层的序列化/反序列化。而且 Hangfire 本身就设计为多客户端访问同一存储。

### Q2: 是否需要在 Web 项目中安装其他 Hangfire 包?

**A**: 不需要。只需要:
- `Hangfire.AspNetCore` (已有)
- `Hangfire.SqlServer` (新增)

### Q3: 如果数据库连接失败会怎样?

**A**: Web 项目启动时会抛出异常,提示配置 `ConnectionStrings:HangfireDatabase`。

### Q4: Web 项目会修改 Hangfire 数据吗?

**A**: 是的,当用户点击"删除"、"重试"、"触发定时任务"等操作时,Web 项目会通过 `BackgroundJob` 和 `RecurringJob` API 修改数据。但不会执行任务本身。

## 总结

通过在 Web 项目中配置 Hangfire 存储(但不启动 Server),实现了:

- ✅ Web 项目可以读取 Hangfire 任务数据
- ✅ Web 项目可以管理任务(删除、重试等)
- ✅ 保持架构清晰(API 执行,Web 展示)
- ✅ 避免重复执行任务
- ✅ 统一的监控界面
