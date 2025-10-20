# Serilog 配置指南

## 1. 安装 NuGet 包

```bash
# Api 项目
cd src/DbArchiveTool.Api
dotnet add package Serilog.AspNetCore
dotnet add package Serilog.Sinks.File
dotnet add package Serilog.Sinks.Console

# Web 项目
cd ../DbArchiveTool.Web
dotnet add package Serilog.AspNetCore
dotnet add package Serilog.Sinks.File
dotnet add package Serilog.Sinks.Console
```

## 2. 修改 Program.cs (Api 项目)

```csharp
using Serilog;

// 在 builder 创建之前配置 Serilog
Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .MinimumLevel.Override("Microsoft.AspNetCore", LogEventLevel.Warning)
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .WriteTo.File(
        path: "logs/api-.log",
        rollingInterval: RollingInterval.Day,
        retainedFileCountLimit: 30,
        outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {SourceContext} {Message:lj}{NewLine}{Exception}"
    )
    .CreateLogger();

try
{
    Log.Information("启动 DbArchiveTool API 服务");
    
    var builder = WebApplication.CreateBuilder(args);
    
    // 替换默认日志为 Serilog
    builder.Host.UseSerilog();
    
    // ... 其余代码不变
    
    var app = builder.Build();
    
    // ... 中间件配置
    
    app.Run();
}
catch (Exception ex)
{
    Log.Fatal(ex, "应用启动失败");
    throw;
}
finally
{
    Log.CloseAndFlush();
}
```

## 3. 或使用 appsettings.json 配置 (推荐)

**appsettings.json** 添加:
```json
{
  "Serilog": {
    "Using": ["Serilog.Sinks.Console", "Serilog.Sinks.File"],
    "MinimumLevel": {
      "Default": "Information",
      "Override": {
        "Microsoft.AspNetCore": "Warning",
        "Microsoft.EntityFrameworkCore": "Warning",
        "System.Net.Http.HttpClient": "Warning"
      }
    },
    "WriteTo": [
      {
        "Name": "Console",
        "Args": {
          "outputTemplate": "[{Timestamp:HH:mm:ss} {Level:u3}] {SourceContext} {Message:lj}{NewLine}{Exception}"
        }
      },
      {
        "Name": "File",
        "Args": {
          "path": "logs/api-.log",
          "rollingInterval": "Day",
          "retainedFileCountLimit": 30,
          "outputTemplate": "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {SourceContext} {Message:lj}{NewLine}{Exception}"
        }
      }
    ],
    "Enrich": ["FromLogContext"]
  }
}
```

**Program.cs** 简化为:
```csharp
using Serilog;

var builder = WebApplication.CreateBuilder(args);

// 从配置读取 Serilog 设置
builder.Host.UseSerilog((context, services, configuration) => configuration
    .ReadFrom.Configuration(context.Configuration)
    .ReadFrom.Services(services)
);

// ... 其余代码
```

## 4. 日志输出位置

- **控制台**: 终端窗口实时输出
- **文件**: `DbArchiveTool.Api/logs/api-20251011.log`
- **文件**: `DbArchiveTool.Web/logs/web-20251011.log`
- **滚动策略**: 每天一个文件,保留最近 30 天

## 5. 使用示例

```csharp
public class PartitionExecutionAppService
{
    private readonly ILogger<PartitionExecutionAppService> _logger;
    
    public PartitionExecutionAppService(ILogger<PartitionExecutionAppService> logger)
    {
        _logger = logger;
    }
    
    public async Task<Result<Guid>> StartAsync(StartPartitionExecutionDto dto)
    {
        _logger.LogInformation("开始发起分区执行任务, 配置ID: {ConfigId}, 数据源: {DataSourceId}", 
            dto.PartitionConfigurationId, dto.DataSourceId);
        
        try
        {
            // ... 业务逻辑
            
            _logger.LogInformation("分区执行任务创建成功, 任务ID: {TaskId}", taskId);
            return Result<Guid>.Success(taskId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "创建分区执行任务失败, 配置ID: {ConfigId}", dto.PartitionConfigurationId);
            return Result<Guid>.Failure($"创建任务失败: {ex.Message}");
        }
    }
}
```

## 6. 日志查看

**实时查看**:
```bash
# PowerShell
Get-Content -Path "logs\api-20251011.log" -Wait -Tail 50

# Linux/Mac
tail -f logs/api-20251011.log
```

**搜索错误**:
```bash
# PowerShell
Select-String -Path "logs\*.log" -Pattern "error|exception" -CaseSensitive:$false

# Linux/Mac
grep -i "error\|exception" logs/*.log
```

