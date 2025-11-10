# BCP 工具配置指南

## 问题说明

当使用 BCP 归档方式时,系统报错:

```
BCP001 - 未找到 bcp.exe 命令
建议:请安装 SQL Server 客户端工具或确保 bcp.exe 在系统 PATH 中
```

**这个错误表示:**
- 运行 `DbArchiveTool.Api` 服务的服务器上缺少 BCP 工具
- BCP (Bulk Copy Program) 是 SQL Server 提供的命令行工具,用于高性能数据导入导出

## 配置位置

需要在 **运行 DbArchiveTool API 服务的服务器** 上安装和配置 BCP 工具,而不是:
- ❌ 源数据库服务器
- ❌ 目标数据库服务器
- ❌ 客户端浏览器
- ✅ **DbArchiveTool API 应用服务器** (运行 `DbArchiveTool.Api` 的机器)

## 安装方法

### 方法一:安装 SQL Server 命令行工具 (推荐)

1. **下载 Microsoft Command Line Utilities for SQL Server**
   - 访问: https://aka.ms/sqlcmd
   - 或搜索: "SQL Server Command Line Utilities download"

2. **安装步骤** (Windows)
   ```powershell
   # 下载并运行安装程序
   # 文件名通常为: MsSqlCmdLnUtils.msi
   # 或使用 winget 安装:
   winget install Microsoft.SQLServerCmdLineUtilities
   ```

3. **安装步骤** (Linux)
   ```bash
   # Ubuntu/Debian
   curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
   curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | tee /etc/apt/sources.list.d/msprod.list
   apt-get update
   apt-get install -y mssql-tools unixodbc-dev

   # 添加到 PATH
   echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
   source ~/.bashrc
   ```

### 方法二:安装 SQL Server 客户端工具

安装 SQL Server Management Studio (SSMS) 或 SQL Server 完整安装包,会自动包含 BCP 工具。

**SSMS 安装:**
- 下载地址: https://aka.ms/ssmsfullsetup
- BCP 工具会安装到: `C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\170\Tools\Binn\`

### 方法三:仅安装 BCP 工具 (最轻量)

从 SQL Server 功能包单独安装:
- 访问: https://www.microsoft.com/en-us/download/details.aspx?id=105656
- 选择 "Microsoft SQL Server 2022 Feature Pack"
- 下载: `ENU\x64\MsSqlCmdLnUtils.msi`

## 验证安装

安装完成后,在命令行验证:

```powershell
# 检查 BCP 是否在 PATH 中
bcp -v

# 预期输出类似:
# BCP - Bulk Copy Program for Microsoft SQL Server.
# Copyright (C) Microsoft Corporation. All Rights Reserved.
# Version: 16.0.1000.6
```

如果提示 "bcp 不是内部或外部命令",请执行下一步。

## 配置环境变量 PATH

如果安装后仍然找不到 BCP,需要添加到系统 PATH:

### Windows 配置

1. **找到 BCP 安装路径** (常见位置):
   ```
   C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\170\Tools\Binn\
   C:\Program Files\Microsoft SQL Server\160\Tools\Binn\
   C:\Program Files\Microsoft SQL Server\150\Tools\Binn\
   ```

2. **添加到系统 PATH**:
   - 右键 "此电脑" → "属性" → "高级系统设置"
   - 点击 "环境变量"
   - 在 "系统变量" 中找到 `Path`,点击 "编辑"
   - 点击 "新建",添加 BCP 所在目录路径
   - 点击 "确定" 保存

3. **重启 PowerShell/CMD 窗口**,再次验证 `bcp -v`

### Linux 配置

```bash
# 查找 BCP 路径
which bcp

# 如果没有找到,手动添加到 PATH
export PATH="$PATH:/opt/mssql-tools/bin"

# 永久生效
echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
source ~/.bashrc
```

## 在 DbArchiveTool 中配置自定义 BCP 路径

如果不想修改系统 PATH,可以在代码中指定 BCP 工具的完整路径:

### 配置文件方式 (推荐)

在 `appsettings.json` 中添加:

```json
{
  "Archive": {
    "BcpToolPath": "C:\\Program Files\\Microsoft SQL Server\\Client SDK\\ODBC\\170\\Tools\\Binn\\bcp.exe"
  }
}
```

然后在代码中读取配置:

```csharp
// 在 BcpOptions 中设置
var bcpOptions = new BcpOptions
{
    BcpToolPath = configuration["Archive:BcpToolPath"], // 从配置读取
    BatchSize = 10000,
    MaxErrors = 10,
    // ... 其他选项
};
```

### 代码硬编码方式 (不推荐)

直接在调用时指定:

```csharp
var bcpOptions = new BcpOptions
{
    BcpToolPath = @"C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\170\Tools\Binn\bcp.exe",
    BatchSize = 10000,
    // ...
};
```

## 常见问题

### Q1: 安装后仍然报错 "未找到 bcp.exe"?

**检查清单:**
1. 重启运行 DbArchiveTool API 的应用程序/服务
2. 如果是 Windows 服务,重启服务
3. 如果是 IIS 应用池,回收应用池
4. 重启服务器 (确保环境变量生效)

### Q2: 权限问题 "Access Denied"?

BCP 需要:
- 源数据库的 SELECT 权限
- 目标数据库的 INSERT/BULK INSERT 权限
- 临时目录的读写权限

### Q3: 使用 Docker 容器部署?

在 Dockerfile 中添加:

```dockerfile
FROM mcr.microsoft.com/dotnet/aspnet:8.0

# 安装 SQL Server 命令行工具
RUN apt-get update && \
    apt-get install -y curl apt-transport-https && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y mssql-tools unixodbc-dev && \
    echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc

# ... 其他 Dockerfile 配置
```

### Q4: BCP 和 BulkCopy 有什么区别?

| 特性 | BCP | BulkCopy |
|------|-----|----------|
| 原理 | 命令行工具,导出到文件再导入 | .NET API,内存流式传输 |
| 依赖 | 需要安装 BCP 工具 | 无需额外安装 |
| 性能 | 极高 (适合大数据) | 高 (适合中等数据) |
| 临时文件 | 需要 | 不需要 |
| 跨服务器 | 支持 | 支持 |
| 推荐场景 | 超大表 (GB/TB 级别) | 一般表 (MB/GB 级别) |

**建议:**
- 如果无法安装 BCP,使用 **BulkCopy** 方式归档
- BulkCopy 不依赖外部工具,纯 .NET 实现

## 测试 BCP 功能

安装配置完成后,在 DbArchiveTool Web UI 中:

1. 进入 "分区归档向导"
2. 选择源表和目标
3. 在 "归档方式" 中选择 **"BCP"**
4. 点击 "开始预检"
5. 确认 "BCP001" 错误消失

或者使用 API 测试:

```powershell
# 测试 BCP 预检接口
Invoke-RestMethod -Uri "http://localhost:5083/api/v1/partition-archive/bcp/inspect" `
  -Method POST `
  -ContentType "application/json" `
  -Body @"
{
  "dataSourceId": "your-datasource-guid",
  "schemaName": "dbo",
  "tableName": "YourTable",
  "targetTable": "dbo.YourTable_Archive",
  "tempDirectory": "C:\\Temp\\BcpArchive"
}
"@
```

检查返回结果中 `blockingIssues` 是否还包含 `BCP001`。

## 总结

**必须在运行 DbArchiveTool.Api 服务的机器上:**
1. ✅ 安装 SQL Server 命令行工具 (包含 bcp.exe)
2. ✅ 确保 bcp.exe 在系统 PATH 中,或在配置中指定完整路径
3. ✅ 重启 API 服务使配置生效
4. ✅ 测试确认 BCP001 错误消失

**或者使用替代方案:**
- 使用 **BulkCopy** 归档方式,无需安装 BCP 工具
