# Docker 快速启动指南

> 使用 Docker 一键部署数据归档工具完整环境

## 🚀 5 分钟快速启动

### 前置要求
- ✅ 已安装 [Docker Desktop](https://www.docker.com/products/docker-desktop/)（Windows/Mac）或 Docker Engine（Linux）
- ✅ 可用内存 ≥ 4GB
- ✅ 可用磁盘空间 ≥ 10GB

### 一键启动

```powershell
# 1. 克隆或下载项目
cd F:\tmp\数据归档工具\DBManageTool

# 2. 启动所有服务（SQL Server + API + Web）
docker-compose up -d

# 3. 等待服务就绪（约 60-90 秒）
Write-Host "等待服务启动..." -ForegroundColor Yellow
Start-Sleep -Seconds 90

# 4. 验证服务状态
docker-compose ps

# 5. 打开浏览器
Start-Process "http://localhost:5000"  # Web 界面
Start-Process "http://localhost:5001/swagger"  # API 文档
```

### 服务地址

| 服务 | 地址 | 说明 |
|------|------|------|
| **Web 界面** | http://localhost:5000 | Blazor 前端 |
| **API 服务** | http://localhost:5001 | REST API |
| **Swagger** | http://localhost:5001/swagger | API 文档 |
| **Hangfire** | http://localhost:5001/hangfire | 任务监控 |
| **SQL Server** | localhost:1433 | 数据库（sa / Archive@Pass123!） |

### 注册管理员

```powershell
# 方法 1: 使用 PowerShell
$adminData = @{
    username = "admin"
    password = "Admin@123456"
    email = "admin@example.com"
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:5001/api/v1/auth/register" `
    -Method Post `
    -Body $adminData `
    -ContentType "application/json"

# 方法 2: 使用 Swagger UI
# 访问 http://localhost:5001/swagger
# 找到 POST /api/v1/auth/register
# 点击 "Try it out"，输入用户信息
```

## 📋 常用命令

### 服务管理

```powershell
# 启动所有服务
docker-compose up -d

# 停止所有服务
docker-compose down

# 重启服务
docker-compose restart

# 查看服务状态
docker-compose ps

# 查看日志
docker-compose logs -f

# 只查看 API 日志
docker-compose logs -f api

# 查看资源使用
docker stats
```

### 数据管理

```powershell
# 备份数据库
docker exec dbarchive-sqlserver /opt/mssql-tools18/bin/sqlcmd `
    -S localhost -U sa -P "Archive@Pass123!" -C `
    -Q "BACKUP DATABASE [DbArchiveTool] TO DISK = '/var/opt/mssql/backup/DbArchiveTool.bak'"

# 连接到 SQL Server（使用 SSMS）
# Server: localhost,1433
# Login: sa
# Password: Archive@Pass123!

# 进入 API 容器
docker exec -it dbarchive-api bash

# 进入数据库容器
docker exec -it dbarchive-sqlserver bash
```

### 清理环境

```powershell
# 停止并删除所有容器
docker-compose down

# 同时删除数据卷（⚠️ 会清空所有数据）
docker-compose down -v

# 清理未使用的 Docker 资源
docker system prune -a
```

## 🔧 自定义配置

### 修改数据库密码

编辑 `docker-compose.yml`：
```yaml
services:
  sqlserver:
    environment:
      - SA_PASSWORD=YourNewPassword@2024!  # 修改这里
```

或使用环境变量：
```powershell
$env:SQL_SA_PASSWORD="YourNewPassword@2024!"
docker-compose up -d
```

### 修改端口

编辑 `docker-compose.yml`：
```yaml
services:
  web:
    ports:
      - "8080:5000"  # 映射到 8080 端口
  api:
    ports:
      - "8081:5001"  # 映射到 8081 端口
```

### 启用开发模式

```powershell
# 使用开发配置（支持热重载）
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up
```

## 🐛 故障排查

### 容器无法启动

```powershell
# 查看详细日志
docker-compose logs

# 检查端口占用
netstat -ano | findstr "5000 5001 1433"

# 停止占用端口的进程
Stop-Process -Id <PID> -Force
```

### SQL Server 初始化失败

```powershell
# 查看 SQL Server 日志
docker-compose logs sqlserver

# 常见原因：
# 1. 密码不符合复杂度要求（至少 8 位，包含大小写字母、数字、特殊字符）
# 2. 内存不足（SQL Server 需要至少 2GB）
# 3. 端口 1433 被占用
```

### API 无法连接数据库

```powershell
# 检查网络连通性
docker exec dbarchive-api ping sqlserver

# 检查数据库是否就绪
docker exec dbarchive-sqlserver /opt/mssql-tools18/bin/sqlcmd `
    -S localhost -U sa -P "Archive@Pass123!" -C -Q "SELECT @@VERSION"

# 等待更长时间（SQL Server 初始化需要 30-60 秒）
Start-Sleep -Seconds 60
docker-compose restart api
```

### Web 界面无法访问

```powershell
# 检查 API 是否正常
Invoke-RestMethod -Uri "http://localhost:5001/api/v1/health"

# 检查 Web 容器日志
docker-compose logs web

# 重启 Web 服务
docker-compose restart web
```

## 📚 下一步

### 功能使用

1. **配置数据源**
   - 访问 Web 界面 → "数据源管理"
   - 添加源数据库和目标数据库连接

2. **创建归档任务**
   - "分区管理" → 选择表 → "归档"
   - 配置归档参数（BCP / BulkCopy）

3. **监控任务执行**
   - 访问 "任务监控" 或 Hangfire Dashboard
   - 查看任务状态和执行日志

### 生产部署

- 📖 查看完整的[生产环境部署指南](./生产环境部署指南.md)
- 🔒 配置 HTTPS 证书
- 🔐 修改默认密码
- 💾 配置数据备份策略
- 📊 接入监控系统

### 获取帮助

- 📝 [技术文档](../Docs/)
- 🐛 [问题报告](https://github.com/your-repo/issues)
- 💬 [讨论区](https://github.com/your-repo/discussions)

## 🎉 完成

现在您已经成功部署了数据归档工具！

访问 http://localhost:5000 开始使用。

---

**版本**: v1.0  
**更新日期**: 2025-11-14
