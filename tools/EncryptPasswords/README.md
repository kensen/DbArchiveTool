# 密码加密迁移工具

## 目的

将 `ArchiveDataSource` 表中现有的明文密码加密，确保数据库安全性。

## 功能

- 扫描 `ArchiveDataSource` 表中所有未删除的数据源
- 检测并加密 `Password` 和 `TargetPassword` 字段中的明文密码
- 自动跳过已经加密的密码（通过 `ENCRYPTED:` 前缀识别）
- 提供详细的迁移日志

## 使用方法

### 1. 构建工具

```powershell
cd f:\Source\DBManageTool\DBManageTool\tools\EncryptPasswords
dotnet build
```

### 2. 运行迁移

使用默认连接字符串（localhost）：

```powershell
dotnet run
```

使用自定义连接字符串：

```powershell
dotnet run "Server=YourServer;Database=DbArchiveTool;Trusted_Connection=True;TrustServerCertificate=True"
```

或使用 SQL 认证：

```powershell
dotnet run "Server=YourServer;Database=DbArchiveTool;User Id=sa;Password=YourPassword;TrustServerCertificate=True"
```

## 输出示例

```
========================================
密码加密迁移工具
========================================

连接字符串: Server=localhost;Database=DbArchiveTool;Trusted_Connection=True;TrustServerCertificate=True

找到 2 个数据源需要处理

处理数据源: MES数据库 (a1b2c3d4-...)
  - 加密 Password 字段
  - 加密 TargetPassword 字段
  ✓ 已更新

处理数据源: 测试数据库 (e5f6g7h8-...)
  - Password 已加密，跳过
  - TargetPassword 已加密，跳过
  - 无需更新

========================================
迁移完成！
  已加密: 1 个数据源
  已跳过: 1 个数据源
  总计: 2 个数据源
========================================
```

## 注意事项

⚠️ **重要安全提示**

1. **备份数据库**：运行此工具前，请先备份 `DbArchiveTool` 数据库
2. **测试环境先行**：建议先在测试环境验证后再在生产环境运行
3. **密钥持久化**：Data Protection 密钥默认存储在用户配置文件中，确保：
   - 应用程序使用相同的用户账户运行
   - 或配置 Data Protection 使用共享密钥存储（文件系统、Redis 等）
4. **只运行一次**：工具会自动跳过已加密密码，可以安全地多次运行
5. **不可逆操作**：加密后无法恢复原始明文密码（除非使用相同的 Data Protection 密钥解密）

## 加密机制

- **算法**：ASP.NET Core Data Protection API
- **Purpose**：`DbArchiveTool.PasswordProtection`
- **标识前缀**：`ENCRYPTED:` - 用于区分加密和明文密码
- **向后兼容**：应用程序会自动检测加密状态，兼容现有明文密码

## 故障排除

### 问题：无法连接数据库

**解决方案**：检查连接字符串，确保数据库服务器可访问且连接参数正确。

### 问题：密钥不匹配导致解密失败

**解决方案**：
- 确保 API 应用和迁移工具使用相同的 Data Protection 配置
- 在生产环境中配置持久化密钥存储

### 问题：需要回滚

**解决方案**：从备份恢复数据库。
