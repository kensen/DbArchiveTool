# BCP/BulkCopy 预检功能实现总结

## 完成时间

2025年11月5日

## 实现概述

为 BCP 和 BulkCopy 归档模式添加了预检查(Inspection)功能,在执行归档前自动检测潜在问题,并提供自动修复建议。

## 后端实现

### 1. API 端点

新增两个检查端点:

```csharp
// PartitionArchiveController.cs
[HttpPost("bcp/inspect")]
public async Task<IActionResult> InspectBcpAsync([FromBody] BcpArchiveInspectDto request)

[HttpPost("bulkcopy/inspect")]
public async Task<IActionResult> InspectBulkCopyAsync([FromBody] BulkCopyArchiveInspectDto request)
```

### 2. 请求/响应模型

**请求模型**:
- `BcpArchiveInspectDto`: DataSourceId, SchemaName, TableName, SourcePartitionKey, TargetTable, TargetDatabase, TempDirectory, RequestedBy
- `BulkCopyArchiveInspectDto`: 类似BCP,但不包含 TempDirectory

**响应模型**:
```csharp
public record ArchiveInspectionResultDto
{
    public bool CanExecute { get; init; }                                    // 是否可执行
    public bool TargetTableExists { get; init; }                             // 目标表是否存在
    public bool HasRequiredPermissions { get; init; }                        // 是否具备权限
    public string? BcpCommandPath { get; init; }                             // BCP命令路径(仅BCP模式)
    public List<ArchiveInspectionIssue> BlockingIssues { get; init; }        // 阻塞问题
    public List<ArchiveInspectionIssue> Warnings { get; init; }              // 警告信息
    public List<ArchiveInspectionAutoFixStep> AutoFixSteps { get; init; }    // 自动修复步骤
}

public record ArchiveInspectionIssue(string Code, string Message, string? Recommendation);
public record ArchiveInspectionAutoFixStep(string Code, string Description, string Action);
```

### 3. 应用服务实现

`PartitionArchiveAppService.cs` 新增两个检查方法:

#### InspectBcpAsync (约80行)

检查项目:
1. ✅ **数据源存在性**: 验证 DataSourceId 是否有效
2. ⏳ **目标表存在性**: 框架已实现,TODO标记需要数据库查询
3. ⏳ **BCP命令可用性**: 框架已实现,TODO标记需要系统检查 bcp.exe
4. ✅ **临时目录存在性**: 使用 `Directory.Exists()` 检查
5. ⏳ **执行权限**: 框架已实现,TODO标记需要权限查询

**自动修复建议**:
- 缺失目标表 → 建议执行 `CREATE TABLE [target] AS (SELECT TOP 0 * FROM [source])`
- 缺失临时目录 → 警告(系统可自动创建)
- 缺失BCP命令 → 阻塞,提示安装 SQL Server 客户端工具

#### InspectBulkCopyAsync (约60行)

检查项目:
1. ✅ **数据源存在性**: 验证 DataSourceId 是否有效
2. ⏳ **目标表存在性**: 框架已实现,TODO标记需要数据库查询
3. ⏳ **INSERT权限**: 框架已实现,TODO标记需要权限查询

**自动修复建议**:
- 缺失目标表 → 建议执行 `CREATE TABLE [target] AS (SELECT TOP 0 * FROM [source])`

### 4. 错误代码定义

| 错误代码 | 说明 | 级别 | 模式 |
|---------|------|------|------|
| DS001 | 数据源不存在 | 阻塞 | 通用 |
| TABLE001 | 目标表不存在 | 警告(可自动修复) | 通用 |
| BCP001 | BCP命令不可用 | 阻塞 | BCP |
| DIR001 | 临时目录不存在 | 警告 | BCP |
| PERM001 | 缺少必需权限 | 阻塞 | 通用 |

### 5. 自动修复步骤代码

| 步骤代码 | 说明 | 操作 |
|---------|------|------|
| CREATE_TARGET_TABLE | 创建目标表 | `CREATE TABLE {target} AS (SELECT TOP 0 * FROM {source})` |
| CREATE_TEMP_DIR | 创建临时目录 | 系统自动创建目录(BCP模式) |

## 前端实现

### 1. API客户端扩展

`PartitionArchiveApiClient.cs` 新增两个方法:

```csharp
public async Task<Result<ArchiveInspectionResultDto>> InspectBcpAsync(
    BcpArchiveInspectRequest request, CancellationToken cancellationToken = default)

public async Task<Result<ArchiveInspectionResultDto>> InspectBulkCopyAsync(
    BulkCopyArchiveInspectRequest request, CancellationToken cancellationToken = default)
```

### 2. 向导代码更新

`PartitionArchiveWizard.razor.cs` 新增:

**字段**:
```csharp
private ArchiveInspectionResultDto? _bcpInspectionResult;
private ArchiveInspectionResultDto? _bulkCopyInspectionResult;
```

**方法**:
```csharp
private async Task<bool> RunBcpInspectionAsync()      // BCP预检逻辑
private async Task<bool> RunBulkCopyInspectionAsync() // BulkCopy预检逻辑
```

**流程变更**:
- Step 1 → Step 2 时,根据选中的模式(Switch/BCP/BulkCopy)调用对应的预检方法
- Switch模式: 调用 `RunInspectionAsync()`
- BCP模式: 调用 `RunBcpInspectionAsync()`
- BulkCopy模式: 调用 `RunBulkCopyInspectionAsync()`

### 3. UI显示更新

`PartitionArchiveWizard.razor` 更新 Step 2 预检结果显示:

**原来的显示**(简化提示):
```html
<Alert Type="Info">
    <p>该模式暂不支持自动预检,请确认以下条件已满足:</p>
    <ul>
        <li>已安装 SQL Server 客户端工具(包含 bcp.exe)</li>
        <li>拥有 bulkadmin 或 sysadmin 服务器权限</li>
        ...
    </ul>
</Alert>
```

**现在的显示**(实际检查结果):
```html
@if (currentInspection != null)
{
    @* 总体结果 *@
    <Alert Message="预检通过/存在阻塞问题" Type="Success/Error" />
    
    @* 阻塞问题列表 *@
    @if (currentInspection.BlockingIssues.Any())
    {
        <div class="inspection-section">
            <h4>阻塞问题</h4>
            <Alert Type="Error">
                <ul>
                    @foreach (var issue in currentInspection.BlockingIssues)
                    {
                        <li>@issue.Code - @issue.Message</li>
                    }
                </ul>
            </Alert>
        </div>
    }
    
    @* 警告信息列表 *@
    @if (currentInspection.Warnings.Any()) { ... }
    
    @* 自动修复建议 *@
    @if (currentInspection.AutoFixSteps.Any())
    {
        <div class="inspection-section">
            <h4>可自动修复</h4>
            <Alert Type="Info">
                <p>系统可以自动执行以下修复操作:</p>
                <ul>
                    @foreach (var step in currentInspection.AutoFixSteps)
                    {
                        <li>@step.Code - @step.Description</li>
                    }
                </ul>
                <Button Type="Primary" Disabled>一键修复 (开发中)</Button>
            </Alert>
        </div>
    }
}
```

### 4. 用户体验改进

- ✅ Step 1 填写参数后,点击"下一步"自动触发预检
- ✅ 预检加载时显示 Loading 动画
- ✅ 预检完成后,通过消息提示告知结果
  - 成功: `Message.Success("BCP预检通过,可以执行归档")`
  - 有阻塞: `Message.Error("发现阻塞问题:\n• ...")`
  - 有警告: `Message.Warning("警告信息:\n• ...")`
- ✅ Step 2 显示详细的检查结果,包括:
  - 总体状态(绿色成功/红色失败)
  - 服务器信息(地址、认证方式)
  - 归档参数确认
  - 阻塞问题列表(红色Alert)
  - 警告信息列表(黄色Alert)
  - 自动修复建议(蓝色Alert + 占位按钮)

## 待完善的TODO项

### 高优先级

1. **目标表存在性检查**
   - 位置: `PartitionArchiveAppService.InspectBcpAsync()` 和 `InspectBulkCopyAsync()`
   - 实现方式: 通过 Dapper 查询系统表
   ```csharp
   // 查询目标表是否存在
   var targetTableExists = await _dbConnection.QuerySingleOrDefaultAsync<int>(
       "SELECT COUNT(*) FROM sys.tables t INNER JOIN sys.schemas s ON t.schema_id = s.schema_id " +
       "WHERE s.name = @Schema AND t.name = @Table",
       new { Schema = targetSchema, Table = targetTable }) > 0;
   ```

2. **BCP命令可用性检查**
   - 位置: `PartitionArchiveAppService.InspectBcpAsync()`
   - 实现方式: 调用系统命令检查 bcp.exe
   ```csharp
   // 检查BCP命令
   var bcpPath = FindBcpCommand();
   bool hasBcpCommand = !string.IsNullOrEmpty(bcpPath);
   
   private string? FindBcpCommand()
   {
       try
       {
           var process = Process.Start(new ProcessStartInfo
           {
               FileName = "where",
               Arguments = "bcp",
               RedirectStandardOutput = true,
               UseShellExecute = false
           });
           return process?.StandardOutput.ReadLine();
       }
       catch { return null; }
   }
   ```

3. **权限验证**
   - 位置: 两个检查方法
   - 实现方式: 查询 SQL Server 权限视图
   ```csharp
   // BCP模式: 检查bulkadmin角色
   var hasBulkAdminRole = await _dbConnection.QuerySingleOrDefaultAsync<int>(
       "SELECT COUNT(*) FROM sys.server_role_members rm " +
       "INNER JOIN sys.server_principals rp ON rm.role_principal_id = rp.principal_id " +
       "WHERE rp.name = 'bulkadmin' AND rm.member_principal_id = SUSER_ID()") > 0;
   
   // BulkCopy模式: 检查INSERT权限
   var hasInsertPermission = await _dbConnection.QuerySingleOrDefaultAsync<int>(
       "SELECT COUNT(*) FROM fn_my_permissions(@TargetTable, 'OBJECT') " +
       "WHERE permission_name = 'INSERT'",
       new { TargetTable = $"{targetSchema}.{targetTable}" }) > 0;
   ```

### 中优先级

4. **自动修复执行逻辑**
   - 添加新的 API 端点: `POST /api/v1/partition-archive/bcp/autofix` 和 `/bulkcopy/autofix`
   - 实现 `ExecuteAutoFixAsync()` 方法,根据选中的修复步骤执行:
     - `CREATE_TARGET_TABLE`: 执行 CREATE TABLE 语句
     - `CREATE_TEMP_DIR`: 使用 `Directory.CreateDirectory()`
   - 前端启用"一键修复"按钮,调用自动修复接口

5. **检查结果缓存**
   - 在 Step 2 预检成功后,缓存检查结果
   - 如果用户返回 Step 1 修改参数,清除缓存
   - 避免重复预检相同的配置

### 低优先级

6. **进度详情显示**
   - 目标表行数统计
   - 预估传输时间计算
   - 磁盘空间检查(临时目录和目标数据库)

7. **高级检查选项**
   - 目标表结构兼容性检查(列数量、数据类型匹配)
   - 网络连通性测试(ping 目标服务器)
   - 数据库版本兼容性检查

## 代码变更统计

| 文件 | 变更类型 | 行数变化 |
|------|---------|---------|
| `PartitionArchiveController.cs` | 新增方法 | +30行 (2个端点) |
| `PartitionArchiveDtos.cs` | 新增模型 | +60行 (请求/响应DTO) |
| `IPartitionArchiveAppService.cs` | 新增接口 | +80行 (方法签名+记录类型) |
| `PartitionArchiveAppService.cs` | 新增方法 | +150行 (2个检查方法) |
| `PartitionArchiveApiClient.cs` | 新增方法 | +40行 (2个API调用) |
| `PartitionArchiveWizard.razor.cs` | 新增方法 | +140行 (2个前端检查方法) |
| `PartitionArchiveWizard.razor` | 修改UI | +70行 (检查结果显示) |
| **总计** | - | **+570行** |

## 编译和测试状态

- ✅ 后端编译成功 (Application + Api + Infrastructure)
- ✅ 前端编译成功 (Web项目,7个警告均为已知的AntDesign组件废弃警告)
- ✅ 完整解决方案编译成功
- ⏳ 集成测试待执行(需要实际数据库环境)
- ⏳ E2E测试待编写

## 后续改进方向

### 短期(1-2周)

1. **完善TODO标记的检查项**
   - 实现目标表存在性检查
   - 实现BCP命令可用性检查
   - 实现权限验证查询

2. **自动修复功能完整实现**
   - 添加自动修复API端点
   - 实现CREATE TABLE逻辑
   - 前端启用"一键修复"按钮

3. **集成测试**
   - 在测试数据库上验证BCP预检
   - 验证BulkCopy预检
   - 测试各种错误场景(表不存在、权限不足等)

### 中期(1个月)

4. **增强检查项**
   - 磁盘空间检查
   - 目标表结构兼容性验证
   - 网络连通性测试

5. **性能优化**
   - 并行执行多个检查项
   - 缓存常用查询结果
   - 优化数据库查询

### 长期(持续优化)

6. **智能建议**
   - 根据历史归档数据,智能推荐超时时间
   - 根据表大小,智能推荐批次大小
   - 根据网络环境,自动选择最优归档模式

7. **监控和告警**
   - 实时显示归档进度
   - 预估剩余时间
   - 异常情况自动告警

## 相关文档

- [BCP和BulkCopy超时时间说明.md](./BCP和BulkCopy超时时间说明.md) - 详细解释超时参数的作用和影响
- [开发规范与项目结构.md](./开发规范与项目结构.md) - 项目架构和编码规范
- [数据模型与API规范.md](./数据模型与API规范.md) - API设计规范

## 总结

本次更新为 BCP 和 BulkCopy 归档模式添加了完整的预检查框架:

✅ **已完成**:
- 后端 API 端点和应用服务框架
- 请求/响应数据模型定义
- 前端检查逻辑和 UI 显示
- 数据源存在性检查
- 临时目录存在性检查
- 自动修复建议生成

⏳ **待完善**:
- 目标表存在性数据库查询(TODO标记)
- BCP命令可用性系统检查(TODO标记)
- 权限验证SQL查询(TODO标记)
- 自动修复执行逻辑
- 集成测试验证

📈 **价值**:
- 大幅降低归档失败率(提前发现问题)
- 改善用户体验(清晰的错误提示和修复建议)
- 提高数据安全性(避免误操作导致数据丢失)
- 为后续智能优化打下基础

现在用户在界面上可以看到完整的预检结果,包括阻塞问题、警告信息和自动修复建议。关于超时时间的作用,已在 `BCP和BulkCopy超时时间说明.md` 文档中详细说明。
