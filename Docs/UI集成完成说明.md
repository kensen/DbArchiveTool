# 任务调度模块 - UI 集成完成

## 📋 完成的改动

### 1. **导航菜单** (`Shared/NavMenu.razor`)

添加了"任务调度"菜单项:

```razor
<div class="nav-item px-3">
    <NavLink class="nav-link" href="partition-executions/monitor">
        <span class="oi oi-task" aria-hidden="true"></span> 任务调度
    </NavLink>
</div>
```

**位置**: 数据源管理 和 日志审计 之间

**图标**: `oi-task` (任务图标)

**路由**: `/partition-executions/monitor` (监控页面)

---

### 2. **分区配置草稿 - 执行按钮** (`Pages/Partitions.razor`)

在"配置草稿"表格的操作列添加了"执行分区"按钮:

```razor
<SpaceItem>
    <Button Type="@ButtonType.Link"
            Size="@ButtonSize.Small"
            Disabled="@(context.IsCommitted || context.IsPartitioned)"
            OnClick="() => ExecutePartitionAsync(context)">执行分区</Button>
</SpaceItem>
```

**按钮位置**: 编辑 和 删除 按钮之间

**禁用条件**: 
- `IsCommitted = true` (已执行的配置)
- `IsPartitioned = true` (目标表已是分区表)

**点击行为**: 跳转到执行发起页面,并传递配置ID

---

### 3. **执行分区方法** (`Pages/Partitions.razor`)

添加了 `ExecutePartitionAsync` 方法:

```csharp
/// <summary>
/// 执行分区(跳转到执行发起页面)
/// </summary>
private void ExecutePartitionAsync(ConfigurationDraftInfo draft)
{
    if (draft.IsCommitted)
    {
        Message.Warning("该配置已执行，无需重复执行。");
        return;
    }

    if (draft.IsPartitioned)
    {
        Message.Warning("目标表已是分区表，无法执行分区创建。");
        return;
    }

    // 跳转到执行发起页面,传递配置ID
    Navigation.NavigateTo($"/partition-executions/start/{draft.Id}");
}
```

**功能**:
1. 验证配置状态(是否已执行/是否已分区)
2. 提示警告信息
3. 导航到执行发起页面: `/partition-executions/start/{配置ID}`

---

## 🎯 用户操作流程

### **方式 1: 从导航菜单进入**

1. 点击左侧菜单 **"任务调度"**
2. 进入 `/partition-executions/monitor` 监控页面
3. 查看所有执行任务(待校验、执行中、已完成等)
4. 可以点击"详情"/"日志"/"取消"等操作

### **方式 2: 从配置草稿发起执行** (推荐)

1. 在 **"数据源管理"** 页面选择数据源
2. 进入 **"分区管理"** 页面
3. 切换到 **"配置草稿"** 标签页
4. 选择一个未执行的配置草稿
5. 点击 **"执行分区"** 按钮
6. 自动跳转到 `/partition-executions/start/{配置ID}` 执行发起页面
7. 填写执行参数:
   - 数据源选择(自动预填)
   - 备份确认
   - 备份参考
   - 执行备注
   - 优先级(0-10)
   - 强制执行(可选)
   - 执行人
8. 点击 **"发起执行"** 创建任务
9. 可选择:
   - **"查看任务详情"** → 跳转到监控页面
   - **"继续创建任务"** → 重置表单继续发起

---

## 📊 页面路由

| 路由 | 页面 | 说明 |
|------|------|------|
| `/partition-executions/monitor` | 任务监控页面 | 查看所有执行任务列表 |
| `/partition-executions/monitor/{DataSourceId}` | 任务监控页面(过滤) | 按数据源ID过滤任务 |
| `/partition-executions/start` | 执行发起页面 | 手动创建执行任务 |
| `/partition-executions/start/{PartitionConfigId}` | 执行发起页面(预填) | 预选分区配置的执行发起 |

---

## ✅ 编译状态

- **编译结果**: ✅ 成功
- **警告数**: 9 个(非关键性)
- **错误数**: 0

---

## 🎨 UI 预览

### **导航菜单**
```
┌─────────────────────┐
│ 📊 数据源管理       │
│ 📋 任务调度   ← 新增 │
│ 📝 日志审计         │
└─────────────────────┘
```

### **配置草稿操作列**
```
操作列:
[编辑] [执行分区] ← 新增 [删除]
```

### **执行发起页面**
```
分区执行发起
├── 执行参数配置
│   ├── 分区配置 (下拉选择)
│   ├── 数据源 (下拉选择)
│   ├── 备份确认 (复选框) ⚠️
│   ├── 备份参考 (输入框)
│   ├── 执行备注 (文本域)
│   ├── 优先级 (滑块 0-10)
│   ├── 强制执行 (复选框) ⚠️
│   └── 执行人 (输入框)
├── 配置详情预览
└── [发起执行] [重置]
```

### **执行结果对话框**
```
✅ 成功:
   任务ID: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
   [查看任务详情] [继续创建任务]

❌ 失败:
   错误信息: ...
   [关闭]
```

---

## 🚀 下一步

1. **启动服务**:
   ```bash
   # Terminal 1: 启动 API
   cd DbArchiveTool/src/DbArchiveTool.Api
   dotnet run
   
   # Terminal 2: 启动 Web
   cd DbArchiveTool/src/DbArchiveTool.Web
   dotnet run
   ```

2. **访问应用**: http://localhost:5011

3. **测试流程**:
   - 点击左侧 "任务调度" 菜单 → 查看监控页面
   - 进入 "数据源管理" → 选择数据源 → "分区管理"
   - 切换到 "配置草稿" 标签
   - 点击 "执行分区" 按钮
   - 填写执行参数
   - 发起执行

---

## 📝 待完成工作

- [ ] 实现 `DataSourceApi` 客户端(Start.razor.cs 中预留)
- [ ] 实现 `PartitionApi` 客户端(Start.razor.cs 中预留)
- [ ] 替换模拟数据为真实 API 调用
- [ ] 添加用户上下文,自动获取"执行人"信息
- [ ] 集成测试验证完整流程

---

## 🔗 相关文件

- `DbArchiveTool.Web/Shared/NavMenu.razor` - 导航菜单
- `DbArchiveTool.Web/Pages/Partitions.razor` - 分区管理页面
- `DbArchiveTool.Web/Pages/PartitionExecutions/Monitor.razor` - 任务监控
- `DbArchiveTool.Web/Pages/PartitionExecutions/Start.razor` - 执行发起
- `DbArchiveTool.Web/Services/PartitionExecutionApiClient.cs` - API 客户端
