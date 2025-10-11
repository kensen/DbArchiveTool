using AntDesign;
using DbArchiveTool.Web.Services;
using Microsoft.AspNetCore.Components;

namespace DbArchiveTool.Web.Pages.PartitionExecutions;

/// <summary>
/// 分区执行发起页面 - 代码逻辑部分
/// </summary>
public partial class StartBase : ComponentBase
{
    #region 依赖注入

    [Inject] private PartitionExecutionApiClient ExecutionApi { get; set; } = default!;
    [Inject] private NavigationManager Navigation { get; set; } = default!;
    [Inject] private MessageService Message { get; set; } = default!;
    [Inject] private ILogger<StartBase> Logger { get; set; } = default!;

    // TODO: 待实现的 API 客户端
    // [Inject] private DataSourceApi DataSourceApi { get; set; } = default!;
    // [Inject] private PartitionApi PartitionApi { get; set; } = default!;

    #endregion

    #region 路由参数

    /// <summary>
    /// 分区配置ID路由参数(可选,用于预选配置)
    /// </summary>
    [Parameter]
    public Guid? PartitionConfigId { get; set; }

    #endregion

    #region 状态字段

    /// <summary>
    /// 表单模型
    /// </summary>
    protected StartExecutionFormModel FormModel { get; set; } = new();

    /// <summary>
    /// 分区配置选项列表
    /// </summary>
    protected List<PartitionConfigOption> PartitionConfigs { get; set; } = new();

    /// <summary>
    /// 数据源选项列表
    /// </summary>
    protected List<DataSourceOption> DataSources { get; set; } = new();

    /// <summary>
    /// 是否正在加载配置
    /// </summary>
    protected bool LoadingConfigs { get; set; }

    /// <summary>
    /// 是否正在加载数据源
    /// </summary>
    protected bool LoadingDataSources { get; set; }

    /// <summary>
    /// 是否正在提交
    /// </summary>
    protected bool Submitting { get; set; }

    /// <summary>
    /// 选中的配置详情
    /// </summary>
    protected PartitionConfigDetail? SelectedConfigDetail { get; set; }

    /// <summary>
    /// 执行任务ID(提交成功后)
    /// </summary>
    protected Guid? ExecutionTaskId { get; set; }

    /// <summary>
    /// 错误信息
    /// </summary>
    protected string? ErrorMessage { get; set; }

    /// <summary>
    /// 结果对话框是否可见
    /// </summary>
    protected bool ResultModalVisible { get; set; }

    /// <summary>
    /// 结果对话框标题
    /// </summary>
    protected string ResultModalTitle => ExecutionTaskId.HasValue ? "执行成功" : "执行失败";

    #endregion

    #region 生命周期

    /// <summary>
    /// 组件初始化时加载数据
    /// </summary>
    protected override async Task OnInitializedAsync()
    {
        // 初始化表单默认值
        FormModel.RequestedBy = "当前用户"; // TODO: 从用户上下文获取

        // 并行加载配置和数据源
        await Task.WhenAll(
            LoadPartitionConfigsAsync(),
            LoadDataSourcesAsync()
        );

        // 如果有路由参数,预选配置
        if (PartitionConfigId.HasValue)
        {
            FormModel.PartitionConfigurationId = PartitionConfigId.Value;
            await OnPartitionConfigChanged(PartitionConfigs.FirstOrDefault(c => c.Value == PartitionConfigId.Value));
        }
    }

    #endregion

    #region 数据加载

    /// <summary>
    /// 加载分区配置列表
    /// </summary>
    private async Task LoadPartitionConfigsAsync()
    {
        try
        {
            LoadingConfigs = true;
            StateHasChanged();

            // TODO: 调用实际的分区配置 API
            // 暂时使用模拟数据
            PartitionConfigs = new List<PartitionConfigOption>
            {
                new PartitionConfigOption { Value = Guid.NewGuid(), Label = "订单表按月分区配置" },
                new PartitionConfigOption { Value = Guid.NewGuid(), Label = "日志表按日分区配置" },
                new PartitionConfigOption { Value = Guid.NewGuid(), Label = "交易表按年分区配置" }
            };
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "加载分区配置失败");
            Message.Error("加载分区配置失败,请稍后重试");
        }
        finally
        {
            LoadingConfigs = false;
            StateHasChanged();
        }
    }

    /// <summary>
    /// 加载数据源列表
    /// </summary>
    private async Task LoadDataSourcesAsync()
    {
        try
        {
            LoadingDataSources = true;
            StateHasChanged();

            // TODO: 调用实际的数据源 API
            // 暂时使用模拟数据
            DataSources = new List<DataSourceOption>
            {
                new DataSourceOption { Value = Guid.NewGuid(), Label = "生产数据库 - SQL Server" },
                new DataSourceOption { Value = Guid.NewGuid(), Label = "测试数据库 - SQL Server" },
                new DataSourceOption { Value = Guid.NewGuid(), Label = "开发数据库 - SQL Server" }
            };
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "加载数据源失败");
            Message.Error("加载数据源失败,请稍后重试");
        }
        finally
        {
            LoadingDataSources = false;
            StateHasChanged();
        }
    }

    #endregion

    #region 事件处理

    /// <summary>
    /// 分区配置选择变化事件
    /// </summary>
    protected async Task OnPartitionConfigChanged(PartitionConfigOption? option)
    {
        if (option == null || !option.Value.HasValue)
        {
            SelectedConfigDetail = null;
            return;
        }

        try
        {
            // TODO: 调用实际的配置详情 API
            // 暂时使用模拟数据
            SelectedConfigDetail = new PartitionConfigDetail
            {
                Id = option.Value.Value,
                TargetTable = "dbo.Orders",
                PartitionKey = "OrderDate",
                PartitionGranularity = "Month",
                RetentionMonths = 36,
                CreatedAtUtc = DateTime.UtcNow.AddDays(-30)
            };

            StateHasChanged();
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "加载配置详情失败: {ConfigId}", option.Value);
            Message.Error("加载配置详情失败");
        }
    }

    /// <summary>
    /// 表单提交事件
    /// </summary>
    protected async Task OnSubmitAsync()
    {
        // 验证表单
        if (!ValidateForm())
        {
            return;
        }

        try
        {
            Submitting = true;
            StateHasChanged();

            // 构建请求模型
            var request = new StartPartitionExecutionRequestModel(
                PartitionConfigurationId: FormModel.PartitionConfigurationId!.Value,
                DataSourceId: FormModel.DataSourceId!.Value,
                RequestedBy: FormModel.RequestedBy!,
                BackupConfirmed: FormModel.BackupConfirmed,
                BackupReference: FormModel.BackupReference,
                Notes: FormModel.Notes,
                ForceWhenWarnings: FormModel.ForceWhenWarnings,
                Priority: FormModel.Priority
            );

            // 调用 API 发起执行
            var taskId = await ExecutionApi.StartAsync(request);

            if (taskId.HasValue)
            {
                ExecutionTaskId = taskId;
                ErrorMessage = null;
                Message.Success("执行任务已创建");
            }
            else
            {
                ExecutionTaskId = null;
                ErrorMessage = "API 返回结果为空,请检查后端日志";
                Message.Error(ErrorMessage);
            }

            // 显示结果对话框
            ResultModalVisible = true;
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "创建执行任务失败");
            ExecutionTaskId = null;
            ErrorMessage = ex.Message;
            ResultModalVisible = true;
            Message.Error($"创建任务失败: {ex.Message}");
        }
        finally
        {
            Submitting = false;
            StateHasChanged();
        }
    }

    /// <summary>
    /// 重置表单
    /// </summary>
    protected void ResetForm()
    {
        FormModel = new StartExecutionFormModel
        {
            RequestedBy = "当前用户" // TODO: 从用户上下文获取
        };
        SelectedConfigDetail = null;
        ExecutionTaskId = null;
        ErrorMessage = null;
        StateHasChanged();
    }

    #endregion

    #region 导航

    /// <summary>
    /// 导航到监控页面
    /// </summary>
    protected void NavigateToMonitor()
    {
        Navigation.NavigateTo("/partition-executions/monitor");
    }

    /// <summary>
    /// 导航到监控页面并显示当前任务
    /// </summary>
    protected void NavigateToMonitorWithTask()
    {
        if (ExecutionTaskId.HasValue)
        {
            Navigation.NavigateTo("/partition-executions/monitor");
            // TODO: 可以通过 query string 传递 taskId,让监控页自动打开详情
        }
    }

    /// <summary>
    /// 创建新任务(重置表单并关闭对话框)
    /// </summary>
    protected void CreateNewTask()
    {
        ResultModalVisible = false;
        ResetForm();
    }

    #endregion

    #region 验证

    /// <summary>
    /// 验证表单是否有效
    /// </summary>
    private bool ValidateForm()
    {
        if (!FormModel.PartitionConfigurationId.HasValue)
        {
            Message.Warning("请选择分区配置");
            return false;
        }

        if (!FormModel.DataSourceId.HasValue)
        {
            Message.Warning("请选择数据源");
            return false;
        }

        if (!FormModel.BackupConfirmed)
        {
            Message.Warning("请确认数据库已备份");
            return false;
        }

        if (string.IsNullOrWhiteSpace(FormModel.RequestedBy))
        {
            Message.Warning("请输入执行人");
            return false;
        }

        return true;
    }

    /// <summary>
    /// 判断表单是否有效(用于禁用提交按钮)
    /// </summary>
    protected bool IsFormValid()
    {
        return FormModel.PartitionConfigurationId.HasValue
            && FormModel.DataSourceId.HasValue
            && FormModel.BackupConfirmed
            && !string.IsNullOrWhiteSpace(FormModel.RequestedBy);
    }

    #endregion
}

#region 表单模型和选项类

/// <summary>
/// 执行发起表单模型
/// </summary>
public class StartExecutionFormModel
{
    /// <summary>分区配置ID</summary>
    public Guid? PartitionConfigurationId { get; set; }

    /// <summary>数据源ID</summary>
    public Guid? DataSourceId { get; set; }

    /// <summary>执行人</summary>
    public string? RequestedBy { get; set; }

    /// <summary>备份确认</summary>
    public bool BackupConfirmed { get; set; }

    /// <summary>备份参考</summary>
    public string? BackupReference { get; set; }

    /// <summary>执行备注</summary>
    public string? Notes { get; set; }

    /// <summary>强制执行(忽略警告)</summary>
    public bool ForceWhenWarnings { get; set; }

    /// <summary>优先级(0-10)</summary>
    public int Priority { get; set; } = 0;

    /// <summary>优先级的 double 类型绑定(用于 AntDesign Slider)</summary>
    public double PriorityDouble
    {
        get => Priority;
        set => Priority = (int)Math.Round(value);
    }
}

/// <summary>
/// 分区配置下拉选项
/// </summary>
public class PartitionConfigOption
{
    public Guid? Value { get; set; }
    public string Label { get; set; } = string.Empty;
}

/// <summary>
/// 数据源下拉选项
/// </summary>
public class DataSourceOption
{
    public Guid? Value { get; set; }
    public string Label { get; set; } = string.Empty;
}

/// <summary>
/// 分区配置详情(预览)
/// </summary>
public class PartitionConfigDetail
{
    public Guid Id { get; set; }
    public string TargetTable { get; set; } = string.Empty;
    public string PartitionKey { get; set; } = string.Empty;
    public string PartitionGranularity { get; set; } = string.Empty;
    public int RetentionMonths { get; set; }
    public DateTime CreatedAtUtc { get; set; }
}

#endregion
