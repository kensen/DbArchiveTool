using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using AntDesign;
using DbArchiveTool.Web.Services;
using Microsoft.AspNetCore.Components;
using System.Linq;
using OneOf;

namespace DbArchiveTool.Web.Pages.PartitionExecutions;

/// <summary>
/// 执行向导页面 - Code Behind
/// </summary>
public partial class ExecutionWizard
{
    [Parameter] public Guid ConfigId { get; set; }

    [Inject] private PartitionExecutionApiClient ApiClient { get; set; } = default!;
    [Inject] private NavigationManager Navigation { get; set; } = default!;
    [Inject] private IMessageService Message { get; set; } = default!;
    [Inject] private ReuseTabsService ReuseTabsService { get; set; } = default!;

    private bool Loading { get; set; } = true;
    private bool Submitting { get; set; }
    private ExecutionWizardContextModel? Context { get; set; }
    private int CurrentStep { get; set; }

    private static readonly OneOf<int, Dictionary<string, int>> SingleColumnLayout = 1;
    
    // 表单数据
    private string RequestedBy { get; set; } = string.Empty;
    private string? BackupReference { get; set; }
    private string? Notes { get; set; }
    private bool BackupConfirmed { get; set; }

    // 结果对话框
    private bool ResultVisible { get; set; }
    private bool ExecutionSuccess { get; set; }
    private string ResultTitle { get; set; } = string.Empty;
    private string ErrorMessage { get; set; } = string.Empty;
    private Guid? ExecutionTaskId { get; set; }

    protected override async Task OnInitializedAsync()
    {
        await LoadContextAsync();
    }

    private async Task LoadContextAsync()
    {
        Loading = true;
        try
        {
            Context = await ApiClient.GetWizardContextAsync(ConfigId);
            
            if (Context != null && Context.IsCommitted)
            {
                // Ignore returned task
                Message.Warning("该配置已执行,无法重复提交");
            }
        }
        catch (Exception ex)
        {
            // Ignore returned task
            Message.Error($"加载配置失败: {ex.Message}");
        }
        finally
        {
            Loading = false;
        }
    }

    private RenderFragment RenderConfigReview() => builder =>
    {
        builder.OpenComponent<Card>(0);
        builder.AddAttribute(1, "Title", "分区配置信息");
        builder.AddAttribute(2, "ChildContent", (RenderFragment)(cardBuilder =>
        {
            cardBuilder.OpenComponent<Descriptions>(0);
            cardBuilder.AddAttribute(1, "Bordered", true);
            cardBuilder.AddAttribute(2, "ChildContent", (RenderFragment)(descBuilder =>
            {
                // 目标表
                descBuilder.OpenComponent<DescriptionsItem>(0);
                descBuilder.AddAttribute(1, "Title", "目标表");
                descBuilder.AddAttribute(2, "ChildContent", (RenderFragment)(b => b.AddContent(0, Context!.FullTableName)));
                descBuilder.CloseComponent();

                // 分区函数
                descBuilder.OpenComponent<DescriptionsItem>(10);
                descBuilder.AddAttribute(11, "Title", "分区函数");
                descBuilder.AddAttribute(12, "ChildContent", (RenderFragment)(b => b.AddContent(0, Context!.PartitionFunctionName)));
                descBuilder.CloseComponent();

                // 分区方案
                descBuilder.OpenComponent<DescriptionsItem>(20);
                descBuilder.AddAttribute(21, "Title", "分区方案");
                descBuilder.AddAttribute(22, "ChildContent", (RenderFragment)(b => b.AddContent(0, Context!.PartitionSchemeName)));
                descBuilder.CloseComponent();

                // 分区列
                descBuilder.OpenComponent<DescriptionsItem>(30);
                descBuilder.AddAttribute(31, "Title", "分区列");
                descBuilder.AddAttribute(32, "ChildContent", (RenderFragment)(b => b.AddContent(0, $"{Context!.PartitionColumnName} ({Context.PartitionColumnType})")));
                descBuilder.CloseComponent();

                // 边界数量
                descBuilder.OpenComponent<DescriptionsItem>(40);
                descBuilder.AddAttribute(41, "Title", "分区边界数");
                descBuilder.AddAttribute(42, "ChildContent", (RenderFragment)(b => b.AddContent(0, Context!.Boundaries.Count)));
                descBuilder.CloseComponent();

                // Range类型
                descBuilder.OpenComponent<DescriptionsItem>(50);
                descBuilder.AddAttribute(51, "Title", "Range类型");
                descBuilder.AddAttribute(52, "ChildContent", (RenderFragment)(b => b.AddContent(0, Context!.IsRangeRight ? "RIGHT" : "LEFT")));
                descBuilder.CloseComponent();

                // 主文件组
                descBuilder.OpenComponent<DescriptionsItem>(60);
                descBuilder.AddAttribute(61, "Title", "主文件组");
                descBuilder.AddAttribute(62, "ChildContent", (RenderFragment)(b => b.AddContent(0, Context!.PrimaryFilegroup)));
                descBuilder.CloseComponent();

                // 备注
                if (!string.IsNullOrWhiteSpace(Context!.Remarks))
                {
                    descBuilder.OpenComponent<DescriptionsItem>(70);
                    descBuilder.AddAttribute(71, "Title", "备注");
                    descBuilder.AddAttribute(72, "ChildContent", (RenderFragment)(b => b.AddContent(0, Context.Remarks)));
                    descBuilder.CloseComponent();
                }
            }));
            cardBuilder.CloseComponent(); // Descriptions

            // 边界值预览
            if (Context!.Boundaries.Count > 0)
            {
                cardBuilder.OpenElement(100, "div");
                cardBuilder.AddAttribute(101, "style", "margin-top: 16px;");
                cardBuilder.OpenElement(102, "h4");
                cardBuilder.AddContent(103, "分区边界值");
                cardBuilder.CloseElement();
                cardBuilder.OpenElement(110, "div");
                cardBuilder.AddAttribute(111, "style", "display: flex; flex-wrap: wrap; gap: 8px; margin-top: 8px;");
                
                foreach (var boundary in Context.Boundaries.Take(10))
                {
                    cardBuilder.OpenComponent<Tag>(120);
                    cardBuilder.AddAttribute(121, "ChildContent", (RenderFragment)(b => b.AddContent(0, boundary.DisplayValue)));
                    cardBuilder.CloseComponent();
                }

                if (Context.Boundaries.Count > 10)
                {
                    cardBuilder.OpenComponent<Tag>(130);
                    cardBuilder.AddAttribute(131, "ChildContent", (RenderFragment)(b => b.AddContent(0, $"...共{Context.Boundaries.Count}个边界")));
                    cardBuilder.CloseComponent();
                }

                cardBuilder.CloseElement();
                cardBuilder.CloseElement();
            }
        }));
        builder.CloseComponent();

        if (Context is not null)
        {
            var inspection = Context.IndexInspection;
            var statusAlertType = inspection.BlockingReason is not null
                ? AlertType.Error
                : (inspection.IndexesNeedingAlignment.Count > 0 ? AlertType.Warning : AlertType.Success);
            var alertMessage = inspection.BlockingReason ?? (inspection.IndexesNeedingAlignment.Count > 0
                ? "检测到部分索引尚未包含分区列，执行阶段将自动对齐。"
                : "当前索引结构已包含分区列，无需调整。");
            var alertDescription = inspection.HasExternalForeignKeys && inspection.ExternalForeignKeys.Count > 0
                ? $"外部外键引用：{string.Join("、", inspection.ExternalForeignKeys)}"
                : null;

            builder.OpenComponent<Card>(180);
            builder.AddAttribute(181, "Title", "索引对齐检查");
            builder.AddAttribute(182, "Style", "margin-top: 16px;");
            builder.AddAttribute(183, "ChildContent", (RenderFragment)(inspectBuilder =>
            {
                inspectBuilder.OpenComponent<Alert>(0);
                inspectBuilder.AddAttribute(1, "Type", statusAlertType);
                inspectBuilder.AddAttribute(2, "ShowIcon", true);
                inspectBuilder.AddAttribute(3, "Message", alertMessage);
                if (!string.IsNullOrWhiteSpace(alertDescription))
                {
                    inspectBuilder.AddAttribute(4, "Description", alertDescription);
                }
                inspectBuilder.CloseComponent();

                inspectBuilder.OpenComponent<Descriptions>(10);
                inspectBuilder.AddAttribute(11, "Bordered", true);
                inspectBuilder.AddAttribute(12, "Column", SingleColumnLayout);
                inspectBuilder.AddAttribute(13, "Style", "margin-top: 16px;");
                inspectBuilder.AddAttribute(14, "ChildContent", (RenderFragment)(descBuilder =>
                {
                    descBuilder.OpenComponent<DescriptionsItem>(0);
                    descBuilder.AddAttribute(1, "Title", "聚集索引");
                    descBuilder.AddAttribute(2, "ChildContent", (RenderFragment)(b =>
                    {
                        if (!inspection.HasClusteredIndex)
                        {
                            b.AddContent(0, "未检测到聚集索引");
                        }
                        else
                        {
                            var keyColumns = inspection.ClusteredIndexKeyColumns.Count > 0
                                ? string.Join(", ", inspection.ClusteredIndexKeyColumns)
                                : "未读取到键列";
                            b.AddContent(0, $"{inspection.ClusteredIndexName ?? "(未命名)"} ({keyColumns})");
                            if (!inspection.ClusteredIndexContainsPartitionColumn)
                            {
                                b.OpenElement(10, "div");
                                b.AddAttribute(11, "style", "margin-top:4px;color:#ff4d4f;");
                                b.AddContent(12, "⚠ 聚集索引尚未包含分区列");
                                b.CloseElement();
                            }
                        }
                    }));
                    descBuilder.CloseComponent();

                    descBuilder.OpenComponent<DescriptionsItem>(20);
                    descBuilder.AddAttribute(21, "Title", "外部外键");
                    descBuilder.AddAttribute(22, "ChildContent", (RenderFragment)(b =>
                    {
                        if (inspection.HasExternalForeignKeys && inspection.ExternalForeignKeys.Count > 0)
                        {
                            b.AddContent(0, string.Join("、", inspection.ExternalForeignKeys));
                        }
                        else
                        {
                            b.AddContent(0, "无");
                        }
                    }));
                    descBuilder.CloseComponent();

                    descBuilder.OpenComponent<DescriptionsItem>(30);
                    descBuilder.AddAttribute(31, "Title", "需要对齐的索引");
                    descBuilder.AddAttribute(32, "ChildContent", (RenderFragment)(b =>
                    {
                        if (inspection.IndexesNeedingAlignment.Count == 0)
                        {
                            b.AddContent(0, "无");
                        }
                        else
                        {
                            b.OpenElement(0, "ul");
                            b.AddAttribute(1, "style", "padding-left: 20px; margin: 0;");
                            foreach (var item in inspection.IndexesNeedingAlignment)
                            {
                                b.OpenElement(10, "li");
                                var label = item.IsPrimaryKey ? "主键" :
                                    item.IsUniqueConstraint ? "唯一约束" :
                                    item.IsClustered ? "聚集索引" :
                                    item.IsUnique ? "唯一索引" : "索引";
                                var keyColumns = item.KeyColumns.Count > 0 ? string.Join(", ", item.KeyColumns) : "未读取到键列";
                                b.AddContent(11, $"{item.IndexName} ({label}) - 键列: {keyColumns}");
                                b.CloseElement();
                            }
                            b.CloseElement();
                        }
                    }));
                    descBuilder.CloseComponent();

                    descBuilder.OpenComponent<DescriptionsItem>(40);
                    descBuilder.AddAttribute(41, "Title", "自动对齐策略");
                    descBuilder.AddAttribute(42, "ChildContent", (RenderFragment)(b =>
                    {
                        if (inspection.BlockingReason is not null)
                        {
                            b.AddContent(0, "需要人工处理，已阻止继续执行。");
                        }
                        else if (inspection.IndexesNeedingAlignment.Count > 0)
                        {
                            b.AddContent(0, "执行阶段将自动补齐分区列并重建索引。");
                        }
                        else
                        {
                            b.AddContent(0, "索引已满足要求，无需额外操作。");
                        }
                    }));
                    descBuilder.CloseComponent();
                }));
                inspectBuilder.CloseComponent(); // Descriptions
            }));
            builder.CloseComponent(); // Card
        }

        // 警告提示
        builder.OpenComponent<Alert>(200);
        builder.AddAttribute(201, "Type", AlertType.Warning);
        builder.AddAttribute(202, "Message", "请确认配置信息无误");
        builder.AddAttribute(203, "Description", "执行后将创建分区函数、分区方案并修改表结构,操作不可逆,请确保数据库已备份!");
        builder.AddAttribute(204, "ShowIcon", true);
        builder.AddAttribute(205, "Style", "margin-top: 16px;");
        builder.CloseComponent();
    };

    private RenderFragment RenderExecutionParams() => builder =>
    {
        builder.OpenComponent<Card>(0);
        builder.AddAttribute(1, "Title", "执行参数");
        builder.AddAttribute(2, "ChildContent", (RenderFragment)(cardBuilder =>
        {
            cardBuilder.OpenElement(0, "div");
            cardBuilder.AddAttribute(1, "style", "padding: 16px;");

            // 执行人
            cardBuilder.OpenElement(10, "div");
            cardBuilder.AddAttribute(11, "style", "margin-bottom: 16px;");
            cardBuilder.OpenElement(12, "label");
            cardBuilder.AddAttribute(13, "style", "display: block; margin-bottom: 8px; font-weight: bold;");
            cardBuilder.AddContent(14, "执行人 *");
            cardBuilder.CloseElement();
            cardBuilder.OpenComponent<Input<string>>(15);
            cardBuilder.AddAttribute(16, "Placeholder", "请输入执行人姓名或工号");
            cardBuilder.AddAttribute(17, "MaxLength", 100);
            cardBuilder.AddAttribute(18, "Value", RequestedBy);
            cardBuilder.AddAttribute(19, "ValueChanged", EventCallback.Factory.Create<string>(this, value => RequestedBy = value));
            cardBuilder.CloseComponent();
            cardBuilder.CloseElement();

            // 备份确认
            cardBuilder.OpenElement(20, "div");
            cardBuilder.AddAttribute(21, "style", "margin-bottom: 16px;");
            cardBuilder.OpenElement(22, "label");
            cardBuilder.AddAttribute(23, "style", "display: block; margin-bottom: 8px; font-weight: bold;");
            cardBuilder.AddContent(24, "备份确认 *");
            cardBuilder.CloseElement();
            cardBuilder.OpenComponent<Checkbox>(25);
            cardBuilder.AddAttribute(26, "Checked", BackupConfirmed);
            cardBuilder.AddAttribute(27, "CheckedChanged", EventCallback.Factory.Create<bool>(this, value => BackupConfirmed = value));
            cardBuilder.AddAttribute(28, "ChildContent", (RenderFragment)(b => b.AddContent(0, "我已确认数据库在最近12小时内完成了备份")));
            cardBuilder.CloseComponent();
            cardBuilder.CloseElement();

            // 备份参考
            cardBuilder.OpenElement(30, "div");
            cardBuilder.AddAttribute(31, "style", "margin-bottom: 16px;");
            cardBuilder.OpenElement(32, "label");
            cardBuilder.AddAttribute(33, "style", "display: block; margin-bottom: 8px;");
            cardBuilder.AddContent(34, "备份参考");
            cardBuilder.CloseElement();
            cardBuilder.OpenComponent<Input<string>>(35);
            cardBuilder.AddAttribute(36, "Placeholder", "可选:备份文件名或备份任务ID");
            cardBuilder.AddAttribute(37, "MaxLength", 200);
            cardBuilder.AddAttribute(38, "Value", BackupReference);
            cardBuilder.AddAttribute(39, "ValueChanged", EventCallback.Factory.Create<string>(this, value => BackupReference = value));
            cardBuilder.CloseComponent();
            cardBuilder.CloseElement();

            // 执行备注
            cardBuilder.OpenElement(40, "div");
            cardBuilder.AddAttribute(41, "style", "margin-bottom: 16px;");
            cardBuilder.OpenElement(42, "label");
            cardBuilder.AddAttribute(43, "style", "display: block; margin-bottom: 8px;");
            cardBuilder.AddContent(44, "执行备注");
            cardBuilder.CloseElement();
            cardBuilder.OpenComponent<TextArea>(45);
            cardBuilder.AddAttribute(46, "Placeholder", "可选:说明执行原因或注意事项");
            cardBuilder.AddAttribute(47, "Rows", (uint)3);
            cardBuilder.AddAttribute(48, "MaxLength", 500);
            cardBuilder.AddAttribute(49, "Value", Notes);
            cardBuilder.AddAttribute(50, "ValueChanged", EventCallback.Factory.Create<string>(this, value => Notes = value));
            cardBuilder.CloseComponent();
            cardBuilder.CloseElement();

            cardBuilder.CloseElement(); // div
        }));
        builder.CloseComponent();

        // 提示
        builder.OpenComponent<Alert>(100);
        builder.AddAttribute(101, "Type", AlertType.Info);
        builder.AddAttribute(102, "Message", "温馨提示");
        builder.AddAttribute(103, "Description", "执行人信息将记录在审计日志中,请如实填写。备份确认是必填项,确保数据安全。");
        builder.AddAttribute(104, "ShowIcon", true);
        builder.AddAttribute(105, "Style", "margin-top: 16px;");
        builder.CloseComponent();
    };

    private RenderFragment RenderConfirmation() => builder =>
    {
        builder.OpenComponent<Result>(0);
        builder.AddAttribute(1, "Status", ResultStatus.Info);
        builder.AddAttribute(2, "Title", "确认执行");
        builder.AddAttribute(3, "SubTitle", $"即将对表 {Context!.FullTableName} 执行分区操作");
        builder.AddAttribute(4, "ChildContent", (RenderFragment)(resultBuilder =>
        {
            resultBuilder.OpenElement(0, "div");
            resultBuilder.AddAttribute(1, "style", "text-align: left; max-width: 600px; margin: 0 auto;");
            
            resultBuilder.OpenElement(10, "h4");
            resultBuilder.AddContent(11, "执行信息确认:");
            resultBuilder.CloseElement();

            resultBuilder.OpenElement(20, "ul");
            resultBuilder.AddAttribute(21, "style", "line-height: 2;");
            
            resultBuilder.OpenElement(30, "li");
            resultBuilder.AddContent(31, $"执行人: {RequestedBy}");
            resultBuilder.CloseElement();

            resultBuilder.OpenElement(40, "li");
            resultBuilder.AddContent(41, $"目标表: {Context.FullTableName}");
            resultBuilder.CloseElement();

            resultBuilder.OpenElement(50, "li");
            resultBuilder.AddContent(51, $"分区边界数: {Context.Boundaries.Count}");
            resultBuilder.CloseElement();

            resultBuilder.OpenElement(60, "li");
            resultBuilder.AddContent(61, $"备份确认: {(BackupConfirmed ? "✓ 已确认" : "✗ 未确认")}");
            resultBuilder.CloseElement();

            if (!string.IsNullOrWhiteSpace(BackupReference))
            {
                resultBuilder.OpenElement(70, "li");
                resultBuilder.AddContent(71, $"备份参考: {BackupReference}");
                resultBuilder.CloseElement();
            }

            if (!string.IsNullOrWhiteSpace(Notes))
            {
                resultBuilder.OpenElement(80, "li");
                resultBuilder.AddContent(81, $"备注: {Notes}");
                resultBuilder.CloseElement();
            }

            resultBuilder.CloseElement(); // ul
            resultBuilder.CloseElement(); // div
        }));
        builder.CloseComponent();
    };

    private void PreviousStep()
    {
        if (CurrentStep > 0)
        {
            CurrentStep--;
        }
    }

    private void NextStep()
    {
        if (CanProceedToNextStep())
        {
            CurrentStep++;
        }
    }

    private bool CanProceedToNextStep()
    {
        return CurrentStep switch
        {
            0 => Context != null && !Context.IsCommitted && string.IsNullOrEmpty(Context.IndexInspection.BlockingReason),
            1 => !string.IsNullOrWhiteSpace(RequestedBy) && BackupConfirmed,
            _ => false
        };
    }

    private async Task StartExecution()
    {
        if (Context == null || Submitting)
        {
            return;
        }

        Submitting = true;
        try
        {
            var request = new StartPartitionExecutionRequestModel(
                PartitionConfigurationId: ConfigId,
                DataSourceId: Context.DataSourceId,
                RequestedBy: RequestedBy,
                BackupConfirmed: BackupConfirmed,
                BackupReference: BackupReference,
                Notes: Notes,
                ForceWhenWarnings: false,
                Priority: 0
            );

            var startResponse = await ApiClient.StartAsync(request);

            if (startResponse.Success && startResponse.TaskId.HasValue)
            {
                ExecutionSuccess = true;
                ExecutionTaskId = startResponse.TaskId.Value;
                ResultTitle = "执行成功";
                // Ignore returned task
                Message.Success("分区执行任务已创建");
            }
            else
            {
                ExecutionSuccess = false;
                ErrorMessage = startResponse.Error ?? "创建任务失败,请查看API日志";
                ResultTitle = "执行失败";
                Message.Error($"执行失败: {ErrorMessage}");
            }

            ResultVisible = true;
        }
        catch (Exception ex)
        {
            ExecutionSuccess = false;
            ErrorMessage = ex.Message;
            ResultTitle = "执行失败";
            ResultVisible = true;
            // Ignore returned task
            Message.Error($"执行失败: {ex.Message}");
        }
        finally
        {
            Submitting = false;
        }
    }

    private async void NavigateBack()
    {
        // 关闭Modal
        ResultVisible = false;
        
        // 触发UI更新，确保Modal和遮罩层完全移除
        StateHasChanged();
        
        // 等待Modal关闭动画完成
        await Task.Delay(300);
        
        try
        {
            // 关闭当前向导标签页（这会自动激活前一个标签）
            var currentPath = $"/partition-executions/wizard/{ConfigId}";
            ReuseTabsService.ClosePage(currentPath);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"关闭向导标签页失败: {ex.Message}");
        }
    }

    private void NavigateToMonitor()
    {
        // 先导航到监控页面
        var targetUrl = ExecutionTaskId.HasValue 
            ? $"/partition-executions/monitor?taskId={ExecutionTaskId.Value}" 
            : "/partition-executions/monitor";
        
        Navigation.NavigateTo(targetUrl);
        
        // 关闭Modal
        ResultVisible = false;
        
        // 延迟关闭当前向导标签页，避免用户返回时重复操作
        _ = Task.Run(async () =>
        {
            await Task.Delay(100); // 等待导航完成
            await InvokeAsync(() =>
            {
                try
                {
                    // 关闭当前向导标签页
                    var currentPath = $"/partition-executions/wizard/{ConfigId}";
                    ReuseTabsService.ClosePage(currentPath);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"关闭向导标签页失败: {ex.Message}");
                }
            });
        });
    }

    private void CloseModal()
    {
        ResultVisible = false;
    }

    /// <summary>
    /// Modal可见性变化事件处理
    /// </summary>
    private async void OnModalVisibleChanged(bool visible)
    {
        ResultVisible = visible;
        
        // 当Modal关闭时（visible = false），自动关闭向导标签页
        if (!visible)
        {
            // 触发UI更新
            StateHasChanged();
            
            // 等待Modal关闭动画完成
            await Task.Delay(300);
            
            try
            {
                // 关闭当前向导标签页
                var currentPath = $"/partition-executions/wizard/{ConfigId}";
                ReuseTabsService.ClosePage(currentPath);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"关闭向导标签页失败: {ex.Message}");
            }
        }
    }
}
