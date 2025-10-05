using System.ComponentModel.DataAnnotations;
using DbArchiveTool.Application.Partitions;
using DbArchiveTool.Shared.Results;
using DbArchiveTool.Web.Services;
using Microsoft.AspNetCore.Components;

namespace DbArchiveTool.Web.Pages;

public partial class Partitions
{
    private bool showSplitDrawer;
    private bool submittingSplit;
    private SplitPartitionModel splitModel = new();
    private PartitionCommandPreviewDto? preview;
    private string? alertMessage;
    private string alertLevel = "info";

    [Parameter]
    public Guid DataSourceId { get; set; }

    [SupplyParameterFromQuery(Name = "schema")]
    public string? Schema { get; set; }

    [SupplyParameterFromQuery(Name = "table")]
    public string? Table { get; set; }

    protected override void OnParametersSet()
    {
        if (!string.IsNullOrWhiteSpace(Schema))
        {
            splitModel.SchemaName = Schema!;
        }

        if (!string.IsNullOrWhiteSpace(Table))
        {
            splitModel.TableName = Table!;
        }
    }

    private void OpenSplitDrawer()
    {
        preview = null;
        alertMessage = null;
        if (string.IsNullOrWhiteSpace(splitModel.RequestedBy))
        {
            splitModel.RequestedBy = "tester";
        }

        showSplitDrawer = true;
    }

    private void CloseSplitDrawer()
    {
        showSplitDrawer = false;
        submittingSplit = false;
    }

    private async Task PreviewSplitAsync()
    {
        if (string.IsNullOrWhiteSpace(splitModel.TableName) || string.IsNullOrWhiteSpace(splitModel.SchemaName))
        {
            ShowAlert("请填写架构与表名", "danger");
            return;
        }

        submittingSplit = true;
        try
        {
            var request = splitModel.ToRequest(DataSourceId);
            var result = await PartitionApi.PreviewSplitAsync(DataSourceId, request);
            if (!result.IsSuccess)
            {
                ShowAlert(result.Error ?? "预览拆分命令失败", "warning");
                return;
            }

            preview = result.Value;
            ShowAlert("脚本已生成", "success");
        }
        finally
        {
            submittingSplit = false;
        }
    }

    private async Task SubmitSplitAsync()
    {
        if (preview is null)
        {
            ShowAlert("请先预览脚本", "warning");
            return;
        }

        submittingSplit = true;
        try
        {
            var request = splitModel.ToRequest(DataSourceId);
            var result = await PartitionApi.ExecuteSplitAsync(DataSourceId, request);
            if (!result.IsSuccess)
            {
                ShowAlert(result.Error ?? "创建拆分命令失败", "danger");
                return;
            }

            CloseSplitDrawer();
            ShowAlert("拆分命令已提交待审批", "success");
            preview = null;
        }
        finally
        {
            submittingSplit = false;
        }
    }

    private void ShowAlert(string message, string level)
    {
        alertMessage = message;
        alertLevel = level;
    }

    private sealed class SplitPartitionModel
    {
        [Required(ErrorMessage = "请输入架构名")]
        public string SchemaName { get; set; } = string.Empty;
        [Required(ErrorMessage = "请输入表名")]
        public string TableName { get; set; } = string.Empty;
        [Required(ErrorMessage = "请输入申请人")]
        public string RequestedBy { get; set; } = string.Empty;
        public bool BackupConfirmed { get; set; }
        [Required(ErrorMessage = "请填写拆分边界值")]
        public string BoundaryValue { get; set; } = string.Empty;

        public SplitPartitionRequest ToRequest(Guid dataSourceId)
            => new(dataSourceId, SchemaName, TableName, new[] { BoundaryValue }, BackupConfirmed, RequestedBy);
    }
}
