namespace DbArchiveTool.Shared.Results;

public sealed record PagedResult<T>(IReadOnlyList<T> Items, int TotalCount, int Page, int PageSize)
{
    public static PagedResult<T> Empty(int page = 1, int pageSize = 20) => new(Array.Empty<T>(), 0, page, pageSize);
}
