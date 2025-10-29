// 测试解析 KeyColumns 的逻辑

var testCases = new Dictionary<string, string[]>
{
    // 测试案例: KeyColumns → 期望解析结果
    { "[OrderDate] DESC, [CustomerId] ASC, [OrderId] ASC", new[] { "OrderDate", "CustomerId", "OrderId" } },
    { "[CustomerId] ASC", new[] { "CustomerId" } },
    { "[OrderDate] DESC", new[] { "OrderDate" } },
    { "[ProductId] ASC, [Quantity] DESC", new[] { "ProductId", "Quantity" } },
};

Console.WriteLine("=== 测试 KeyColumns 解析逻辑 ===\n");

foreach (var (input, expected) in testCases)
{
    var parsed = input.Split(',')
        .Select(c => c.Trim().TrimStart('[').TrimEnd(']').Split(' ')[0])
        .ToArray();
    
    var match = expected.SequenceEqual(parsed);
    
    Console.WriteLine($"输入: {input}");
    Console.WriteLine($"期望: [{string.Join(", ", expected)}]");
    Console.WriteLine($"实际: [{string.Join(", ", parsed)}]");
    Console.WriteLine($"结果: {(match ? "✅ 通过" : "❌ 失败")}\n");
}

// 测试特殊情况:多个空格、无空格等
Console.WriteLine("=== 边界情况测试 ===\n");

var edgeCases = new[]
{
    "[OrderDate]DESC,[CustomerId]ASC",  // 无空格
    "[OrderDate]  DESC  ,  [CustomerId]  ASC",  // 多个空格
    "[Order Date] DESC",  // 列名中有空格(SQL Server不允许,但测试解析)
};

foreach (var input in edgeCases)
{
    var parsed = input.Split(',')
        .Select(c => c.Trim().TrimStart('[').TrimEnd(']').Split(' ')[0])
        .ToArray();
    
    Console.WriteLine($"输入: {input}");
    Console.WriteLine($"解析: [{string.Join(", ", parsed)}]");
    Console.WriteLine();
}
