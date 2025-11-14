# DbArchiveTool Web Dockerfile
# 多阶段构建，优化镜像大小

# ============ 构建阶段 ============
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# 复制项目文件并还原依赖
COPY ["src/DbArchiveTool.Web/DbArchiveTool.Web.csproj", "src/DbArchiveTool.Web/"]
COPY ["src/DbArchiveTool.Shared/DbArchiveTool.Shared.csproj", "src/DbArchiveTool.Shared/"]

RUN dotnet restore "src/DbArchiveTool.Web/DbArchiveTool.Web.csproj"

# 复制所有源代码
COPY . .

# 构建和发布应用
WORKDIR "/src/src/DbArchiveTool.Web"
RUN dotnet build "DbArchiveTool.Web.csproj" -c Release -o /app/build
RUN dotnet publish "DbArchiveTool.Web.csproj" -c Release -o /app/publish /p:UseAppHost=false

# ============ 运行时阶段 ============
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app

# 创建应用目录
RUN mkdir -p /app/logs

# 从构建阶段复制发布文件
COPY --from=build /app/publish .

# 配置环境变量
ENV ASPNETCORE_ENVIRONMENT=Production \
    ASPNETCORE_URLS=http://+:5000 \
    TZ=Asia/Shanghai

# 暴露端口
EXPOSE 5000

# 健康检查
HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:5000/health || exit 1

# 启动应用
ENTRYPOINT ["dotnet", "DbArchiveTool.Web.dll"]
