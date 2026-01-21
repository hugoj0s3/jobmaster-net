FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
USER $APP_UID
WORKDIR /app
EXPOSE 8080
EXPOSE 8081

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["JobMaster.SampleWeb/JobMaster.SampleWeb.csproj", "JobMaster.SampleWeb/"]
COPY ["JobMaster.Core/JobMaster.Core.csproj", "JobMaster.Core/"]
COPY ["JobMaster/JobMaster.csproj", "JobMaster/"]
COPY ["JobMaster.Server/JobMaster.Server.csproj", "JobMaster.Server/"]
COPY ["Sql/JobMaster.SqlServer.Repository/JobMaster.SqlServer.Repository.csproj", "Sql/JobMaster.SqlServer.Repository/"]
COPY ["Sql/JobMaster.BaseSql.Repository/JobMaster.BaseSql.Repository.csproj", "Sql/JobMaster.BaseSql.Repository/"]
RUN dotnet restore "JobMaster.SampleWeb/JobMaster.SampleWeb.csproj"
COPY . .
WORKDIR "/src/JobMaster.SampleWeb"
RUN dotnet build "JobMaster.SampleWeb.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "JobMaster.SampleWeb.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "JobMaster.SampleWeb.dll"]
