FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["tests/TargetTestApi/TargetTestApi.csproj", "tests/TargetTestApi/"]
COPY ["src/JobMaster.Api/JobMaster.Api.csproj", "src/JobMaster.Api/"]
COPY ["src/JobMaster/JobMaster.csproj", "src/JobMaster/"]
COPY ["src/StorageProviders/JobMaster.SqlBase/JobMaster.SqlBase.csproj", "src/StorageProviders/JobMaster.SqlBase/"]
COPY ["src/StorageProviders/JobMaster.Postgres/JobMaster.Postgres.csproj", "src/StorageProviders/JobMaster.Postgres/"]
COPY ["src/StorageProviders/JobMaster.MySql/JobMaster.MySql.csproj", "src/StorageProviders/JobMaster.MySql/"]
COPY ["src/StorageProviders/JobMaster.SqlServer/JobMaster.SqlServer.csproj", "src/StorageProviders/JobMaster.SqlServer/"]
COPY ["src/StorageProviders/JobMaster.RavenDB/JobMaster.RavenDB.csproj", "src/StorageProviders/JobMaster.RavenDB/"]
COPY ["Directory.Build.props", "Directory.Packages.props", "./"]
RUN dotnet restore "tests/TargetTestApi/TargetTestApi.csproj"
COPY . .
WORKDIR "/src/tests/TargetTestApi"
RUN dotnet build "TargetTestApi.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "TargetTestApi.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENV ASPNETCORE_URLS=http://+:8080
ENTRYPOINT ["dotnet", "TargetTestApi.dll"]
