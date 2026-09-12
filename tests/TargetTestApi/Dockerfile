FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["tests/TargetTestApi/TargetTestApi.csproj", "tests/TargetTestApi/"]
COPY ["src/JobMaster.Api/JobMaster.Api.csproj", "src/JobMaster.Api/"]
COPY ["src/JobMaster/JobMaster.csproj", "src/JobMaster/"]
COPY ["src/Providers/JobMaster.SqlBase/JobMaster.SqlBase.csproj", "src/Providers/JobMaster.SqlBase/"]
COPY ["src/Providers/JobMaster.Postgres/JobMaster.Postgres.csproj", "src/Providers/JobMaster.Postgres/"]
COPY ["src/Providers/JobMaster.MySql/JobMaster.MySql.csproj", "src/Providers/JobMaster.MySql/"]
COPY ["src/Providers/JobMaster.SqlServer/JobMaster.SqlServer.csproj", "src/Providers/JobMaster.SqlServer/"]
COPY ["src/Providers/JobMaster.RavenDb/JobMaster.RavenDb.csproj", "src/Providers/JobMaster.RavenDb/"]
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
