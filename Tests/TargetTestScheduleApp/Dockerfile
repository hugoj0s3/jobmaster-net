FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["Tests/TargetTestScheduleApp/TargetTestScheduleApp.csproj", "Tests/TargetTestScheduleApp/"]
COPY ["JobMaster/JobMaster.csproj", "JobMaster/"]
COPY ["Sql/JobMaster.SqlBase/JobMaster.SqlBase.csproj", "Sql/JobMaster.SqlBase/"]
COPY ["Sql/JobMaster.Postgres/JobMaster.Postgres.csproj", "Sql/JobMaster.Postgres/"]
COPY ["Sql/JobMaster.MySql/JobMaster.MySql.csproj", "Sql/JobMaster.MySql/"]
COPY ["Sql/JobMaster.SqlServer/JobMaster.SqlServer.csproj", "Sql/JobMaster.SqlServer/"]
COPY ["JobMaster.NatsJetStream/JobMaster.NatsJetStream.csproj", "JobMaster.NatsJetStream/"]
COPY ["Directory.Build.props", "Directory.Packages.props", "./"]
RUN dotnet restore "Tests/TargetTestScheduleApp/TargetTestScheduleApp.csproj"
COPY . .
WORKDIR "/src/Tests/TargetTestScheduleApp"
RUN dotnet build "TargetTestScheduleApp.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "TargetTestScheduleApp.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENV ASPNETCORE_URLS=http://+:8080
ENTRYPOINT ["dotnet", "TargetTestScheduleApp.dll"]
