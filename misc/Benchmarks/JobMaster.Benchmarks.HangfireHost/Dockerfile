FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["misc/Benchmarks/JobMaster.Benchmarks.HangfireHost/JobMaster.Benchmarks.HangfireHost.csproj", "misc/Benchmarks/JobMaster.Benchmarks.HangfireHost/"]
COPY ["misc/Benchmarks/JobMaster.Benchmarks.Common/JobMaster.Benchmarks.Common.csproj", "misc/Benchmarks/JobMaster.Benchmarks.Common/"]
COPY ["Directory.Build.props", "Directory.Packages.props", "./"]
RUN dotnet restore "misc/Benchmarks/JobMaster.Benchmarks.HangfireHost/JobMaster.Benchmarks.HangfireHost.csproj"
COPY . .
WORKDIR "/src/misc/Benchmarks/JobMaster.Benchmarks.HangfireHost"
RUN dotnet build "JobMaster.Benchmarks.HangfireHost.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "JobMaster.Benchmarks.HangfireHost.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENV ASPNETCORE_URLS=http://+:8080
ENTRYPOINT ["dotnet", "JobMaster.Benchmarks.HangfireHost.dll"]
