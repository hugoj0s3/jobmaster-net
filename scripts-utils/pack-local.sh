#!/bin/bash
set -e

cd "$(dirname "$0")/.."

OUTPUT_DIR="./nupkgs"
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

dotnet pack JobMaster/JobMaster.csproj                                   -c Release -p:UseProjectRefs=false -o "$OUTPUT_DIR"
dotnet pack JobMaster.Api/JobMaster.Api.csproj                           -c Release -p:UseProjectRefs=false -o "$OUTPUT_DIR"
dotnet pack JobMaster.Dashboard/JobMaster.Dashboard.csproj               -c Release -p:UseProjectRefs=false -o "$OUTPUT_DIR"
dotnet pack JobMaster.NatsJetStream/JobMaster.NatsJetStream.csproj       -c Release -p:UseProjectRefs=false -o "$OUTPUT_DIR"
dotnet pack Sql/JobMaster.SqlBase/JobMaster.SqlBase.csproj               -c Release -p:UseProjectRefs=false -o "$OUTPUT_DIR"
dotnet pack Sql/JobMaster.Postgres/JobMaster.Postgres.csproj             -c Release -p:UseProjectRefs=false -o "$OUTPUT_DIR"
dotnet pack Sql/JobMaster.MySql/JobMaster.MySql.csproj                   -c Release -p:UseProjectRefs=false -o "$OUTPUT_DIR"
dotnet pack Sql/JobMaster.SqlServer/JobMaster.SqlServer.csproj           -c Release -p:UseProjectRefs=false -o "$OUTPUT_DIR"

echo ""
echo "Packages generated in $OUTPUT_DIR:"
ls -1 "$OUTPUT_DIR"/*.nupkg
