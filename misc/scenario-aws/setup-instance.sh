#!/usr/bin/env bash
# Runs on a fresh Ubuntu EC2 instance: Docker, .NET, the repo, one build. Args: <max-minutes>
set -euo pipefail
MAX_MINUTES="${1:-300}"

# Safety net: the instance is launched with shutdown behavior "terminate", so this ends it (and its billing)
# even if the orchestrator on the PC dies.
sudo shutdown -h "+${MAX_MINUTES}" >/dev/null 2>&1 || true

# A fresh Ubuntu AMI resolves archive.ubuntu.com over IPv6 it can't route; apt stalls for minutes without this.
echo 'Acquire::ForceIPv4 "true";' | sudo tee /etc/apt/apt.conf.d/99force-ipv4 >/dev/null

sudo DEBIAN_FRONTEND=noninteractive apt-get update -q >/dev/null
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q docker.io >/dev/null
sudo usermod -aG docker ubuntu
sudo systemctl enable --now docker >/dev/null 2>&1

# global.json pins SDK 10; the scenario tests target net8.0 and use ASP.NET Core (YARP proxy), so the
# ASP.NET Core 8.0 runtime too (it includes the base runtime).
curl -sSL https://dot.net/v1/dotnet-install.sh -o /tmp/dotnet-install.sh
bash /tmp/dotnet-install.sh --channel 10.0 --install-dir "$HOME/dotnet" >/dev/null
bash /tmp/dotnet-install.sh --channel 8.0 --runtime aspnetcore --install-dir "$HOME/dotnet" >/dev/null
export DOTNET_ROOT="$HOME/dotnet" PATH="$HOME/dotnet:$PATH" DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1

mkdir -p "$HOME/jobmaster-net" "$HOME/results"
tar -xzf "$HOME/repo.tgz" -C "$HOME/jobmaster-net"
cd "$HOME/jobmaster-net"
# The flavor matrix (also builds JobMaster.ScenarioTests, its runner and fixtures).
dotnet build tests/JobMaster.ScenarioMatrixTests -v q -nologo
