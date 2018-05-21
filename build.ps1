<#
.SYNOPSIS
This is a Powershell script to bootstrap a Cake build.
.DESCRIPTION
This Powershell script will download NuGet if missing, restore NuGet tools (including Cake)
and execute your Cake build script with the parameters you provide.

.PARAMETER Script
The build script to execute.
.PARAMETER Target
The build script target to run.
.PARAMETER Configuration
The build configuration to use.
.PARAMETER Verbosity
Specifies the amount of information to be displayed.
.PARAMETER Experimental
Tells Cake to use the latest Roslyn release.
.PARAMETER WhatIf
Performs a dry run of the build script.
No tasks will be executed.
.PARAMETER ScriptArgs
Remaining arguments are added here.
.LINK
http://cakebuild.net
#>

[CmdletBinding()]
Param(
    [string]$Script = "build.cake",
    [string]$Target = "Default",
    [ValidateSet("Release", "Debug")]
    [string]$Configuration = "Release",
    [ValidateSet("Quiet", "Minimal", "Normal", "Verbose", "Diagnostic")]
    [string]$Verbosity = "Verbose",
    [switch]$Experimental,
    [Alias("DryRun","Noop")]
    [switch]$WhatIf,
    [Parameter(Position=0,Mandatory=$false,ValueFromRemainingArguments=$true)]
    [string[]]$ScriptArgs
)

$cakeVersion = "0.27.2"
$buildPath = "$PSScriptRoot/build"
$proj = @"
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>netcoreapp2.0</TargetFramework>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Cake.CoreCLR" Version="$cakeVersion" />
  </ItemGroup>
</Project>
"@

##########################
# Install .NET Core CLI
##########################
$dotNetCoreVersion = "2.0.0"
$dotNetInstallerUri = "https://dot.net/dotnet-install.ps1"
Function Remove-PathVariable([string]$variableToRemove)
{
    $path = [Environment]::GetEnvironmentVariable("PATH", "User")
    if ($path -ne $null)
    {
        $newItems = $path.Split(';', [StringSplitOptions]::RemoveEmptyEntries) | Where-Object { "$($_)" -inotlike $variableToRemove }
        [Environment]::SetEnvironmentVariable("PATH", [System.String]::Join(';', $newItems), "User")
    }

    $path = [Environment]::GetEnvironmentVariable("PATH", "Process")
    if ($path -ne $null)
    {
        $newItems = $path.Split(';', [StringSplitOptions]::RemoveEmptyEntries) | Where-Object { "$($_)" -inotlike $variableToRemove }
        [Environment]::SetEnvironmentVariable("PATH", [System.String]::Join(';', $newItems), "Process")
    }
}

$installPath = Join-Path $PSScriptRoot ".dotnet"
if (!(Test-Path $installPath)) {
    mkdir -Force $installPath | Out-Null;
}
(New-Object System.Net.WebClient).DownloadFile($dotNetInstallerUri, "$installPath\dotnet-install.ps1");

$foundDotNetCliVersion = $null;
if (Get-Command dotnet -ErrorAction SilentlyContinue) {
    $foundDotNetCliVersion = dotnet --version;
}
if($foundDotNetCliVersion -eq $dotNetCoreVersion) {
    Write-Host ".Net Core version $dotNetCoreVersion installed locally."
}
else {
    Write-Host ".Net Core version $dotNetCoreVersion not installated locally. Downloading..."

    & $installPath\dotnet-install.ps1 -Channel Current -Version $dotNetCoreVersion -InstallDir $installPath;

    Remove-PathVariable "$installPath"
    $env:PATH = "$installPath;$env:PATH"
    $env:DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1
    $env:DOTNET_CLI_TELEMETRY_OPTOUT=1
}

##########################
# Install Cake
##########################

Write-Host "Preparing cake..."
# Make sure tools folder exists*
$toolsPath = Join-Path $PSScriptRoot "tools"
if (!(Test-Path $toolsPath)) {
    Write-Host "Creating tools directory..."
    New-Item -Path $toolsPath -Type directory | out-null
}
$proj | Out-File $toolsPath\cake.csproj

Push-Location $toolsPath

dotnet restore --packages ./

Pop-Location

##########################
# Run buildscript
##########################
Write-Host "Running build script..."
$arguments = @{
    target=$Target;
    configuration=$Configuration;
    verbosity=$Verbosity;
    dryrun=$WhatIf;
    NuGet_UseInProcessClient=$true;
}.GetEnumerator() | %{"--{0}=`"{1}`"" -f $_.key, $_.value };

Write-Host $toolsPath/cake.coreclr/$cakeVersion/Cake.dll $Script $arguments $ScriptArgs

$env:CAKE_SETTINGS_SKIPVERIFICATION=$true

&dotnet $toolsPath/cake.coreclr/$cakeVersion/Cake.dll $Script $arguments $ScriptArgs

exit $LASTEXITCODE
