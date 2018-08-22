<#
.SYNOPSIS
This is a Powershell script to bootstrap a Cake build.
.DESCRIPTION
This Powershell script will download NuGet if missing, restore NuGet tools (including Cake)
and execute your Cake build script with the parameters you provide.
.PARAMETER Target
The build script target to run.
.PARAMETER Configuration
The build configuration to use.
.PARAMETER Verbosity
Specifies the amount of information to be displayed.
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
    [string]$Target = "Default",
    [ValidateSet("Release", "Debug")]
    [string]$Configuration = "Release",
    [ValidateSet("Quiet", "Minimal", "Normal", "Verbose", "Diagnostic")]
    [string]$Verbosity = "Verbose",
    [switch]$WhatIf,
    [Parameter(Position=0,Mandatory=$false,ValueFromRemainingArguments=$true)]
    [string[]]$ScriptArgs
)

######################
# Install .NET Core
######################

$dotNetVersionString = dotnet --version
$dotNetVersion = [Version]$dotnetVersionString
$minDotNetVersionString = "2.1.401"
$minDotNetVersion = [Version]$minDotNetVersionString 

If($dotnetVersion -lt $minDotnetVersion){
    "Installing .Net SDK $minDotNetVersion..."
    Get-Process "dotnet" | Stop-Process
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
    & ./dotnet-install.ps1 -Channel Current -Version $minDotNetVersionString
}
else{
    ".Net SDK $dotNetVersionString installed"
}

##########################
# Install Cake
##########################

Write-Host "Preparing cake..."
# Make sure tools folder exists
$toolsPath = Join-Path $PSScriptRoot "tools"
if (!(Test-Path $toolsPath)) {
    Write-Host "Creating tools directory..."
    New-Item -Path $toolsPath -Type directory | out-null
}
Invoke-Expression "&dotnet restore tools.csproj --packages tools"
$cakeDllPath = (Get-ChildItem tools/cake.coreclr/*/Cake.dll)[0].FullName

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
Invoke-Expression "&dotnet $cakeDllPath build.cake $arguments $ScriptArgs";
exit $LASTEXITCODE
