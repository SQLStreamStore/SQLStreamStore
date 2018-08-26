# Install .NET Core

$dotNetVersionString = dotnet --version
$dotNetVersion = [Version]$dotnetVersionString
$minDotNetVersionString = "2.1.400"
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

# Run Build

dotnet run -p build\build.csproj -c Release -- $args
