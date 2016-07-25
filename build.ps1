param(
	[int]$buildNumber = 0,
	[string]$task = "default"
)

if(Test-Path Env:\APPVEYOR_BUILD_NUMBER){
	$buildNumber = [int]$Env:APPVEYOR_BUILD_NUMBER
	Write-Host "Using APPVEYOR_BUILD_NUMBER"
}

if(Test-Path Env:\TEAMCITY_PROJECT_NAME){
    Write-Output "Building in TeamCity"
}

"Build number $buildNumber"

src\.nuget\nuget.exe install src\.nuget\packages.config -o src\packages

Import-Module .\src\packages\psake.4.6.0\tools\psake.psm1

Invoke-Psake .\default.ps1 $task -framework "4.6x64" -properties @{ buildNumber=$buildNumber }

Remove-Module psake
