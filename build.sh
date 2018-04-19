#!/usr/bin/env bash
##########################################################################
# This is the Cake bootstrapper script for Linux and OS X.
# This file was downloaded from https://github.com/cake-build/resources
# Feel free to change this file to fit your needs.
##########################################################################
 
# Define directories.
SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
TOOLS_DIR=$SCRIPT_DIR/tools
TOOLS_PROJ=$TOOLS_DIR/tools.csproj
CAKE_VERSION=0.26.1
CAKE_DLL=$TOOLS_DIR/Cake.CoreCLR.$CAKE_VERSION/cake.coreclr/$CAKE_VERSION/Cake.dll
DOTNET_INSTALL_PATH=$SCRIPT_DIR/.dotnet 
 
# Make sure the tools folder exist.
if [ ! -d "$TOOLS_DIR" ]; then
  mkdir "$TOOLS_DIR"
fi

###########################################################################
# Install .NET Core CLI
###########################################################################
curl -sSL https://dot.net/dotnet-install.sh | bash /dev/stdin --channel current --version 2.0.0 --install-dir $DOTNET_INSTALL_PATH
 
###########################################################################
# INSTALL CAKE
###########################################################################
if [ ! -f "$CAKE_DLL" ]; then
    echo "<Project Sdk=\"Microsoft.NET.Sdk\"><PropertyGroup><TargetFramework>netcoreapp2.0</TargetFramework></PropertyGroup></Project>" > $TOOLS_PROJ
    dotnet add $TOOLS_PROJ package cake.coreclr -v $CAKE_VERSION --package-directory $TOOLS_DIR/Cake.CoreCLR.$CAKE_VERSION
fi
 
# Make sure that Cake has been installed.
if [ ! -f "$CAKE_DLL" ]; then
    echo "Could not find Cake.exe at '$CAKE_DLL'."
    exit 1
fi
 
###########################################################################
# RUN BUILD SCRIPT
###########################################################################
 
# Start Cake
CAKE_SETTINGS_SKIPVERIFICATION=true exec dotnet "$CAKE_DLL" "$@"
