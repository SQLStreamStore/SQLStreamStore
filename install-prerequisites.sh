#!/usr/bin/env bash

#install mono
# sudo apt-get update
# sudo apt-get install curl libunwind8 gettext apt-transport-https -y
# sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 3FA7E0328081BFF6A14DA29AA6A19B38D3D831EF
# sudo echo "deb http://download.mono-project.com/repo/ubuntu trusty main" | tee /etc/apt/sources.list.d/mono-official.list
# sudo apt-get update
# sudo apt-get install mono-complete -y

#install dotnet core 2.0
# sudo curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > microsoft.gpg
# sudo mv microsoft.gpg /etc/apt/trusted.gpg.d/microsoft.gpg
# sudo sh -c 'echo "deb [arch=amd64] https://packages.microsoft.com/repos/microsoft-ubuntu-trusty-prod trusty main" > /etc/apt/sources.list.d/dotnetdev.list' 
# sudo apt-get update
# sudo apt-get install dotnet-sdk-2.0.0 -y