#!/usr/bin/env bash

echo "Setting environment variables..."
export GENUVEM_RESOURCES_BUCKET=genuvem-resources
export GENOOGLE_HOME="/app/genoogle"
export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
export PATH=$PATH:$JAVA_HOME/bin

echo 'GENOOGLE_HOME="/app/genoogle"' >>/etc/environment

echo "Installing custom packages..."
sudo apt-get -y update
sudo apt install -y git ant openjdk-8-jdk-headless openjdk-8-jre-headless

java -version
echo "Successfully installed custom packages."

echo "Installing Genoogle at $GENOOGLE_HOME..."
mkdir -p $GENOOGLE_HOME
cd $GENOOGLE_HOME || exit
git clone https://github.com/rsantanna/Genoogle.git .
ant jar
mv ./ant-build/genoogle.jar .

echo "Downloading Genoogle configs and files..."
rm -r ./conf

gsutil -m cp -r gs://$GENUVEM_RESOURCES_BUCKET/conf .
gsutil -m cp -r gs://$GENUVEM_RESOURCES_BUCKET/queries .
gsutil -m cp -r gs://$GENUVEM_RESOURCES_BUCKET/files .

echo "Downloading Genoogle scripts..."
gsutil cp gs://$GENUVEM_RESOURCES_BUCKET/scripts/run_genoogle.sh .
sed -i -e 's/\r$//' run_genoogle.sh # fix for line-ending characters
chmod +x run_genoogle.sh

echo "Setting permissions..."
groupadd genoogle
for ID in $(cat /etc/passwd | cut -d ':' -f1); do
  (adduser $ID genoogle)
done

chgrp -R genoogle $GENOOGLE_HOME
chmod -R 777 $GENOOGLE_HOME

echo "Successfully installed Genoogle."
