#!/bin/bash

if [ $# -lt 1 ]; then
    echo "You must specify the deploy directory"
    exit 1
fi

mkdir -p "$1"
mkdir -p "$1"/client/config
cp client/src/main/resources/cities.json "$1"/client/config/
cd "${0%/*}" || exit
mvn clean install -DskipTests=true
export CLASSPATH=$PWD/server.jar
tar -xzf "${0%/*}"/../pod-tp2/server/target/tpe2-g9-server-1.0-SNAPSHOT.tar.gz
tar -xzf "${0%/*}"/../pod-tp2/client/target/tpe2-g9-client-1.0-SNAPSHOT.tar.gz
mv tpe2-g9-server-1.0-SNAPSHOT "$1"/server
mv tpe2-g9-client-1.0-SNAPSHOT/* "$1"/client