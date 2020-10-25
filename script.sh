mvn clean install
export CLASSPATH=$PWD/server.jar
java -Dhazelcast.config=$PWD/hazelcast.xml ./server/src/main/java/itba/pod/server/Server
