FROM anapsix/alpine-java
MAINTAINER pod-tp2
COPY server/target/tpe2-g9-server-1.0-SNAPSHOT.jar /home/tpe2-g9-server-1.0-SNAPSHOT.jar
CMD ["java","-jar","/home/tpe2-g9-server-1.0-SNAPSHOT.jar"]
