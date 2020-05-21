FROM bde2020/spark-base:2.4.4-hadoop2.7
COPY target/TP2-1.0-SNAPSHOT.jar /
ENTRYPOINT ["/spark/bin/spark-submit", "--class", "stream.Log", "--master", "spark://spark-master:7077", "/TP2-1.0-SNAPSHOT.jar"]