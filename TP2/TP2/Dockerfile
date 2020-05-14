FROM bde2020/spark-base
COPY target/TP2-1.0-SNAPSHOT.jar /
ENTRYPOINT ["/spark/bin/spark-submit", "--class", "batch.Top10", "--master", "spark://spark-master:7077", "/TP2-1.0-SNAPSHOT.jar"]

# batch.Friends
# "--executor-memory", "4g", "--driver-memory", "4g"