FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8
WORKDIR /app
RUN curl https://storage.googleapis.com/ggcdimdb/micro/title.ratings.tsv.bz2 --output title.ratings.tsv.bz2
COPY target/streamgen-1.0-SNAPSHOT.jar .
ENTRYPOINT ["/entrypoint.sh", "java", "-jar", "streamgen-1.0-SNAPSHOT.jar"]
