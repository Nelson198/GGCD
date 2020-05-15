Gerador de dados para teste de processamento de streams.

Como compilar:

    $ mvn package
    $ docker build -t streamgen .

Modo de usar como servidor local:

    $ java -jar target/streamgen-1.0-SNAPSHOT.jar ./title.ratings.tsv.gz 120
    
Mode de usar como container docker ligado a uma rede Hadoop:

    $ docker run --env-file hadoop.env --network docker-hadoop_default \
        -p 12345:12345 streamgen hdfs:///input/title.ratings.tsv 120

O primeiro parametro é a localização do ficheiro do IMDb (local, http ou hdfs)
e o segundo é o numero de eventos gerados por minuto.

Para testar, ligar a localhost:12345, por exemplo com:

    $ nc localhost 12345