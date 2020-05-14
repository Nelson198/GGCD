# Gestão de Grandes Conjuntos de Dados - 2019/2020

## TP2

### *Batch*

* *Hadoop* :

    ```bash
    $ git clone https://github.com/big-data-europe/docker-hadoop.git
    $ cd docker-hadoop
    $ docker-compose up
    ```

* *Spark* :

    ```bash
    $ git clone https://github.com/big-data-europe/docker-spark.git
    $ cd docker-spark
    $ docker-compose up
    ```

    * Acrescentar ao ficheiro *docker-compose.yml* :

        ```bash
        networks:
          default:
            external:
              name: docker-hadoop_default
        ```

* *Dockerfile* :

    ```dockerfile
    FROM bde2020/spark-base
    COPY target/jarname.jar /
    ENTRYPOINT ["/spark/bin/spark-submit", "--class", "mainclass", "--master", "spark://spark-master:7077", "/jarname.jar"]
    ```

    * Opções de execução :

        ```bash
        -p 4040:4040 --network docker-hadoop_default --env-file ../docker-hadoop/hadoop.env
        ```

* *Clean up with* :

    ```bash
    $ docker-compose down
    $ docker volume prune
    ```

    &rarr; Transferência do ficheiro *title.principals.tsv* para o *HDFS* :

    ```bash
    $ docker run --network docker-hadoop_default --env-file docker-hadoop/hadoop.env -it bde2020/hadoop-base bash
    
    $ hdfs dfs -mkdir /data
    
    $ curl https://datasets.imdbws.com/title.principals.tsv.gz | gunzip | hdfs dfs -put - hdfs://namenode:9000/data/title.principals.tsv
    ```

### *Streamgen*

***Streamgen*** : gerador de dados para teste de processamento de *streams*.

* Como compilar :

    ```bash
    $ mvn package
    $ docker build -t streamgen
    ```

* Modo de usar como servidor local :

    ```bash
    $ java -jar target/streamgen-1.0-SNAPSHOT.jar ./title.ratings.tsv.gz 120
    ```

* Modo de usar como *container* *docker* ligado a uma rede *Hadoop* :

    ```bash
    $ docker run --env-file hadoop.env --network docker-hadoop_default -p 12345:12345 run streamgen hdfs:///input/title.ratings.tsv 120
    ```

* Parâmetros :
    * **1º parâmetro**: localização do ficheiro do *IMDb* (local, *http* ou *hdfs*);
    * **2º parâmetro:** número de eventos gerados por minuto.

Para testar, ligar a `localhost:12345`, por exemplo com :

```bash
$ nc localhost 12345
```
