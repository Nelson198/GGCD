# Gestão de Grandes Conjuntos de Dados - 2019/2020

## TP2

### *Swarm setup*

&rarr; *Create docker-machine* :

```bash
docker-machine create \
               --driver google --google-project ferrous-aleph-271712 \
               --google-zone europe-west1-b \
               --google-machine-type n1-standard-4 \
               --google-disk-size=100 \
               --google-disk-type=pd-ssd \
               --google-machine-image https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/centos-7-v20200309 \
               master
```

```bash
docker-machine create \
               --driver google --google-project ferrous-aleph-271712 \
               --google-zone europe-west1-b \
               --google-machine-type n1-standard-4 \
               --google-disk-size=100 \
               --google-disk-type=pd-ssd \
               --google-machine-image https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/centos-7-v20200309 \
               worker1
```

```bash
docker-machine create \
               --driver google --google-project ferrous-aleph-271712 \
               --google-zone europe-west1-b \
               --google-machine-type n1-standard-4 \
               --google-disk-size=100 \
               --google-disk-type=pd-ssd \
               --google-machine-image https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/centos-7-v20200309 \
               worker2
```

&rarr; *Setup swarm master with* :

```bash
$ docker-machine ssh master sudo docker swarm init
```

&rarr; *Setup each swarm worker with* :

```bash
$ docker-machine ssh worker1 sudo docker swarm join --token SWMTKN-1-5zfy2iio54tma997pnt96gq5095fimqn2hxr2a8j16ogq0n3c9-0kp6mi5iuj956gpl9sfccd5bo 10.132.0.8:2377
```

```bash
$ docker-machine ssh worker2 sudo docker swarm join --token SWMTKN-1-5zfy2iio54tma997pnt96gq5095fimqn2hxr2a8j16ogq0n3c9-0kp6mi5iuj956gpl9sfccd5bo 10.132.0.8:2377
```

&rarr; *Activate master environment* :

```bash
$ docker-machine env master
$ eval $(docker-machine env master)
```

&rarr; *List swarm nodes with* :

```bash
$ docker node ls
```

#### *Configuration*

&rarr; *Deployment configuration for master* :

```
deploy:
    mode: replicated
    replicas: 1
    placement:
        constraints:
            - "node.role==manager"
```

&rarr; *Deployment configuration for worker1* :

```
deploy:
    mode: replicated
    replicas: 1
    placement:
        constraints:
            - "node.role==worker"
            - "node.hostname==worker1"
```

&rarr; *Deployment configuration for worker2* :

```
deploy:
    mode: replicated
    replicas: 1
    placement:
        constraints:
            - "node.role==worker"
            - "node.hostname==worker2"
```

&rarr; ***Streamgen*** *service* :

```
streamgen:
  image: streamgen
  command: hdfs:///data/title.ratings.tsv 120
  env_file:
    - ./hadoop.env
  deploy:
    mode: replicated
    replicas: 1
    placement:
      constraints:
        - "node.role==manager"
```

#### *Deployment*

&rarr; *Deploy configuration on swarm with* :

```bash
$ docker stack deploy -c ../swarm-spark/docker-compose.yml mystack
```

&rarr; *Check status with* :

```bash
$ docker stack ls
$ docker service ls
$ docker network ls
```

&rarr; *Attach client containers as usual with* :

```bash
$ docker run --network mystack_default --env-file ../swarm-spark/hadoop.env -it bde2020/hadoop-base bash
```

&rarr; *Remove configuration from swarm with* :

```bash
$ docker stack rm mystack
```

#### *Dockerfile*

```dockerfile
FROM bde2020/spark-base:2.4.4-hadoop2.7
COPY target/TP2-1.0-SNAPSHOT.jar /
ENTRYPOINT ["/spark/bin/spark-submit", "--class", "package.classname", "--master", "spark://spark-master:7077", "/TP2-1.0-SNAPSHOT.jar"]
```

* Opções de execução do ficheiro *Dockerfile* :

    ```bash
    -p 4040:4040 --network mystack_default --env-file ../swarm-spark/hadoop.env
    ```

---

### *Streamgen*

***Streamgen*** : gerador de dados para teste de processamento de *streams*.

* Como compilar :

    ```bash
    $ mvn package
    $ docker build -t streamgen .
    ```

* Modo de usar como servidor local :

    ```bash
    $ java -jar target/streamgen-1.0-SNAPSHOT.jar ./title.ratings.tsv.gz 120
    ```

* Modo de usar como *container* *docker* ligado a uma rede *Hadoop* :

    ```bash
    $ docker run --env-file ../swarm-spark/hadoop.env --network mystack_default -p 12345:12345 streamgen hdfs:///data/title.ratings.tsv 120
    ```

* Parâmetros :
    * **1º parâmetro**: localização do ficheiro do *IMDb* (local, *http* ou *hdfs*);
    * **2º parâmetro:** número de eventos gerados por minuto.

Para testar, ligar a `localhost:12345`, por exemplo com :

```bash
$ nc localhost 12345
```

---

### *Hadoop HDFS*

&rarr; Transferência dos ficheiros *IMDb* para a plataforma *Hadoop HDFS* :

* *"title.ratings.tsv.gz"* :

    ```bash
    curl https://datasets.imdbws.com/title.ratings.tsv.gz | gunzip | hdfs dfs -put - hdfs://namenode:9000/data/title.ratings.tsv
    ```

* *"title.principals.tsv.gz"* :

    ```bash
    curl https://datasets.imdbws.com/title.principals.tsv.gz | gunzip | hdfs dfs -put - hdfs://namenode:9000/data/title.principals.tsv
    ```

* *"title.basics.tsv.gz"* :

    ```bash
    curl https://datasets.imdbws.com/title.basics.tsv.gz | gunzip | hdfs dfs -put - hdfs://namenode:9000/data/title.basics.tsv
    ```

* *"name.basics.tsv.gz"* :

    ```bash
    curl https://datasets.imdbws.com/name.basics.tsv.gz | gunzip | hdfs dfs -put - hdfs://namenode:9000/data/name.basics.tsv
    ```

