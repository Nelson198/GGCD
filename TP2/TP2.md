# Gestão de Grandes Conjuntos de Dados - 2019/2020

## TP2

### *Swarm setup*

&rarr; *Create docker-machine* :

```bash
docker-machine create \
               --driver google --google-project ferrous-aleph-271712 \
               --google-zone europe-west1-b \
               --google-machine-type n1-standard-2 \
               --google-disk-size=100 \
               --google-disk-type=pd-ssd \
               --google-machine-image https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/centos-7-v20200309 \
               master
```

```bash
docker-machine create \
               --driver google --google-project ferrous-aleph-271712 \
               --google-zone europe-west1-b \
               --google-machine-type n1-standard-2 \
               --google-disk-size=100 \
               --google-disk-type=pd-ssd \
               --google-machine-image https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/centos-7-v20200309 \
               worker1
```

```bash
docker-machine create \
               --driver google --google-project ferrous-aleph-271712 \
               --google-zone europe-west1-b \
               --google-machine-type n1-standard-2 \
               --google-disk-size=100 \
               --google-disk-type=pd-ssd \
               --google-machine-image https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/centos-7-v20200309 \
               worker2
```

&rarr; *Setup swarm master with* :

```bash
docker-machine ssh master sudo docker swarm init
```

&rarr; *Setup each swarm worker with* :

```bash
docker-machine ssh worker1 sudo docker swarm join --token SWMTKN-1-5zfy2iio54tma997pnt96gq5095fimqn2hxr2a8j16ogq0n3c9-0kp6mi5iuj956gpl9sfccd5bo 10.132.0.8:2377
```

```bash
docker-machine ssh worker2 sudo docker swarm join --token SWMTKN-1-5zfy2iio54tma997pnt96gq5095fimqn2hxr2a8j16ogq0n3c9-0kp6mi5iuj956gpl9sfccd5bo 10.132.0.8:2377
```

&rarr; *Activate master environment* :

```bash
docker-machine env master
eval $(docker-machine env master)
```

&rarr; *List swarm nodes with* :

```bash
docker node ls
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

#### *Deployment*

&rarr; *Deploy configuration on swarm with* :

```bash
docker stack deploy -c ../swarm-spark/docker-compose.yml mystack
```

&rarr; *Check status with* :

```bash
docker stack ls
docker service ls
docker network ls
```

&rarr; *Attach client containers as usual with* :

```bash
docker run --network mystack_default --env-file ../swarm-spark/hadoop.env -it bde2020/hadoop-base bash
```

&rarr; *Remove configuration from swarm with* :

```bash
docker stack rm mystack
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
    $ docker run --env-file hadoop.env --network docker-hadoop_default -p 12345:12345 streamgen hdfs:///input/title.ratings.tsv 120
    ```

* Parâmetros :
    * **1º parâmetro**: localização do ficheiro do *IMDb* (local, *http* ou *hdfs*);
    * **2º parâmetro:** número de eventos gerados por minuto.

Para testar, ligar a `localhost:12345`, por exemplo com :

```bash
$ nc localhost 12345
```
