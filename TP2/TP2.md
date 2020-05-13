# Gestão de Grandes Conjuntos de Dados - 2019/2020

## TP2

### *Streamgen*

***Streamgen*** : gerador de dados para teste de processamento de *streams*.

* Como compilar :

    ```bash
    $ mvn package
    $ docker build -t streamgen
    ```

* Modo de usar como servidor local :

    ```bash
    $ java -jar target/TP2-1.0-SNAPSHOT.jar ./title.ratings.tsv.gz 120
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

