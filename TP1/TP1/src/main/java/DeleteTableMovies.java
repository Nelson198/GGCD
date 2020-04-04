import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * DeleteTableMovies
 */
public class DeleteTableMovies {
    public static void main(String[] args) throws IOException {
        /* Configuração e conexão */
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "zoo");

        Connection conn = ConnectionFactory.createConnection(conf);

        /* Remoção da tabela */
        Admin admin = conn.getAdmin();

        // Verificar se a tabela "movies" existe
        if (admin.tableExists(TableName.valueOf("movies"))) {
            admin.disableTable(TableName.valueOf("movies"));
            admin.deleteTable(TableName.valueOf("movies"));
            System.out.println("A tabela \"movies\" foi removida com sucesso !");
        } else {
            System.out.println("A tabela \"movies\" não existe !");
        }

        /* Fechar conexão */
        admin.close();
        conn.close();
    }
}