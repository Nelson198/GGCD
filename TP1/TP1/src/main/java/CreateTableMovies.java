import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * CreateTableMovies
 */
public class CreateTableMovies {
    public static void main(String[] args) throws IOException {
        /* Configuração e conexão */
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "zoo");

        Connection conn = ConnectionFactory.createConnection(conf);

        /* Criação da tabela */
        Admin admin = conn.getAdmin();

        /* Tabela "movies" */
        HTableDescriptor htd1 = new HTableDescriptor(TableName.valueOf("movies"));
        htd1.addFamily(new HColumnDescriptor("details"));

        // Verificar se a tabela "movies" existe
        if (admin.tableExists(TableName.valueOf("movies"))) {
            System.out.println("A tabela \"movies\" já existe !");
        } else {
            admin.createTable(htd1);
            System.out.println("A tabela \"movies\" foi criada com sucesso !");
        }

        /* Fechar conexão */
        admin.close();
        conn.close();
    }
}
