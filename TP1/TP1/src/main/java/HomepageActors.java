import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.InputMismatchException;
import java.util.Scanner;

/**
 * HomepageActors
 */
public class HomepageActors {
    public static void main(String[] args) throws Exception {
        /* Tratamento do input */
        Scanner s = new Scanner(System.in);
        System.out.print("Indique o identificador do ator que pretende consultar: ");

        String idActor;
        try {
            idActor = s.next();
        } catch (InputMismatchException ime) {
            System.out.println("Por favor introduza um identificador de um ator válido!");
            return;
        } catch (Exception e) {
            e.printStackTrace();
            return;
        } finally {
            s.close();
        }

        /* Configuração e conexão */
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "zoo");

        Connection conn = ConnectionFactory.createConnection(conf);

        /* Obtenção da tabela */
        Table table = conn.getTable(TableName.valueOf("actors"));

        /* Processamento dos dados */
        Get g = new Get(Bytes.toBytes(idActor));
        Result res = table.get(g);

        System.out.println("\nHomepage of actor \"" + idActor + "\" :\n");

        // Coluna "info"
        String name = Bytes.toString(res.getValue(Bytes.toBytes("details"), Bytes.toBytes("name")));
        System.out.println("Name: " + name);

        String birth = Bytes.toString(res.getValue(Bytes.toBytes("details"), Bytes.toBytes("birthYear")));
        System.out.println("Birth: " + birth);

        String death = Bytes.toString(res.getValue(Bytes.toBytes("details"), Bytes.toBytes("deathYear")));
        System.out.println("Death: " + death);

        // Coluna "movies"
        String total = Bytes.toString(res.getValue(Bytes.toBytes("movies"), Bytes.toBytes("total")));
        System.out.println("Number of movies: " + total);

        /* Fechar conexões */
        table.close();
        conn.close();
    }
}