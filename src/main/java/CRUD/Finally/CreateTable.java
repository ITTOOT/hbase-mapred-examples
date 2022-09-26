package CRUD.Finally;// HBase
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
// Java
import java.io.IOException;

public class CreateTable {
    public static void main(String[] args) throws IOException{
        // Base for all Hadoop ecosystem config objects
        Configuration config = HBaseConfiguration.create();
        // Connection factory manages creation of connections to clusters using config above
        Connection connection = ConnectionFactory.createConnection(config);
        try{
            // Admin for commands
            Admin admin = connection.getAdmin();
            // Build table & column families
            HTableDescriptor tableName = new HTableDescriptor(TableName.valueOf("census"));
            // Helpers for above operation
            tableName.addFamily(new HColumnDescriptor("personal"));
            tableName.addFamily(new HColumnDescriptor("professional"));

            // If table does NOT exist, create it
            if (!admin.tableExists(tableName.getTableName())){
                System.out.print("Creating table...  ");

                // Admin command
                admin.createTable(tableName);

                System.out.println("Done.");
                // Check with - describe 'table name'
            }else{
                System.out.println("Table already exists.  Closing connection!");
            }

        }finally {
            // Always close the connection after use
            connection.close();
        }
    }
}
