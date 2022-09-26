package CRUD;// HBase
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
// Java
import java.io.IOException;

public class DeleteTable {
    public static void main(String[] args) throws IOException{
        // Base for all Hadoop ecosystem config objects
        Configuration config = HBaseConfiguration.create();
        // Connection factory manages creation of connections to clusters using config above
        Connection connection = ConnectionFactory.createConnection(config);

        // Automatic resource management - Closes the connection, without a "finally" block
        try(connection) {
            // Admin obj for command permission
            Admin admin = connection.getAdmin();
            // Table name for admin obj
            TableName tableName = TableName.valueOf("census");

            // If table does NOT exist, create it
            if (admin.tableExists(tableName)){
                System.out.print("Table exists, deleting...  ");

                // Admin command
                admin.disableTable(tableName); // In memory contents, indexed, change log flushed & merged to disk.
                admin.deleteTable(tableName);

                System.out.println("Done.");
                // Check with - describe 'table name'
            }else{
                System.out.println("Table does not exist.  Closing connection!");
            }
            // Automatic resource management - Closes the connection, without a "finally" block
        }
    }
}

