package CRUD.Finally;// HBase

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class DeleteTable {
    public static void main(String[] args) throws IOException{
        // Base for all Hadoop ecosystem config objects
        Configuration config = HBaseConfiguration.create();
        // Connection factory manages creation of connections to clusters using config above
        Connection connection = ConnectionFactory.createConnection(config);
        try{
            // Admin for commands
            Admin admin = connection.getAdmin();
            // Retrieve table & column families
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

        }finally {
            // Always close the connection after use
            connection.close();
        }
    }
}

