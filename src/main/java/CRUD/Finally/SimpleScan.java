package CRUD.Finally;// HBase
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.*;
// Java
import java.io.IOException;

public class SimpleScan {
    public static void main(String[] args) throws IOException {
        // Base for all Hadoop ecosystem config objects
        Configuration config = HBaseConfiguration.create();
        // Connection factory manages creation of connections to clusters using config above
        Connection connection = ConnectionFactory.createConnection(config);

        Table table = null;
        ResultScanner scanResult = null; // Iterable
        try{
            // Table instance, from the configured connection, of the specified name
            table = connection.getTable(TableName.valueOf("census"));
            // Scan obj - all scan methods available
            Scan scan = new Scan();
            // Helpers for above operation
            scanResult = table.getScanner(scan); // Whole table

            // Extract results
            for (Result res : scanResult){
                // List all cells with data returned
                for (Cell cell : res.listCells()){
                    // Retrieve 4 dimensions of a cell, in byte array
                    String row = new String(CellUtil.cloneRow(cell));
                    String family = new String(CellUtil.cloneFamily(cell));
                    String column = new String(CellUtil.cloneQualifier(cell));
                    String value = new String(CellUtil.cloneValue(cell));
                    String tag = new String(CellUtil.cloneTags(cell));
                    System.out.println(row + " " + family + " " + column + " " + value + " " + tag);
                }
            }

            System.out.println("Done.");
            // Check with - describe 'table name'

        }finally {
            // Always close the connection after use
            connection.close();
            // Close table if still open
            if (table != null){
                table.close();
            }
            // Close scan if still open
            if (scanResult != null){
                scanResult.close();
            }
        }
    }
}
