package CRUD.Finally;// HBase
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
// Java
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SimpleDelete {
    // All data in HBase is stored in byte arrays.
    // Column Families
    private static byte[] PERSONAL_CF = Bytes.toBytes("personal");
    private static byte[] PROFESSIONAL_CF = Bytes.toBytes("professional");
    // Columns
    // Personal:
    private static byte[] GENDER_COLUMN = Bytes.toBytes("gender");
    private static byte[] FIELD_COLUMN = Bytes.toBytes("field");

    public static void main(String[] args) throws IOException {
        // Base for all Hadoop ecosystem config objects
        Configuration config = HBaseConfiguration.create();
        // Connection factory manages creation of connections to clusters using config above
        Connection connection = ConnectionFactory.createConnection(config);

        Table table = null;
        try{
            // Table instance, from the configured connection, of the specified name
            table = connection.getTable(TableName.valueOf("census"));
            // DELETE
            Delete delete = new Delete(Bytes.toBytes("1"));
            // Helpers for above operation
            delete.addColumn(PERSONAL_CF, GENDER_COLUMN);
            delete.addColumn(PROFESSIONAL_CF, FIELD_COLUMN);
            // Execute
            table.delete(delete);

            System.out.println("Delete done.");
            // Check with - describe 'table name'
        }finally {
            // Always close the connection after use
            connection.close();
            // Close table if still open
            if (table != null){
                table.close();
            }
        }
    }
}
