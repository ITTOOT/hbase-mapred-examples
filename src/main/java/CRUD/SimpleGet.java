package CRUD;// HBase
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
// Java
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SimpleGet {
    // All data in HBase is stored in byte arrays.
    // Column Families
    private static byte[] PERSONAL_CF = Bytes.toBytes("personal");
    private static byte[] PROFESSIONAL_CF = Bytes.toBytes("professional");
    // Columns
    // Personal:
    private static byte[] NAME_COLUMN = Bytes.toBytes("name");
    private static byte[] FIELD_COLUMN = Bytes.toBytes("field");

    public static void main(String[] args) throws IOException {
        // Base for all Hadoop ecosystem config objects
        Configuration config = HBaseConfiguration.create();
        // Connection factory manages creation of connections to clusters using config above
        Connection connection = ConnectionFactory.createConnection(config);
        // Table instance, from the configured connection, of the specified name
        Table table = connection.getTable(TableName.valueOf("census"));

        // Automatic resource management - Closes the connection, without a "finally" block
        try(connection; table) {
            // Table instance, from the configured connection, of the specified name
            // GET obj with row key
            Get get_1 = new Get(Bytes.toBytes("1"));
            // Helpers for above operation
            get_1.addColumn(PERSONAL_CF, NAME_COLUMN);
            get_1.addColumn(PROFESSIONAL_CF, FIELD_COLUMN);
            // Execute
            Result result = table.get(get_1);

            byte[] nameValue = result.getValue(PERSONAL_CF, NAME_COLUMN);
            System.out.println("Name: " + Bytes.toString(nameValue) + " retrieved from table.  ");

            byte[] fieldValue = result.getValue(PROFESSIONAL_CF, FIELD_COLUMN);
            System.out.println("Field: " + Bytes.toString(fieldValue) + " retrieved from table.  ");

            System.out.println("Done.");
            // Check with - describe 'table name'

            ///////////////////////// BATCH OPERATION ////////////////////////////
            System.out.println();
            System.out.println("Get multiple results. ");

            // Build batch
            List<Get> getList = new ArrayList<Get>();
            Get get_2 = new Get(Bytes.toBytes("2"));
            get_2.addColumn(PERSONAL_CF, NAME_COLUMN);
            Get get_3 = new Get(Bytes.toBytes("3"));
            get_3.addColumn(PERSONAL_CF, NAME_COLUMN);
            getList.add(get_2);
            getList.add(get_3);
            // Execute
            Result[] results = table.get(getList);
            // Extract results from object
            for (Result res : results) {
                nameValue = res.getValue(PERSONAL_CF, NAME_COLUMN);
                System.out.println("Name: " + Bytes.toString(nameValue) + " retrieved from table.  ");
            }
        }
        // Automatic resource management - Closes the connection, without a "finally" block
        // Close table if still open
    }
}
