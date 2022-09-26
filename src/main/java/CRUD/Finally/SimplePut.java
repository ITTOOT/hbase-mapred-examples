package CRUD.Finally;// HBase
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
// Java
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SimplePut {
    // All data in HBase is stored in byte arrays.
    // Column Families
    private static byte[] PERSONAL_CF = Bytes.toBytes("personal");
    private static byte[] PROFESSIONAL_CF = Bytes.toBytes("professional");
    // Columns
    // Personal:
    private static byte[] NAME_COLUMN = Bytes.toBytes("name");
    private static byte[] GENDER_COLUMN = Bytes.toBytes("gender");
    private static byte[] MARITAL_STATUS_COLUMN = Bytes.toBytes("marital_status");
    // Professional:
    private static byte[] EMPLOYED_COLUMN = Bytes.toBytes("employed");
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
            // PUT command with row key
            Put put_1 = new Put(Bytes.toBytes("1"));
            // Helpers for above operation
            put_1.addColumn(PERSONAL_CF, NAME_COLUMN, Bytes.toBytes("Mike Jones"));
            put_1.addColumn(PERSONAL_CF, GENDER_COLUMN, Bytes.toBytes("male"));
            put_1.addColumn(PERSONAL_CF, MARITAL_STATUS_COLUMN, Bytes.toBytes("unmarried"));
            put_1.addColumn(PROFESSIONAL_CF, FIELD_COLUMN, Bytes.toBytes("construction"));
            // Execute
            table.put(put_1);

            String name = "Mike Jones";
            System.out.println("Row for " + name + " inserted into table.");
            // Check with - describe 'table name'

            ///////////////////////// BATCH OPERATION ////////////////////////////
            // PUT command with row key
            Put put_2 = new Put(Bytes.toBytes("2"));
            // Helpers for above operation
            put_2.addColumn(PERSONAL_CF, NAME_COLUMN, Bytes.toBytes("Hillary Clinton"));
            put_2.addColumn(PERSONAL_CF, GENDER_COLUMN, Bytes.toBytes("female"));
            put_2.addColumn(PERSONAL_CF, MARITAL_STATUS_COLUMN, Bytes.toBytes("married"));
            put_2.addColumn(PROFESSIONAL_CF, FIELD_COLUMN, Bytes.toBytes("politics"));

            // PUT command with row key
            Put put_3 = new Put(Bytes.toBytes("3"));
            // Helpers for above operation
            put_3.addColumn(PERSONAL_CF, NAME_COLUMN, Bytes.toBytes("Donald Trump"));
            put_3.addColumn(PERSONAL_CF, GENDER_COLUMN, Bytes.toBytes("male"));
            put_3.addColumn(PROFESSIONAL_CF, FIELD_COLUMN, Bytes.toBytes("politics, real"));

            // Build batch
            List<Put> putList = new ArrayList<Put>();
            putList.add(put_2);
            putList.add(put_3);
            // Execute
            table.put(putList);

            String name_2 = "Hillary Clinton";
            String name_3 = "Donald Trump";
            System.out.println("Row for " + name_2 + " & " + name_3 + " inserted into table.");
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
