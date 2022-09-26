package Filters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
// Java
import java.io.IOException;

public class FilterOnColumnValues {
    // Results of filtering
    private static void printResults(ResultScanner scanResult){
        System.out.println();
        System.out.println("Results: ");

        // Loop through every result, then every cell for a result
        for (Result res : scanResult) {
            for (Cell cell : res.listCells()) {
                String row = new String(CellUtil.cloneRow(cell));
                String family = new String(CellUtil.cloneFamily(cell));
                String column = new String(CellUtil.cloneQualifier(cell));
                String value = new String(CellUtil.cloneValue(cell));

                // Print results
                System.out.println(row + " " + family + " " + column + " " + value);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        // Base for all Hadoop ecosystem config objects
        Configuration config = HBaseConfiguration.create();
        //ResultScanner scanResult = null; // Iterable
        //Table table = null;
        // Connection factory manages creation of connections to clusters using config above
        Connection connection = ConnectionFactory.createConnection(config);
        // Table instance, from the configured connection, of the specified name
        Table table = connection.getTable(TableName.valueOf("census"));
        // Scan obj - all scan methods available
        Scan scan = new Scan();
        // Filter 1 type - With assigned comparator value
//        SingleColumnValueFilter filter = new SingleColumnValueFilter(
//                Bytes.toBytes("personal"), // column family
//                Bytes.toBytes("gender"), // column
//                CompareFilter.CompareOp.EQUAL, // row
//                new BinaryComparator(Bytes.toBytes("male"))); // Binary comparators compare any type of data
        // Filter 2 type - With assigned comparator value
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                Bytes.toBytes("personal"), // column family
                Bytes.toBytes("name"), // column
                CompareFilter.CompareOp.EQUAL, // row - EQUAL/NOT_EQUAL
                new SubstringComparator("Jones")); // Finds WHERE column is LIKE comparator string
        // Filter options
        filter.setFilterIfMissing(true);
        // Filter above applied to scan obj
        scan.setFilter(filter);
        // Helpers for above operation
        ResultScanner scanResult = table.getScanner(scan); // Whole table

        // Automatic resource management - Closes the connection, without a "finally" block
        try(connection; table; scanResult) {
            // Output results
            printResults(scanResult);
            // Done
            System.out.println("Done.");
            // Check with - describe 'table name'
        }
        // Automatic resource management - Closes the connection, without a "finally" block
        // Close table if still open
        // Close scanner if still open
    }
}
