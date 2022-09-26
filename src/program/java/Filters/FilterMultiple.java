package Filters;
import org.apache.commons.collections.list.SynchronizedList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
// Java
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FilterMultiple {
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
        SingleColumnValueFilter filter_1 = new SingleColumnValueFilter(
                Bytes.toBytes("personal"), // column family
                Bytes.toBytes("marital_status"), // column
                CompareFilter.CompareOp.EQUAL, // row - EQUAL/NOT_EQUAL
                new BinaryComparator(Bytes.toBytes("divorced"))); // Finds WHERE column is LIKE comparator string
        // filter options
        filter_1.setFilterIfMissing(true);
        // Filter 1 type - With assigned comparator value
        SingleColumnValueFilter filter_2 = new SingleColumnValueFilter(
                Bytes.toBytes("personal"), // column family
                Bytes.toBytes("gender"), // column
                CompareFilter.CompareOp.EQUAL, // row - EQUAL/NOT_EQUAL
                new BinaryComparator(Bytes.toBytes("male"))); // Finds WHERE column is LIKE comparator string
        // Filter options
        filter_2.setFilterIfMissing(true);
        // Filter list obj
        List<Filter> filterList = new ArrayList<Filter>();
        // Helpers for above
        filterList.add(filter_1);
        filterList.add(filter_2);
        FilterList filters = new FilterList(filterList);

        // Filter above applied to scan obj
        scan.setFilter(filters); // Enum must pass 1

        // Helpers for above operation
        ResultScanner scanResult = table.getScanner(scan); // Whole table

        // Automatic resource management - Closes the connection, without a "finally" block
        try(connection; table; scanResult) {
            // Output results
            printResults(scanResult);

            System.out.println("Done.");
            // Check with - describe 'table name'
        }
        // Automatic resource management - Closes the connection, without a "finally" block
        // Close table if still open
        // Close scanner if still open
    }
}

