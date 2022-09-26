package MapReduce;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
// Java
import java.io.IOException;

// Base reducer in HBase lib - Output types Writable allows for serialising
public class Reducer extends
        TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {
    // Constants
    public static final byte[] CF = "marital_status".getBytes();
    public static final byte[] COUNT = "count".getBytes();

    // Reducer - processes one record at a time <key, list<value>> pair, context = bridge to the outside world
    public  void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
        // Store for values
        Integer sum = 0;
        // Calculation to perform on values
        for(IntWritable val : values){
            sum += val.get();
        }
        // Get the key and save in put operation
        Put put = new Put(key.get());
        // Create a column and add the calculated value
        put.addColumn(CF, COUNT, Bytes.toBytes(sum.toString()));
        // Write mutation to table with the key from mapping and the new column and calc value
        context.write(key, put);
    }
}

