package MapReduce;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
// Java
import java.io.IOException;

// Base mapper in HBase lib - Output types Writable allows for serialising
public class Mapper extends
        TableMapper<ImmutableBytesWritable, IntWritable> {
    // Constants
    public static final byte[] CF = "personal".getBytes();
    public static final byte[] MARITAL_STATUS = "marital_status".getBytes();
    // Output count for every row always 1
    private final IntWritable ONE = new IntWritable();
    private ImmutableBytesWritable key = new ImmutableBytesWritable();

    // Mapper - processes one record at a time <key, value> pair, context = bridge to the outside world
    public  void map(ImmutableBytesWritable row, Result value, Context context)
        throws IOException, InterruptedException{
        // Extract family:column value and set as key
        key.set(value.getValue(CF, MARITAL_STATUS));
        // Write the key to the context
        context.write(key, ONE);
    }
}
