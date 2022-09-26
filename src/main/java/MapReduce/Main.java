package MapReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.hbase.client.*;
// Java
import java.io.IOException;

public class Main {
    // Entry point
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // Base for all Hadoop ecosystem config objects
        Configuration config = HBaseConfiguration.create();
        // Needs to know which archive holds mapper and reducer classes
        Job job = Job.getInstance(config);
        job.setJarByClass(Main.class); // Archive that holds Main, also holds Mapper & Reducer
        // Tables
        String sourceTable = "census";
        String targetTable = "summary";
        // Scan obj - with options
        Scan scan = new Scan();
        scan.setCaching(500); // Default is 1 record
        scan.setCacheBlocks(false); // Do not store in memory if not to be used again after mapReduce
        // Which jars the job depends on
        TableMapReduceUtil.addDependencyJars(job);
        // Initialise the table mapper job
        TableMapReduceUtil.initTableMapperJob(
                sourceTable,
                scan,
                Mapper.class,
                ImmutableBytesWritable.class,
                IntWritable.class,
                job);
        // Initialise the reduce job
        TableMapReduceUtil.initTableReducerJob(
                targetTable,
                Reducer.class,
                job);
        // Processes
        job.setNumReduceTasks(1);
        // Submit to cluster
        job.waitForCompletion(true);
        // Must be packaged into a jar, to submit to hadoop via the command line.
    }
}