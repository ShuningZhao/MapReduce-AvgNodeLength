/**************************************************
* COMP9313 Project 1
* Shuning Zhao z3332916
* EdgeAvgLen1
**************************************************/

package comp9313.ass1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EdgeAvgLen1 {

    //==========================================================================
    // Pair
    // - Class to store a count and a sum of distance for each ToNodeId
    //==========================================================================
    public static class Pair implements Writable {
        private int count_;
        private double sum_;

        public Pair() {
            set(0, 0);
        };

        public Pair(int count, double sum) {
            set(count, sum);
        }

        public void set(int count, double sum) {
            count_ = count;
            sum_ = sum;
        }

        public int getCount() {
            return count_;
        }

        public double getSum() {
            return sum_;
        }

        public void write(DataOutput out) throws IOException {
            out.writeInt(count_);
            out.writeDouble(sum_);
        }

        public void readFields(DataInput in) throws IOException {
            count_ = in.readInt();
            sum_ = in.readDouble();
        }
    }

    //==========================================================================
    // EdgeLenMapper
    // - The Mapper Class
    // - Extracts 3rd and 4th column of the input table (ToNodeId and Distance)
    // - Stores the distance to the ToNodeId with count being 1.
    //==========================================================================
    public static class EdgeLenMapper extends Mapper<Object, Text, IntWritable, Pair> {
    	private Pair pair = new Pair();
        private IntWritable node = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            
            while (itr.hasMoreTokens()) {
                String line = itr.nextToken().toLowerCase();
                String[] l = line.split(" ");
                int j = Integer.parseInt(l[2]);
                double i = Double.parseDouble(l[3]);
                node.set(j);
                pair.set(1, i);
                context.write(node, pair);
                }
            }
        }

    //==========================================================================
    // EdgeLenCombiner
    // - The Combiner Class
    // - Combines the input pairs from the Mapper class
    // - Creates a new pair to store the combined results
    //==========================================================================
    public static class EdgeLenCombiner extends Reducer<IntWritable, Pair, IntWritable, Pair> {
        private Pair result = new Pair();

        public void reduce(IntWritable key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            double sum = 0;
            
            for (Pair val : values) {
                count += val.getCount();
                sum += val.getSum();
                System.out.print(sum);
            }
            
            result.set(count, sum);
            context.write(key, result);
           
        }
    }

    //==========================================================================
    // EdgeLenReducer
    // - The Reducer Class
    // - Calculates the average length with the input pairs
    // - Writes to output
    //==========================================================================
    public static class EdgeLenReducer extends Reducer<IntWritable, Pair, IntWritable, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(IntWritable key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            double count = 0;
            
            for (Pair val : values) {
                count += val.getCount();
                sum += val.getSum();
            }
            double avg = sum / count;
            result.set(avg);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Length of the in-coming edges for each node");
        job.setJarByClass(EdgeAvgLen1.class);
        job.setMapperClass(EdgeLenMapper.class);
        job.setReducerClass(EdgeLenReducer.class);
        job.setCombinerClass(EdgeLenCombiner.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Pair.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}