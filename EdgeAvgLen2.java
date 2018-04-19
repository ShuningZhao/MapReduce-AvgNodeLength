/**************************************************
* EdgeAvgLen2 
* Given a directed graph, compute the average length of
* the in-coming edges for each node with in-mappper combiner
* By Shuning Zhao
* 2018-04-16
**************************************************/

package mapreduceprojects.avglen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map;

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

public class EdgeAvgLen2 {

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
        
        public void set(Pair p) {
        	count_ = p.getCount();
        	sum_ = p.getSum();
        }

        public int getCount() {
            return count_;
        }

        public double getSum() {
            return sum_;
        }
        
        public void CombineCount(int n) {
        	count_ += n;
        }
        
        public void CombineSum(double n) {
        	sum_ += n;
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
    // - The Mapper Class with in-mapper combiner
    // - Extracts 3rd and 4th column of the input table (ToNodeId and Distance)
    // - Stores the distance to the ToNodeId with count being 1.
    //==========================================================================
    public static class EdgeLenMapper extends Mapper<Object, Text, IntWritable, Pair> {
    	private Pair pair = new Pair();
        private IntWritable node = new IntWritable();
        private static HashMap<Integer, Pair> map = new HashMap<>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            
            while (itr.hasMoreTokens()) {
                String line = itr.nextToken().toLowerCase();
                String[] l = line.split(" ");
                int ToNode = Integer.parseInt(l[2]);
                double i = Double.parseDouble(l[3]);
                
                if (map.containsKey(ToNode)) {
                	Pair pPair = map.get(ToNode);
                	pPair.CombineCount(1);
                	pPair.CombineSum(i);
                } else {
                	map.put(ToNode, new Pair(1, i));
                }
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
        	for (Map.Entry<Integer, Pair> entry : map.entrySet()) {
        		node.set(entry.getKey());
        		pair.set(entry.getValue());
        		context.write(node, pair);
        		}
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
        job.setJarByClass(EdgeAvgLen2.class);
        job.setMapperClass(EdgeLenMapper.class);
        job.setReducerClass(EdgeLenReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Pair.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}