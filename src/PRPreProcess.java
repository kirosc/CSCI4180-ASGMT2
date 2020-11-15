import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

public class PRPreProcess extends Configured implements Tool {

  public static class TokenizerMapper
      extends Mapper<Object, Text, IntWritable, IntWritable> {

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString());
      String fromNodeId, toNodeId;

      while (itr.hasMoreTokens()) {
        fromNodeId = itr.nextToken();
        toNodeId = itr.nextToken();
        itr.nextToken(); // Skip weight
        context.write(
            new IntWritable(Integer.parseInt(fromNodeId)),
            new IntWritable(Integer.parseInt(toNodeId))
        );
      }
    }
  }

  public static class EdgeReducer
      extends Reducer<IntWritable, IntWritable, IntWritable, PRNodeWritable> {

    public void reduce(IntWritable key, Iterable<IntWritable> toNodes, Context context)
        throws IOException, InterruptedException {

      PRNodeWritable node = new PRNodeWritable(key);
      ArrayList<IntWritable> edges = new ArrayList<>();

      // Adjacency list
      for (IntWritable toNodeId : toNodes) {
        edges.add(new IntWritable(toNodeId.get()));
      }
      node.setEdges(edges.toArray(new IntWritable[0]));

      context.write(key, node); // Serialize the node
    }
  }

  @Override
  public int run(String[] strings) throws Exception {
    Path inputPath = new Path(strings[0]);
    Path outputPath = new Path(strings[1]);
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "PreProcess");

    job.setJarByClass(PRPreProcess.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(EdgeReducer.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PRNodeWritable.class);
    // Writes binary files suitable for reading into subsequent MapReduce jobs
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    FileInputFormat.addInputPath(job, inputPath);
    SequenceFileOutputFormat.setOutputPath(job, outputPath);

    job.waitForCompletion(true);
    return 0;
  }
}