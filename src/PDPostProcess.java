import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class PDPostProcess extends Configured implements Tool {

  public static class PostProcessMapper extends
      Mapper<IntWritable, PDNodeWritable, IntWritable, PDNodeWritable> {

    @Override
    protected void map(IntWritable key, PDNodeWritable value, Context context)
        throws IOException, InterruptedException {
      context.write(key, value);
    }
  }

  public static class PostProcessReducer extends
      Reducer<IntWritable, PDNodeWritable, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<PDNodeWritable> nodes, Context context)
        throws IOException, InterruptedException {
      for (PDNodeWritable node : nodes) {
        // Reachable nodes
        if (node.distance.get() != Integer.MAX_VALUE) {
          Text output = new Text(node.distance.get() + " " + node.prevNodeId.get());
          context.write(key, output); // Serialize the node
        }
      }
    }
  }

  @Override
  public int run(String[] strings) throws Exception {
    Path inputPath = new Path(strings[0]);
    Path outputPath = new Path(strings[1]);
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "PDPostProcess");

    job.setJarByClass(PDPostProcess.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapperClass(PostProcessMapper.class);
    job.setReducerClass(PostProcessReducer.class);
    job.setMapOutputValueClass(PDNodeWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    SequenceFileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.waitForCompletion(true);
    return 0;
  }
}
