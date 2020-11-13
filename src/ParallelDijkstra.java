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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ParallelDijkstra extends Configured implements Tool {

  public static class PDMapper
      extends Mapper<IntWritable, PDNodeWritable, IntWritable, PDNodeWritable> {

    @Override
    protected void map(IntWritable key, PDNodeWritable value, Context context)
        throws IOException, InterruptedException {

      context.write(key, value);
    }
  }

  public static class PDReducer
      extends Reducer<IntWritable, PDNodeWritable, IntWritable, PDNodeWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<PDNodeWritable> values, Context context)
        throws IOException, InterruptedException {

      for (PDNodeWritable val : values) {
        context.write(key, val); // Serialize the node
      }
    }
  }

  @Override
  public int run(String[] strings) throws Exception {
    Path inputPath = new Path(strings[0]);
    Path outputPath = new Path(strings[1]);
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "ParallelDijkstra");

    job.setJarByClass(ParallelDijkstra.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapperClass(PDMapper.class);
    job.setReducerClass(PDReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PDNodeWritable.class);
    // Writes binary files suitable for reading into subsequent MapReduce jobs
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    SequenceFileInputFormat.addInputPath(job, inputPath);
    SequenceFileOutputFormat.setOutputPath(job, outputPath);

    job.waitForCompletion(true);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      System.out
          .println("hadoop jar [.jar file] ParallelDijkstra [infile] [outdir] [src] [iterations]");
      System.exit(1);
    }
    String inputPath = args[0];
    String outputPath = args[1];
    String tempPath = "tmp";
    String src = args[2];
    String iterations = args[3];

    System.out.println("--------------------Running PDPreProcess--------------------");
    ToolRunner.run(new PDPreProcess(), new String[]{inputPath, tempPath, src});
    System.out.println("--------------------Running ParallelDijkstra--------------------");
    ToolRunner.run(new ParallelDijkstra(), new String[]{tempPath, "tmp-out"});
    System.out.println("--------------------Running PDPostProcess--------------------");
    ToolRunner.run(new PDPostProcess(), new String[]{"tmp-out", outputPath});
    System.exit(0);
  }
}