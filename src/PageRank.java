import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRank extends Configured implements Tool {

  public static class PRMapper
      extends Mapper<IntWritable, PRNodeWritable, IntWritable, PRNodeWritable> {

    @Override
    protected void map(IntWritable key, PRNodeWritable node, Context context)
        throws IOException, InterruptedException {
      Writable[] toNodeIds = node.edges.get();
      double rankToPass = node.rank.get() / node.edges.get().length;
      context.write(key, node); // Emit itself

      for (Writable writable : toNodeIds) {
        IntWritable toNodeId = (IntWritable) writable;
        PRNodeWritable neighborNode = new PRNodeWritable(toNodeId.get(), rankToPass);
        context.write(toNodeId, neighborNode);
      }
    }
  }

  public static class PRReducer
      extends Reducer<IntWritable, PRNodeWritable, IntWritable, PRNodeWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<PRNodeWritable> nodes, Context context)
        throws IOException, InterruptedException {

      Writable[] edges = new IntWritable[0];
      double sum = 0.0;

      for (PRNodeWritable node : nodes) {
        if (node.edges.get().length == 0) {
          // Contain only page rank value
          sum += node.rank.get();
        } else {
          edges = node.edges.get();
        }
      }

      PRNodeWritable node = new PRNodeWritable(key.get(), sum);
      node.setEdges(edges);
      context.write(key, node); // Serialize the node
    }
  }

  private static class PRPostProcess extends Configured implements Tool {

    public static class PRMapper
        extends Mapper<IntWritable, PRNodeWritable, IntWritable, Text> {

      @Override
      protected void map(IntWritable key, PRNodeWritable node, Context context)
          throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        double threshold = Double.parseDouble(conf.get("threshold"));
        double rank = node.rank.get();
        if (rank > threshold) {
          context.write(key, new Text(String.valueOf(rank)));
        }
      }
    }

    @Override
    public int run(String[] strings) throws Exception {
      Path inputPath = new Path(strings[0]);
      Path outputPath = new Path(strings[1]);
      String threshold = strings[2];
      Configuration conf = new Configuration();
      conf.set("threshold", threshold);
      Job job = Job.getInstance(conf, "PRPostProcess");

      job.setJarByClass(PRPostProcess.class);
      job.setInputFormatClass(SequenceFileInputFormat.class);
      job.setMapperClass(PRMapper.class);
      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(Text.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      SequenceFileInputFormat.addInputPath(job, inputPath);
      TextOutputFormat.setOutputPath(job, outputPath);

      job.waitForCompletion(true);
      return 0;
    }
  }

  @Override
  public int run(String[] strings) throws Exception {
    Path inputPath = new Path(strings[0]);
    Path outputPath = new Path(strings[1]);
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "PageRank");

    job.setJarByClass(PageRank.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapperClass(PRMapper.class);
    job.setReducerClass(PRReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PRNodeWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    SequenceFileInputFormat.addInputPath(job, inputPath);
    SequenceFileOutputFormat.setOutputPath(job, outputPath);

    job.waitForCompletion(true);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 5) {
      System.out
          .println(
              "hadoop jar [.jar file] PageRank [alpha] [iteration] [threshold] [infile] [outdir]");
      System.exit(1);
    }
    String threshold = args[2];
    String inputPath = args[3];
    String outputPath = args[4];
    String tempPath = "tmp-";
    int iterations = Integer.parseInt(args[1]);

    // PreProcess
    ToolRunner.run(new PRPreProcess(), new String[]{inputPath, tempPath + 0});

    // PageRank Algorithm
    for (int i = 0; i < iterations; i++) {
      runPageRank(tempPath, i);
    }

    // PostProcess
    ToolRunner.run(new PRPostProcess(), new String[]{tempPath + iterations, outputPath, threshold});

    System.exit(0);
  }

  private static int runPageRank(String tempPath, int i) throws Exception {
    return ToolRunner.run(new PageRank(), new String[]{tempPath + i, tempPath + (i + 1)});
  }
}