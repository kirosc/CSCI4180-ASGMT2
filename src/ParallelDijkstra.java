import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ParallelDijkstra extends Configured implements Tool {

  public static class PDMapper
      extends Mapper<IntWritable, PDNodeWritable, IntWritable, PDNodeWritable> {

    private static final int TO_NODE_ID = 0;
    private static final int WEIGHT = 1;

    @Override
    protected void map(IntWritable key, PDNodeWritable node, Context context)
        throws IOException, InterruptedException {
      int distance = node.distance.get();
      context.write(key, node); // Emit itself

      for (Writable[] edge : node.edges.get()) {
        IntWritable id = (IntWritable) edge[TO_NODE_ID];
        int weight = ((IntWritable) edge[WEIGHT]).get();
        int newDistance = distance == Integer.MAX_VALUE ? Integer.MAX_VALUE : distance + weight;
        // Writing (toNodeId, prevNodeId, distance)
        context.write(id, new PDNodeWritable(id, new IntWritable(newDistance), key));
      }
    }
  }

  public static class PDReducer
      extends Reducer<IntWritable, PDNodeWritable, IntWritable, PDNodeWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<PDNodeWritable> nodes, Context context)
        throws IOException, InterruptedException {

      int distance = Integer.MAX_VALUE;
      IntWritable prevNodeId = new IntWritable(-1);
      Writable[][] edges = new IntWritable[0][];

      for (PDNodeWritable node : nodes) {

        System.out.println("--------------------" + node.id.get() + "--------------------");
        System.out.println(node.toString());
        System.out.println("----------------------------------------");

        int mDistance = node.distance.get();

        // If it's a shorter path
        if (mDistance < distance) {
          System.out.println("Updatung distance!");
          distance = mDistance;
          prevNodeId.set(node.prevNodeId.get());
        }

        // If contains edges
        if (node.edges.get().length > 0) {
          edges = node.edges.get();
        }
      }

      PDNodeWritable node = new PDNodeWritable(key, new IntWritable(distance), prevNodeId);
      node.setEdges(edges);

      context.write(key, node); // Serialize the node
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
    String tempPath = "tmp-";
    String src = args[2];
    int iterations = Integer.parseInt(args[3]);

    System.out.println("--------------------Running PDPreProcess--------------------");
    ToolRunner.run(new PDPreProcess(), new String[]{inputPath, tempPath + 0, src});
    for (int i = 0; i < iterations; i++) {
      System.out
          .println("--------------------Running ParallelDijkstra (" + i + ")--------------------");
      ToolRunner.run(new ParallelDijkstra(), new String[]{tempPath + i, tempPath + (i + 1)});
    }
    System.out.println("--------------------Running PDPostProcess--------------------");
    ToolRunner.run(new PDPostProcess(), new String[]{tempPath + iterations, outputPath});
    System.exit(0);
  }
}