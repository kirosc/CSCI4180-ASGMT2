import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

public class PRPreProcess extends Configured implements Tool {

  public enum NodeCounter {COUNT}

  public static class TokenizerMapper
      extends Mapper<Object, Text, IntWritable, IntWritable> {

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString());

      while (itr.hasMoreTokens()) {
        int fromNodeId = Integer.parseInt(itr.nextToken());
        int toNodeId = Integer.parseInt(itr.nextToken());
        itr.nextToken(); // Skip weight
        context.write(
            new IntWritable(fromNodeId),
            new IntWritable(toNodeId)
        );
      }
    }
  }

  public static class EdgeReducer
      extends Reducer<IntWritable, IntWritable, IntWritable, PRNodeWritable> {

    // Handle isolated nodes and count total node
    private static final Set<Integer> seenNode = new HashSet<>();
    private static final ArrayList<PRNodeWritable> nodes = new ArrayList<>();

    public void reduce(IntWritable key, Iterable<IntWritable> toNodes, Context context) {

      PRNodeWritable node = new PRNodeWritable(key.get());
      ArrayList<IntWritable> edges = new ArrayList<>();

      // Adjacency list
      for (IntWritable toNodeId : toNodes) {
        int id = toNodeId.get();
        edges.add(new IntWritable(id));
      }
      node.setEdges(edges.toArray(new IntWritable[0]));

      seenNode.add(key.get());
      nodes.add(node);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      ArrayList<PRNodeWritable> danglingNodes = new ArrayList<>();
      for (PRNodeWritable node : nodes) {
        for (Writable edge : node.edges.get()) {
          int id = ((IntWritable) edge).get();
          if (!seenNode.contains(id)) {
            // Node without outgoing edge
            seenNode.add(id);
            danglingNodes.add(new PRNodeWritable(id));
          }
        }
      }
      nodes.addAll(danglingNodes);

      for (PRNodeWritable node : nodes) {
        node.rank.set(1.0 / seenNode.size());
        context.write(node.id, node); // Serialize the node
      }
    }
  }

  @Override
  public int run(String[] strings) throws Exception {
    Path inputPath = new Path(strings[0]);
    Path outputPath = new Path(strings[1]);
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "PRPreProcess");

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
    return (int) job.getCounters().findCounter(NodeCounter.COUNT).getValue();
  }
}