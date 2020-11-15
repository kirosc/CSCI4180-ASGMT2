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

public class PDPreProcess extends Configured implements Tool {

  public static class TokenizerMapper
      extends Mapper<Object, Text, IntWritable, Text> {

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString());
      String fromNodeId, toNodeId, edge;

      while (itr.hasMoreTokens()) {
        fromNodeId = itr.nextToken();
        toNodeId = itr.nextToken();
        edge = itr.nextToken();
        context.write(
            new IntWritable(Integer.parseInt(fromNodeId)),
            new Text(toNodeId + " " + edge)
        );
      }
    }
  }

  public static class EdgeReducer
      extends Reducer<IntWritable, Text, IntWritable, PDNodeWritable> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      Configuration conf = context.getConfiguration();
      int srcId = Integer.parseInt(conf.get("src"));
      int distance = srcId == key.get() ? 0 : Integer.MAX_VALUE;
      int prevNodeId = srcId == key.get() ? srcId : -1;
      PDNodeWritable node = new PDNodeWritable(key, new IntWritable(distance), new IntWritable(prevNodeId)
      );
      ArrayList<IntWritable[]> edges = new ArrayList<>();

      for (Text val : values) {
        String[] str = val.toString().split(" ", 2);
        int toNodeId = Integer.parseInt(str[0]);
        int weight = Integer.parseInt(str[1]);
        // Adjacency list
        edges.add(new IntWritable[]{
            new IntWritable(toNodeId),
            new IntWritable(weight)
        });
      }
      node.setEdges(edges.toArray(new IntWritable[edges.size()][]));

      context.write(key, node); // Serialize the node
    }
  }

  @Override
  public int run(String[] strings) throws Exception {
    Path inputPath = new Path(strings[0]);
    Path outputPath = new Path(strings[1]);
    String srcNodeId = strings[2];
    Configuration conf = new Configuration();
    conf.set("src", srcNodeId);
    Job job = Job.getInstance(conf, "PreProcess");

    job.setJarByClass(PDPreProcess.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(EdgeReducer.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PDNodeWritable.class);
    // Writes binary files suitable for reading into subsequent MapReduce jobs
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    FileInputFormat.addInputPath(job, inputPath);
    SequenceFileOutputFormat.setOutputPath(job, outputPath);

    job.waitForCompletion(true);
    return 0;
  }
}