import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class PDPreProcess {

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

      PDNodeWritable node = new PDNodeWritable(key);
      ArrayList<IntWritable[]> edges = new ArrayList<IntWritable[]>();

      for (Text val : values) {
        String[] str = val.toString().split(" ", 2);
        int toNodeId = Integer.parseInt(str[0]);
        int weight = Integer.parseInt(str[1]);
        edges.add(new IntWritable[]{
            new IntWritable(toNodeId),
            new IntWritable(weight)
        });
      }
      node.setEdges(edges.toArray(new IntWritable[edges.size()][]));

      context.write(key, node); // Serialize the node
    }
  }

  public static Configuration getPreProcessConf(Path inputPath,
      Path outputPath) throws IOException {
    Configuration conf = new Configuration();
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

    return job.getConfiguration();
  }
}