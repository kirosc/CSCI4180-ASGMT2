import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ParallelDijkstra {

  public static class PDMapper
      extends Mapper<IntWritable, PDNodeWritable, IntWritable, PDNodeWritable> {

    @Override
    protected void map(IntWritable key, PDNodeWritable value, Context context)
        throws IOException, InterruptedException {

      context.write(key, value);
    }
  }

  public static class PDReducer
      extends Reducer<IntWritable, PDNodeWritable, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<PDNodeWritable> values, Context context)
        throws IOException, InterruptedException {

      for (PDNodeWritable val : values) {
        context.write(key, new Text(val.toString())); // Serialize the node
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);
    Path tempPath = new Path("tmp");

    JobControl jc = new JobControl("ParallelDijkstra");
    Configuration preProcessConf = PDPreProcess
        .getPreProcessConf(inputPath, tempPath);
    Configuration PDConf = getPDConf(tempPath, outputPath);
    ControlledJob preProcessJob = new ControlledJob(preProcessConf);
    ControlledJob PDJob = new ControlledJob(PDConf);

    jc.addJob(preProcessJob);
    jc.addJob(PDJob);
    PDJob.addDependingJob(preProcessJob);

    Thread runJobControl = new Thread(jc);
    runJobControl.start();
    while (!jc.allFinished()) {
    }
    System.out.println("Number of failed job: " + jc.getFailedJobList().size());
    System.exit(jc.allFinished() ? 0 : 1);
  }

  public static Configuration getPDConf(Path inputPath,
      Path outputPath) throws IOException {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "ParallelDijkstra");

    job.setJarByClass(ParallelDijkstra.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapperClass(PDMapper.class);
    job.setReducerClass(PDReducer.class);
    job.setMapOutputValueClass(PDNodeWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    SequenceFileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    return job.getConfiguration();
  }
}