import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MaxTemperature_chaining {

  public static void main(String[] args) throws Exception {
Configuration config = new Configuration();
 String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperature_chaining <input path> <output path>");
      System.exit(-1);
    }


    Job job = Job.getInstance();
    job.setJarByClass(MaxTemperature_chaining.class);
    job.setJobName("Max temperature Chaining for week-08");



    Configuration map1Conf = new Configuration(false);
ChainMapper.addMapper(job,
                      MaxTemperatureMapper.class,
                      LongWritable.class,
                      Text.class,
                      IntWritable.class,
                      Text.class,
                      map1Conf);
Configuration red2Conf = new Configuration(false);
ChainReducer.setReducer(job,
                      MaxTemperatureReducer2.class,
                      Text.class,
                      IntWritable.class,
                      Text.class,
                      IntWritable.class,
                      red2Conf);
job.setReducerClass(MaxTemperatureReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}