import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


public class MyHadoopJob {

//  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
//    private final static IntWritable one = new IntWritable(1);
//
//    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
//        throws IOException {
//      String[] words = value.toString().toLowerCase().replaceAll("[^a-zA-Z\\s]+", "").split("\\s+");
//      for (String word: words) {
//        if (word.length() > 1) { // skip one character word like "a", "i" because they are not valid anagram
//          output.collect(new Text(getSortedForm(word)), new Text(word));
//        }
//      }
//    }
//  }
//
//  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
//    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,
//        Reporter reporter) throws IOException {
//      Set<String> set = new HashSet<>();
//      while (values.hasNext()) {
//        set.add(values.next().toString());
//      }
//
//      output.collect(key, new Text(String.join(",", set)));
//    }
//  }
//
//  public static class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
//    private final static IntWritable one = new IntWritable(1);
//
//    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
//        throws IOException {
//
//      String[] parts = value.toString().split("\\s+");
//      List<String> words = Arrays.asList(parts[1].split(","));
//      Set<String> set = new HashSet<>(words);
//
//      if (set.size() > 1) {
//        output.collect(new Text(parts[0]), new Text(String.join(",", set)));
//      }
//    }
//  }
//
//  public static class Reduce2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
//    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,
//        Reporter reporter) throws IOException {
//      while (values.hasNext()) {
//        output.collect(key, values.next());
//      }
//    }
//  }
//
//  private static String getSortedForm(String word) {
//    char[] chars = word.toCharArray();
//    Arrays.sort(chars);
//    return String.valueOf(chars);
//  }
//
//  public static void main(String[] args) throws Exception {
//    // First job
//    JobConf conf = new JobConf(MyHadoopJob.class);
//    conf.setJobName("anagram");
//    conf.set("mapreduce.job.split.metainfo.maxsize", "-1");
//
//    conf.setOutputKeyClass(Text.class);
//    conf.setOutputValueClass(Text.class);
//
//    conf.setMapperClass(Map.class);
//    conf.setCombinerClass(Reduce.class);
//    conf.setReducerClass(Reduce.class);
//
//    conf.setInputFormat(TextInputFormat.class);
//    conf.setOutputFormat(TextOutputFormat.class);
//
//    String outputPath = "/test/tmp-" + System.currentTimeMillis() + "/";
//    FileInputFormat.setInputPaths(conf, new Path(args[0]));
//    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
//
//    JobClient.runJob(conf);
//
//    // Second job
//    JobConf conf2 = new JobConf(MyHadoopJob.class);
//    conf2.setJobName("removeDuplicate");
//
//    conf2.setOutputKeyClass(Text.class);
//    conf2.setOutputValueClass(Text.class);
//
//    conf2.setMapperClass(Map2.class);
//    conf2.setCombinerClass(Reduce2.class);
//    conf2.setReducerClass(Reduce2.class);
//
//    conf2.setInputFormat(TextInputFormat.class);
//    conf2.setOutputFormat(TextOutputFormat.class);
//
//    String outputPath2 = "gs://bessie_cloud_bucket-1/hadoop/anagram-" + System.currentTimeMillis() + "/";
//    FileInputFormat.setInputPaths(conf2, new Path(outputPath)); // First job's output is second job's input
//    FileOutputFormat.setOutputPath(conf2, new Path("file:///Users/kejiang/Developer/MyHDFS/output/"));
//
//    JobClient.runJob(conf2);
//  }


  /**
   * Find all palindromes
   */
  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      String[] words = value.toString().toLowerCase().replaceAll("[^a-zA-Z\\s]+", "").split("\\s+");
      for (String word: words) {
        if (isPalindrome(word)) {
          output.collect(new Text(word), one);
        }
      }
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
        Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new IntWritable(sum));
    }
  }

  private static boolean isPalindrome(String word) {
    if (word.length() == 0) return false;
    if (word.length() == 1 && !word.equals("a") && !word.equals("i")) return false;

    int start = 0, end = word.length()-1;
    while (start < end) {
      if (word.charAt(start) != word.charAt(end)) return false;
      start++;
      end--;
    }
    return true;
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(MyHadoopJob.class);
    conf.setJobName("palindrome");
    conf.set("mapreduce.job.split.metainfo.maxsize", "-1");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    String outputPath = "file:///Users/kejiang/Developer/MyHDFS/hadoop/palindrome-" + System.currentTimeMillis() + "/";
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    JobClient.runJob(conf);
  }
}
