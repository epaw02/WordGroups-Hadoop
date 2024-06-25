import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordGroups extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordGroups(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        String temporaryPath = conf.get("tmpPath");
        Path tmpPath = new Path(temporaryPath);
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Word Count");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);


        jobA.setMapperClass(WordCountMapper.class);
        jobA.setReducerClass(WordCountReducer.class);
	    jobA.setNumReduceTasks(2);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(WordGroups.class);
        boolean result = jobA.waitForCompletion(true);
        
	if(result) {
          Job jobB = Job.getInstance(conf, "Word Groups");
          jobB.setOutputKeyClass(Text.class);
          jobB.setOutputValueClass(Text.class);

          jobB.setMapOutputKeyClass(Text.class);
          jobB.setMapOutputValueClass(Text.class);

          jobB.setMapperClass(WordGroupMapper.class);
          jobB.setReducerClass(WordGroupReducer.class);
          jobB.setNumReduceTasks(1);

          FileInputFormat.setInputPaths(jobB, tmpPath);
          FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

          jobB.setJarByClass(WordGroups.class);
	  result = jobB.waitForCompletion(true);
        }
       return result ? 0 : 1;
    }
    

    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        Set<String> stopWords = new HashSet<String>(Arrays.asList("the", "a", "an", "and", "of", "to", "in", "am", "is", "are", "at", "not"));
        List<String> stopWordsList = new ArrayList<String>(stopWords);
        String wordDelimiters = " \t,;.?!-:@[](){}_*/'";

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
    	    StringTokenizer tokenizer = new StringTokenizer(line, wordDelimiters);
    	    while (tokenizer.hasMoreTokens()) {
        	    String nextToken= tokenizer.nextToken();

        	    if (!stopWordsList.contains(nextToken.trim().toLowerCase())) {
                    nextToken=nextToken.toLowerCase();
                    context.write(new Text(nextToken), new IntWritable(1));
        	    }
    	    }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
         int sum=0;
            for (IntWritable val: values) {
        	sum += val.get();
    	    }
    	    context.write(key, new IntWritable(sum));
        }
    }

    public static class WordGroupMapper extends Mapper<Object, Text, Text, Text> {
        private Text outValue= new Text();
        private Text outKey= new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\s+");

            String word=fields[0];
            char charArray[] = word.toCharArray();
            Arrays.sort(charArray);
            String sortedString= new String(charArray);

            outKey.set(sortedString);
            int number=Integer.parseInt(fields[1]);
            
            outValue.set(String.format("%s,%d", word, number));
            context.write(outKey, outValue);
        }
    }

    public static class WordGroupReducer extends Reducer<Text, Text, Text, Text> {
        private Text outValue= new Text();
        private Text outKey= new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            TreeSet<String> uniqueWords=new TreeSet<String>();
            int total=0;

            for (Text val: values) {
                String[] fields = val.toString().split(",");
                sum += Integer.parseInt(fields[1]);
                uniqueWords.add(fields[0]);
                total++;
    	    }

            String uniqueWordsString = String.join(" ", uniqueWords);
            
            outKey.set(String.format("%d, %d", total, sum));
            outValue.set(uniqueWordsString);
    	    context.write(outKey, outValue);
        }
    }
}