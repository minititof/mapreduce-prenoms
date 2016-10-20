import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class GenderPercentage{
    private static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            String line = text.toString();
            String[] lineSplit = line.split(";");
            String[] genderSplit = lineSplit[1].split(",");
            if (genderSplit.length == 1) {
                outputCollector.collect(new Text(genderSplit[0].startsWith("m") ? "male" : "female"), one);
                reporter.incrCounter("group", "counter", 1);
            }
        }
    }

    private static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            int sum = 0;
            while (iterator.hasNext()) {
                sum += iterator.next().get();
            }
            outputCollector.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        JobConf conf1 = new JobConf(GenderPercentage.class);
        conf1.setJobName("GenderPercentage");
        conf1.setOutputKeyClass(Text.class);
        conf1.setOutputValueClass(IntWritable.class);

        conf1.setMapperClass(Map.class);
        conf1.setCombinerClass(Reduce.class);
        conf1.setReducerClass(Reduce.class);

        conf1.setInputFormat(TextInputFormat.class);
        conf1.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf1, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf1, new Path(args[1]));

        JobClient.runJob(conf1);
    }
}