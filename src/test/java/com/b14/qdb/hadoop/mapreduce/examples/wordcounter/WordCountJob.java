package com.b14.qdb.hadoop.mapreduce.examples.wordcounter;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

public class WordCountJob extends Configured implements Tool {

    public int run(String[] arg0) throws Exception {
        return 0;
    }
    
    public static class TokenCounterMapper extends Mapper<Text, Chapter, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        
        public void map(Text key, Chapter value, Context context) throws IOException, InterruptedException {
            String[] splits = new String[] {};
            if (value != null && value.getText() != null) {
                splits = value.getText().split("[\\s\\.,\\?!;:\"]");
            }
            
            for (String s : splits) {
                word.set(s.trim().toLowerCase());
                context.write(word, one);
            }
        }
    }
    
    public static class TokenCounterReducer extends Reducer<Text, IntWritable, Text, WordCountResult> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }

            String qdbKey = key.toString();
            if (sum > 1 && !"".equals(qdbKey)) { 
                // drop any words that only show up once
                context.write(key, new WordCountResult(qdbKey, sum));
            }
        }
    }

}
