package com.hadoop.mapreduce.demo01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by LDDFY on 2017/1/2.
 */
public class WcReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
       int sum = 0;
       for (IntWritable i:values){
           sum+=i.get();
       }
       context.write(new Text(key),new IntWritable(sum));
    }
}
