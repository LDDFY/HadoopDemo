package com.hadoop.mapreduce.demo01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.*;

import java.io.IOException;

/**
 * Created by LDDFY on 2017/1/2.
 */
public class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer st = new StringTokenizer(line);
        while (st.hasMoreTokens()) {
            String world=st.nextToken();
            context.write(new Text(world),new IntWritable(1));
        }

    }
}
