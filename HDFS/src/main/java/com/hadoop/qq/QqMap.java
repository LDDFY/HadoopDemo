package com.hadoop.qq;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by LDDFY on 2017/1/7.
 */
public class QqMap extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] ss=line.split("\t");
        context.write(new Text(ss[0]),new Text(ss[1]));
        context.write(new Text(ss[1]),new Text(ss[0]));
    }
}
