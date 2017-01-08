package com.hadoop.temperature;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by LDDFY on 2017/1/7.
 */
public class FirstPartion  extends Partitioner<KeyPair,Text> {


    public int getPartition(KeyPair key, Text value, int numPartitions) {

        return (key.getYear()*127)%numPartitions;
    }
}
