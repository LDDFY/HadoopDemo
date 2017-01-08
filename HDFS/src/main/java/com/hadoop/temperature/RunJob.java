package com.hadoop.temperature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by LDDFY on 2017/1/7.
 */
public class RunJob {
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    static class HotMapper extends Mapper<LongWritable, Text, KeyPair, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            System.out.println("----------------->"+line);
            String[] strs = line.split("\t");
            if (strs.length == 2) {
                try {
                    Date date = sdf.parse(strs[0]);
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTime(date);
                    int year = calendar.get(1);
                    String hot = strs[1].substring(0, strs[1].indexOf("℃"));
                    KeyPair keyPair = new KeyPair(year, Integer.parseInt(hot));
                    context.write(keyPair, value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static class HotReduce extends Reducer<KeyPair, Text, KeyPair, Text> {

        @Override
        protected void reduce(KeyPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text t : values) {
                context.write(key, t);
            }
        }
    }

    public static void main(String[] args) {
        try {
            System.setProperty("hadoop.home.dir", "D:\\DevelopSoftware\\hadoop");
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://master:9000/");
            conf.set("mapred.remote.os", "Linux");
            conf.set("mapreduce.framework.name", "yarn");
            conf.set("mapreduce.app-submission.cross-platform", "true");
            conf.set("yarn.resourcemanager.address", "master:18040");
            conf.set("yarn.resourcemanager.scheduler.address", "master:18030");
            conf.set("yarn.resourcemanager.resource-tracker.address", "master:18025");
            conf.set("yarn.resourcemanager.admin.address", "master:18141");
            conf.set("yarn.resourcemanager.webapp.address", "master:18088");
            //设置远程提交方式将本地jar包提交到hadoop上
            conf.set("mapred.jar", "D:\\WorkSpaces\\InteliJ IDE\\HadoopDemo\\HDFS\\out\\artifacts\\Hot\\HDFS.jar");
            Job job = Job.getInstance(conf);
            job.setJarByClass(RunJob.class);

            job.setJobName("Hot Split");
            //设置map类
            job.setMapperClass(HotMapper.class);

            //设置reduce类
            job.setReducerClass(HotReduce.class);

            //设置map输出Key类型
            job.setMapOutputKeyClass(KeyPair.class);

            //设置map输出Value类型
            job.setMapOutputValueClass(Text.class);

            //设置reduce个数
            job.setNumReduceTasks(3);
            //设置自定义partition
            job.setPartitionerClass(FirstPartion.class);
            //设置自定义排序
            job.setSortComparatorClass(SortHot.class);
            //设置自定义分组
            job.setGroupingComparatorClass(GroupHot.class);

            //输入文件地址
            FileInputFormat.addInputPath(job, new Path("/input/hot/"));
            //输出文件地址
            FileOutputFormat.setOutputPath(job, new Path("/output/hot/"));
            //设置退出
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
