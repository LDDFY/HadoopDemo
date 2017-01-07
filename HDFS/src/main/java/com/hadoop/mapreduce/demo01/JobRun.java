package com.hadoop.mapreduce.demo01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class JobRun {
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
            conf.set("mapred.jar","D:\\WorkSpaces\\InteliJ IDE\\HadoopDemo\\HDFS\\out\\artifacts\\WcCount\\WcCount.jar");
            Job job = Job.getInstance(conf);
            job.setJarByClass(JobRun.class);
            job.setJobName("Word Count");
            job.setMapperClass(WcMapper.class);
            job.setReducerClass(WcReduce.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setNumReduceTasks(1);
            //输入文件地址
            FileInputFormat.addInputPath(job, new Path("/input/"));
            //输出文件地址
            FileOutputFormat.setOutputPath(job, new Path("/output/wc/"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
            System.out.println("运行完成！");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
