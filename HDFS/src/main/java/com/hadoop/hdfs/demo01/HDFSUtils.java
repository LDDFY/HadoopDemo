package com.hadoop.hdfs.demo01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;


public class HDFSUtils {

    static Configuration conf = new Configuration();
    static FileSystem hdfs;

    static {
        try {

            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://master:9000/");
            Path path = new Path("hdfs://master:9000/");
            System.setProperty("hadoop.home.dir", "D:\\DevelopSoftware\\hadoop");
            hdfs = FileSystem.get(path.toUri(), conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createDir(String dir) throws IOException {
        Path path = new Path(dir);
        if (hdfs.exists(path)) {
            System.out.println("dir \t" + conf.get("fs.defaultFS") + dir + "\t already exists");
            return;
        }
        hdfs.mkdirs(path);
        System.out.println("new dir \t" + conf.get("fs.defaultFS") + dir);
    }

    public static void main(String[] args) {
        HDFSUtils ofs = new HDFSUtils();
        try {
            ofs.createDir("/test12");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
