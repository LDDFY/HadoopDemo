package com.hadoop.hdfs.demo01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;


public class HDFSUtils {

    private static Configuration conf = new Configuration();
    private static Path hdfsPath = null;

    static {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000/");
        System.setProperty("hadoop.home.dir", "D:\\DevelopSoftware\\hadoop");
        hdfsPath = new Path("hdfs://master:9000/");
    }

    /**
     * 创建新的文件夹
     *
     * @param dir 目录名称
     */
    public void createDir(String dir) {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(hdfsPath.toUri(), conf);
            Path path = new Path(dir);
            if (fs.exists(path)) {
                System.out.println("dir \t" + conf.get("fs.defaultFS") + dir + "\t already exists");
                return;
            }
            fs.mkdirs(path);
            System.out.println("new dir \t" + conf.get("fs.defaultFS") + dir);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 删除文件夹
     *
     * @param dir 文件夹路径
     */
    public void deleteDir(String dir) {
        FileSystem fs = null;
        try {
            Path path = new Path(dir);
            fs = FileSystem.get(hdfsPath.toUri(), conf);
            if (fs.exists(path)) {
                fs.delete(path, true);
                System.out.println("dir \t" + conf.get("fs.defaultFS") + dir + "\t delete success");
            } else {
                System.out.println("dir \t" + conf.get("fs.defaultFS") + dir + "\t is not exists");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 上传文件到hdfs
     *
     * @param src  本地文件绝对路径
     * @param path hdfs文件路径
     */
    public void copyFromLocal(String src, String path) {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(hdfsPath.toUri(), conf);
            fs.copyFromLocalFile(new Path(src), new Path(path));
            System.out.println("上传文件成功！");
        } catch (IOException e) {
            System.out.println("文件上传失败！");
            e.printStackTrace();
        } finally {
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 拷贝hdfs文件到本地
     *
     * @param src  本地地址
     * @param path hdfs文件地址
     */
    public void copyToLocal(String src, String path) {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(hdfsPath.toUri(), conf);
            fs.copyToLocalFile(false, new Path(src), new Path(path));
            System.out.println("文件下载成功！");
        } catch (IOException e) {
            System.out.println("文件下载失败！");
            e.printStackTrace();
        } finally {
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        HDFSUtils ofs = new HDFSUtils();
//        ofs.createDir("/temp");
//        ofs.copyFromLocal("D:\\WorkSpaces\\InteliJ IDE\\HadoopDemo\\HDFS\\src\\main\\resources\\CHANGES.txt", "hdfs://master:9000/temp/");
//        ofs.copyToLocal("hdfs://master:9000/temp/CHANGES.txt", "D:\\\\WorkSpaces\\\\InteliJ IDE\\\\HadoopDemo\\\\HDFS\\\\target\\\\");
        ofs.deleteDir("/output/wc/");
    }

}
