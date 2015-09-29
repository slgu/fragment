package org.slgu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.PropertyConfigurator;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.io.File;
import java.io.IOException;
import java.net.URI;

/**
 * Created by slgu1 on 9/29/15.
 */
public class HdfsDao {
    private static final String HDFS = "hdfs://10.211.55.3:9000/";
    private static final int CHUNKSIZE = 64 * 1024 * 1024;
    public HdfsDao() throws IOException{
        this(HDFS);
    }

    public HdfsDao(String hdfs) throws IOException{
        this.hdfsPath = hdfs;
        this.conf = new Configuration();
        this.fs = FileSystem.get(URI.create(hdfs), conf);
        fs.exists(new Path("/input"));
    }

    public FileStatus [] ls(String folder) throws IOException {
        Path path = new Path(folder);
        FileStatus [] list = fs.listStatus(path);
        return list;
    }

    public boolean mkdir(String folder) throws IOException {
        Path path = new Path(folder);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
            return true;
        }
        return false;
    }

    //hdfs路径
    private String hdfsPath;

    //Hadoop系统配置
    private Configuration conf;
    private FileSystem fs;

    //启动函数
    public static void main(String[] args) throws IOException {
        PropertyConfigurator.configure("log4j.properties");
        HdfsDao a = new HdfsDao();
        //System.out.println(a.mkdir("/input/hello"));
    }
}
