package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.PropertyConfigurator;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.io.File;
import java.io.IOException;
import java.net.URI;

/**
 * Created by slgu1 on 9/29/15.
 */
public class HdfsDao {
    private static final String HDFS = "hdfs://localhost:9000/";
    private static final int CHUNKSIZE = 64 * 1024 * 1024;
    public HdfsDao() throws IOException{
        this(HDFS);
    }

    public HdfsDao(String hdfs) throws IOException{
        this.hdfsPath = hdfs;
        JobConf conf = new JobConf(HdfsDao.class);
        conf.setJobName("HdfsDAO");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        this.conf = conf;
        this.fs = FileSystem.get(URI.create(hdfs), conf);
    }

    public FileStatus [] ls(String folder) throws IOException {
        Path path = new Path(folder);
        FileStatus [] list = fs.listStatus(path);
        return list;
    }
    public void copyFromLocal(String origin, String target) throws IOException {
        System.out.println(origin);
        System.out.println(target);
        fs.copyFromLocalFile(new Path(origin), new Path(target));
    }
    public boolean mkdir(String folder) throws IOException {
        Path path = new Path(folder);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
            return true;
        }
        return false;
    }

    //hdfs path
    private String hdfsPath;

    //Hadoop system configure
    private Configuration conf;
    private FileSystem fs;

    //test
    public static void main(String[] args) throws IOException {
        HdfsDao a = new HdfsDao();
        System.out.println(a.mkdir("/input/hello"));
    }
}