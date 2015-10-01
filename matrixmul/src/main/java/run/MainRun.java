package run;

import job.MatrixMul;
import org.apache.hadoop.mapred.JobConf;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by hadoop on 9/30/15.
 */
public class MainRun {
    public static final String HDFSURL = "hdfs://localhost:9000";
    //csv ,
    public static final Pattern DELIMETER = Pattern.compile("[\t,]");
    public static void main(String[] args) {
        matrixmul();
    }
    //set config
    public static JobConf config() {
        JobConf conf = new JobConf(MainRun.class);
        conf.setJobName("MartrixMul");
        /*
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        */
        return conf;
    }
    private static void matrixmul() {

        Map <String, String> path = new HashMap<String, String>();
        //local path
        path.put("m1", "/home/hadoop/csv1");
        path.put("m2", "/home/hadoop/csv2");
        path.put("input1", HDFSURL + "/matrix/input1");
        path.put("input2", HDFSURL + "/matrix/input2");
        path.put("output", HDFSURL + "/matrix/output");
        try {
            MatrixMul.run(path);
        }
        catch (Exception e) {
            System.out.println(e.toString());
        }
    }
}
