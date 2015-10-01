package job;

import hdfs.HdfsDao;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import run.MainRun;

import javax.security.auth.callback.TextOutputCallback;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Created by hadoop on 9/30/15.
 */
public class MatrixMul {
    public static void run(Map <String, String> path) throws Exception{
        String origin1 = path.get("m1");
        String origin2 = path.get("m2");
        String input1 = path.get("input1");
        String input2 = path.get("input2");
        String output = path.get("output");

        HdfsDao hdfs = new HdfsDao();
        //copy from local to hdfs
        hdfs.copyFromLocal(origin1, input1);
        hdfs.copyFromLocal(origin2, input2);

        //set job
        Job job = Job.getInstance(new Configuration(), "matrixmul");
        job.setJarByClass(MatrixMul.class);

        job.setMapperClass(MatrixMap.class);
        job.setReducerClass(MatrixReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);

        //set input output
        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }
}
