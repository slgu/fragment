package job;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.BasicConfigurator;
import run.MainRun;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by hadoop on 9/30/15.
 */
public class MatrixMap extends Mapper <LongWritable, Text, Text, Text> {
    private int n = 100;
    private int m = 100;
    private int p = 100;
    private static Logger logger = Logger.getLogger(MatrixMap.class);
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        System.out.println("set up MatrixMap");
        //file section
        FileSplit split = (FileSplit)context.getInputSplit();
        flag = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        BasicConfigurator.configure();
        String [] tokens = MainRun.DELIMETER.split(value.toString());
        if (flag.equals("csv1")) {
            //p
            for (int i = 0; i < p; ++i) {
                Text k = new Text(tokens[0] + "," + (i + 1));
                for (int j = 1; j < tokens.length; ++j) {
                    Text v = new Text("A:" + j + "," + tokens[j]);
                    context.write(k, v);
                }
            }
        }
        else {
            for (int i = 1; i <= n; ++i) {
                for (int j = 1; j <= p; ++j) {
                    Text k = new Text(i + "," + j);
                    Text v = new Text("B:" + tokens[0] + "," + tokens[j]);
                    context.write(k, v);
                }
            }
        }
    }
    private String flag;
}
