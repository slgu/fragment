package job;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import run.MainRun;

import java.io.IOException;

/**
 * Created by hadoop on 9/30/15.
 */
public class MatrixMap extends Mapper {
    private int n = 10000;
    private int m = 10000;
    private int p = 10000;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        /*file section*/
        FileSplit split = (FileSplit)context.getInputSplit();
        flag = split.getPath().getName();
    }

    @Override
    protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
        String [] tokens = MainRun.DELIMETER.split(value.toString());
        if (flag.equals("csv1")) {
            //p
            for (int i = 0; i < p; ++i) {
                Text k = new Text(tokens[0] + "," + i);
                for (int j = 1; j <= tokens.length; ++j) {
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
