package job;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import run.MainRun;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by hadoop on 9/30/15.
 */
public class MatrixReduce extends Reducer{
    @Override
    protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {
        Map <String, String> mapA = new HashMap<String, String>();
        Map <String, String> mapB = new HashMap<String, String>();

        for (Object line: values) {
            String val = line.toString();
            if (val.startsWith("A:")) {
                String []kv = MainRun.DELIMETER.split(val.substring(2));
                mapA.put(kv[0], kv[1]);
            }
            else {
                String []kv = MainRun.DELIMETER.split(val.substring(2));
                mapB.put(kv[0], kv[1]);
            }
        }
        int result = 0;
        Iterator <String> itr = mapA.keySet().iterator();
        while (itr.hasNext()) {
            String k = itr.next();
            int a = Integer.parseInt(mapA.get(k));
            int b = Integer.parseInt(mapB.get(k));
            result += a * b;
        }
        context.write(key, String.valueOf(result));
    }
}
