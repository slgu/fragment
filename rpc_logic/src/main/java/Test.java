import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOError;
import java.io.IOException;

/**
 * Created by slgu1 on 9/14/15.
 */
public class Test {
    public static void test_byteoutputstream() throws IOException{
        ByteArrayOutputStream byte_output = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(byte_output);
        output.writeInt(3);
        output.writeInt(4);
        output.writeInt(4);
        byte [] res = byte_output.toByteArray();
        System.out.println(res.length);
        for(byte a: res){
            System.out.print(a);
            System.out.print(" ");
        }
        System.out.println("");
    }
    public static void main(String [] args) throws IOException{
    }
}
