package rpc.client;

/**
 * Created by slgu1 on 9/12/15.
 */
import data.Addressbook;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class RpcClientStub {
    public static byte[] personencode(Addressbook.Person person){
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            person.writeTo(output);
            return output.toByteArray();
        }
        catch (IOException e){
            return null;
        }
    }
}
