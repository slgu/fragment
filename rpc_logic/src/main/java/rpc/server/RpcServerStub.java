package rpc.server;

/**
 * Created by slgu1 on 9/12/15.
 */
import data.Addressbook;
import java.io.ByteArrayInputStream;
import java.io.IOException;

public class RpcServerStub {
    static Addressbook.Person persondecode(byte[] bytes){
        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        Addressbook.Person person = null;
        try {
            person = Addressbook.Person.parseFrom(input);
        }
        finally {
            return person;
        }
    }
}
