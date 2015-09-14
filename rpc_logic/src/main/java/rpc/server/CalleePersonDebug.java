package rpc.server;

import data.Addressbook;

import java.io.IOException;

/**
 * Created by slgu1 on 9/13/15.
 */
public class CalleePersonDebug implements CalleeFunction{
    @Override
    public String dealRequest(byte[] encodedata) throws IOException {
        Addressbook.Person person;
        person = RpcServerStub.persondecode(encodedata);
        StringBuilder res = new StringBuilder();
        res.append("name:");
        res.append(person.getName());
        res.append(",email:");
        res.append(person.getEmail());
        return res.toString();
    }
}
