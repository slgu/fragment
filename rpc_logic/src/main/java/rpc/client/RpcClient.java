package rpc.client;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import com.sun.corba.se.impl.protocol.INSServerRequestDispatcher;
import db.DBCon;
/**
 * Created by slgu1 on 9/12/15.
 */
public class RpcClient {
    public RpcClient() throws Exception{
        dbconn = new DBCon("localhost","3307","root","kobe31413");
        if(!dbconn.connect()) {
            throw new Exception("init db conn error");
        }
    }
    public boolean _import(String typename, String instance){
        try {
            ResultSet res_set = dbconn.query(typename, instance);
            if (res_set.next()) {
                String channel_name = res_set.getString(DBCon.CHANNEL_FIELD_NAME);
                //insert into local symbol table
                rpc_bind.put(typename + ":" + instance, channel_name);
                return true;
            } else {
                return false;
            }
        }
        catch (SQLException e){
            System.out.println("sql error import fail");
            e.printStackTrace();
            return false;
        }
    }

    //broadcast typename is not included
    public void call(String typename, String instance, byte [] bytes){
        String key = typename + ":" + instance;
        String channel_name = rpc_bind.getOrDefault(key, null);
        if(channel_name != null){
            //call
            byte [] request_data = makeinput(typename, instance, bytes);
            if(request_data == null){
                System.out.println("make input error");
                return;
            }
            request(channel_name, request_data);
        }
        else{
            System.out.println("method not found");
        }
    }

    private static byte[] makeinput(String type, String instance, byte[] bytes){
        DataOutputStream output = new DataOutputStream(new ByteArrayOutputStream());
        try {
            output.writeInt(type.length());
            output.write(type.getBytes());
            output.writeInt(instance.length());
            output.write(instance.getBytes());
            output.write(bytes);
            return output.toString().getBytes();
        }
        catch (IOException e){
            return null;
        }
    }
    public static void main(String [] args) {

    }

    private boolean request(){

    }
    //bind rpc call channel
    private HashMap <String, String> rpc_bind = new HashMap<String,String>();
    private DBCon dbconn;
}
