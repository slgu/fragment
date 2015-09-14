package rpc.client;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import com.google.protobuf.Descriptors;
import com.mysql.jdbc.jdbc2.optional.SuspendableXAConnection;
import com.rabbitmq.client.*;
import data.Addressbook;
import db.DBCon;

import com.rabbitmq.client.AMQP.BasicProperties;
/**
 * Created by slgu1 on 9/12/15.
 */
public class RpcClient {
    public RpcClient() throws Exception{
        dbconn = new DBCon("localhost","3307","root","kobe31413");
        if(!dbconn.connect()) {
            throw new Exception("init db conn error");
        }
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        this.connection = factory.newConnection();
        this.channel = connection.createChannel();

        this.replyQueueName = channel.queueDeclare().getQueue();
        this.consumer = new QueueingConsumer(channel);
        channel.basicConsume(replyQueueName, true, consumer);
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
    public String call(String typename, String instance, byte [] bytes){
        String key = typename + ":" + instance;
        String channel_name = rpc_bind.getOrDefault(key, null);
        if(channel_name != null){
            //call
            byte [] request_data = makeinput(typename, instance, bytes);
            if(request_data == null){
                System.out.println("make input error");
                return null;
            }
            return request(channel_name, request_data);
        }
        else{
            System.out.println("method not found");
            return null;
        }
    }

    private static byte[] makeinput(String type, String instance, byte[] bytes){
        ByteArrayOutputStream byteoutput = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(byteoutput);
        try {
            output.writeInt(type.length());
            output.write(type.getBytes());
            output.writeInt(instance.length());
            output.write(instance.getBytes());
            output.write(bytes);
            byte [] res = byteoutput.toByteArray();
            return res;
        }
        catch (IOException e){
            return null;
        }
        finally {
            try {
                output.close();
            }
            catch (Exception e){

            }
        }
    }
    public static void main(String [] args) {
        RpcClient client = null;
        try {
            client = new RpcClient();
        }
        catch (Exception e){
            System.out.println("init client error");
            e.printStackTrace();
            return;
        }
        client._import("test", "debug");
        Addressbook.Person.Builder personbuilder = Addressbook.Person.newBuilder();
        personbuilder.setId(1);
        personbuilder.setEmail("blackhero98@gmail.com");
        personbuilder.setName("slgu");
        Addressbook.Person person = personbuilder.build();
        byte[] encodedata = RpcClientStub.personencode(person);
        if(encodedata == null){
            System.out.println("encode error");
            return;
        }
        String response = client.call("test", "debug", encodedata);
        System.out.println("rpc result:" + response);
    }

    private String request(String channelname, byte[] input_parameter){
        String response = null;
        String corrId = java.util.UUID.randomUUID().toString();

        BasicProperties props = new BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();

        try {
            channel.basicPublish("", channelname, props, input_parameter);
        }
        catch (IOException e){
            System.out.println("channel basicpublish error");
            e.printStackTrace();
            return null;
        }

        while (true) {
            QueueingConsumer.Delivery delivery = null;
            try {
                delivery = consumer.nextDelivery();
            }
            catch (InterruptedException e){
                System.out.println("nextdelivery error");
                e.printStackTrace();
                return null;
            }
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                response = new String(delivery.getBody());
                break;
            }
        }
        return response;
    }

    //bind rpc call channel
    private HashMap <String, String> rpc_bind = new HashMap<String,String>();
    private DBCon dbconn;
    private Connection connection;
    private Channel channel;
    private QueueingConsumer consumer;
    private String replyQueueName;
}
