package rpc.server;
/**
 * Created by slgu1 on 9/12/15.
 */
import data.Addressbook;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

import db.DBCon;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.AMQP.BasicProperties;

public class RpcServer {

    RpcServer(String channelname) throws Exception{
        this.channelname = channelname;
        dbconn = new DBCon("localhost","3307","root","******");
        if(!dbconn.connect()) {
            throw new Exception("init db conn error");
        }
    }

    public boolean _export(String type, String instance, CalleeFunction invokefunc){
        if(type.contains(":") || instance.contains(":")){
            return false; //a rule don't has ":"
        }

        //insert into dispatcher
        dispatcher.put(type + ":" + instance, invokefunc);

        //store in data center
        try{
            dbconn.store(type,instance,this.channelname);
        }
        catch (SQLException e){
            e.printStackTrace();
            System.out.println("db store error");
            return false;
        }
        return true;
    }

    //loop to deal with rpc request
    public void work() throws IOException, TimeoutException, InterruptedException{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(this.channelname, false, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
        channel.basicQos(3);//set process number
        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(this.channelname, false, consumer);
        //loop to get next request
        while(true){
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            BasicProperties props = delivery.getProperties();
            BasicProperties replyProps = new BasicProperties
                    .Builder()
                    .correlationId(props.getCorrelationId())
                    .build();

            byte [] body = delivery.getBody();
            //rpcruntime to inteprete head of rpc request
            //get type instance
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(body));
            int type_length = input.readInt();
            System.out.println(type_length);
            byte[] type_byte_arr = new byte[type_length];
            input.read(type_byte_arr);
            String type_name = new String(type_byte_arr, "UTF-8");

            //get instance
            int instance_length = input.readInt();
            System.out.println(instance_length);
            byte[] instance_byte_arr = new byte[instance_length];
            input.read(instance_byte_arr);
            String instance_name = new String(instance_byte_arr, "UTF-8");

            //get encoded parameter
            int available_extra = input.available();
            byte[] encoded_parameters = new byte[available_extra];
            input.read(encoded_parameters);

            System.out.println(type_name);
            System.out.println(instance_name);
            //need to dispatch
            CalleeFunction invoker = dispatch(type_name, instance_name);
            String response;
            if(invoker == null){
                response = "not found invoke";
            }
            else {
                //get response
                response = invoker.dealRequest(encoded_parameters);
            }
            channel.basicPublish("", props.getReplyTo(), replyProps, response.getBytes());
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        }
    }

    public static void main(String [] args){
        CalleePersonDebug persondebug = new CalleePersonDebug();
        RpcServer server = null;
        try{
            server = new RpcServer("testrpcserver");
        }
        catch (Exception e){
            System.out.println("init server error");
            e.printStackTrace();
            return;
        }
        //export an instance
        server._export("test", "debug", persondebug);

        //work
        try {
            server.work();
        }
        catch (Exception e){
            System.out.println("work error");
            e.printStackTrace();
        }
    }

    private String channelname = null;
    private CalleeFunction dispatch(String type, String instance){
        String dispatch_key = type + ":" + instance;
        return dispatcher.getOrDefault(dispatch_key,null);
    }

    //inner map dispatch
    private HashMap <String, CalleeFunction> dispatcher = new HashMap <String, CalleeFunction>();
    private DBCon dbconn;
    private Channel channel;
}
