package db;

/**
 * Created by slgu1 on 9/13/15.
 */
import com.mysql.jdbc.jdbc2.optional.SuspendableXAConnection;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.beans.Statement;
import java.io.IOException;
import java.sql.*;


//specialize for this data center
public class DBCon {

    public DBCon(String ip, String port, String username, String passwd) {
        this.ip = ip;
        this.passwd = passwd;
        this.port = port;
        this.username = username;
    }
    public boolean connect(){
        try {
            conn = null;
            Class.forName(DBCon.JDBC_DRIVER).newInstance(); //MYSQL驱动
            conn = DriverManager.getConnection(String.format("jdbc:mysql://%s:%s/%s", ip,port,DBCon.DATABASE_NAME),username, passwd); //链接本地MYSQL
            return true;
        } catch (Exception e) {
            System.out.print("MYSQL ERROR:" + e.getMessage());
            return false;
        }
    }

    public ResultSet query(String type) throws SQLException{
        PreparedStatement pstmt = conn.prepareStatement(String.format(
                "select * from %s where %s = ?", DBCon.TABLE_NAME, DBCon.TYPE_FIELD_NAME));
        pstmt.setString(1, type);
        return pstmt.executeQuery();
    }

    public ResultSet query(String type, String instance) throws SQLException{
        if(instance == null)
            return query(type);

        PreparedStatement pstmt = conn.prepareStatement(String.format(
                "select * from %s where %s = ? and %s = ?",
                DBCon.TABLE_NAME, DBCon.TYPE_FIELD_NAME, DBCon.INSTANCE_FIELD_NAME
        ));
        pstmt.setString(1, type);
        pstmt.setString(2, instance);
        return pstmt.executeQuery();
    }

    public void store(String type, String instance, String channelname) throws SQLException{
        System.out.println(type);
        System.out.println(instance);
        System.out.println(channelname);
        PreparedStatement pstmt = conn.prepareStatement(String.format(
                "insert into %s values (?,?,?)", DBCon.TABLE_NAME
        ));
        pstmt.setString(1, type);
        pstmt.setString(2, instance);
        pstmt.setString(3, channelname);
        pstmt.executeUpdate();
    }

    public void close(){
        try {
            if (!conn.isClosed()) {
                conn.close();
            }
        }
        catch (SQLException e){
            e.printStackTrace();
        }
    }
    public static void main(String [] args){
        DBCon conn = new DBCon("localhost","3307","root","kobe31413");
        conn.connect();
        try{
            ResultSet res_set = conn.query("hehe");
            while (res_set.next()){
                System.out.println(res_set.getString(DBCon.CHANNEL_FIELD_NAME));
            }
        }
        catch (SQLException e){
            System.out.println("execute sql fail");
        }
    }

    private Connection conn;
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private String ip;
    private String port;
    private String username;
    private String passwd;
    public static final String TABLE_NAME = "rpc";
    public static final String TYPE_FIELD_NAME = "type";
    public static final String INSTANCE_FIELD_NAME = "instance";
    public static final String CHANNEL_FIELD_NAME = "channel";
    public static final String DATABASE_NAME = "test";
}
