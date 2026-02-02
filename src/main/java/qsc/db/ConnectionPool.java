package qsc.db;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.duckdb.DuckDBConnection;

public class ConnectionPool {

    private final BlockingQueue<Connection> connectionPool;
    private Connection primary_con;

    public ConnectionPool(String db_info, String db_type, int poolSize, int id, boolean read_only, boolean recover) throws Exception {
        if(db_type.equals("duckdb")){
            File dir = new File(db_info);
            if (!dir.exists()) {
                if (!dir.mkdirs()) {
                    throw new IOException("无法创建目录: " + db_info);
                }
            } else if (!dir.isDirectory()) {
                throw new IOException("目标路径不是目录: " + db_info);
            }
            if(!recover){
                File[] files = dir.listFiles();
                if (files != null&&!recover) {
                    for (File f : files) {
                        if (f.isFile()&&f.getName().startsWith("db"+id)) {
                            boolean deleted = f.delete();
                            if (!deleted) {
                                System.err.println("警告: 删除db文件失败 -> " + f.getAbsolutePath());
                            }
                        }
                    }
                }
            }
        }

        connectionPool = new LinkedBlockingQueue<>(poolSize);

        String connUrl = null;
        primary_con = null;
        if (db_type.equals("duckdb")) {
            connUrl = "jdbc:duckdb:"  + db_info + "db" + id;
            Class.forName("org.duckdb.DuckDBDriver");

            Properties Property = new Properties();

            Property.setProperty("memory_limit", "32GB");
            primary_con = DriverManager.getConnection(connUrl, Property);
        } else if (db_type.equals("pg")) {
            Class.forName("org.postgresql.Driver");
            connUrl = "jdbc:postgresql://" + db_info.split("/")[0]+"/postgres";
            try{
                if(!recover){
                    Connection global_conn = DriverManager.getConnection(connUrl);
                    Statement stmt = global_conn.createStatement();
                    stmt.executeUpdate("DROP DATABASE IF EXISTS "+ db_info.split("/")[1]+"_"+id+";");
                    stmt.executeUpdate("CREATE DATABASE "+ db_info.split("/")[1]+"_"+id+";");
                    stmt.close();
                    global_conn.close();
                }
                primary_con = DriverManager.getConnection("jdbc:postgresql://" + db_info+"_"+id);
            }catch(Exception e){
                e.printStackTrace();
            }
        } else {
            throw new SQLException("Unsupported db type: " + db_type);
        }

        connectionPool.add(primary_con);

        if(read_only){
            System.out.println("read_only=true，创建额外连接，poolSize: " + poolSize);
            if(db_type.equals("duckdb")){
                DuckDBConnection conn = (DuckDBConnection) primary_con;
                for (int i = 0; i < poolSize - 1; i++) {
                    Connection conn_ =  conn.duplicate();
                    connectionPool.add(conn_);
                }
            }else if(db_type.equals("pg")){
                for (int i = 0; i < poolSize - 1; i++) {
                    Connection conn = DriverManager.getConnection(connUrl);
                    connectionPool.add(conn);
                }
            }else{
                throw new SQLException("Unsupported db type: " + db_type);
            }
        } 

    }

    public Connection getPriConn() {
        return primary_con;
    }

    public Connection getConnection() throws InterruptedException {

        Connection conn = connectionPool.poll(3, TimeUnit.SECONDS);
        if (conn == null) {

            if (primary_con != null && !connectionPool.contains(primary_con)) {
                return primary_con;
            }

        }

        return conn;
    }

    public void releaseConnection(Connection conn) {
        connectionPool.offer(conn);
    }

    public void closeAllConnections() throws SQLException {
        primary_con.close();
        for (Connection conn : connectionPool) {
            conn.close();
        }
    }
}