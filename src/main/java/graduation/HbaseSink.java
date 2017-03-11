package graduation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by UrbanSy on 2017/1/13 11:11.
 */
public class HbaseSink implements Serializable {

    private static final Long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(HbaseSink.class);

    private Configuration configuration;
    private HConnection connection;
    private HTableInterface table;


    private HConnection getConnection() {
        if (connection == null || connection.isClosed()) {
            try {
                connection = HConnectionManager.createConnection(configuration);
            } catch (Exception e) {
                throw new RuntimeException("Failed to get connection to HBase", e);
            }
        }
        return connection;
    }

    public void closeConnection() {
        try {
            if (table != null) {
                table.close();
            }
            if (this.connection != null) {
                this.connection.close();
            }
        } catch (Exception e) {
        }
    }

    private HTableInterface getTable(String tableName) {
        if (table == null) {
            try {
                table = connection.getTable(Bytes.toBytes(tableName));
            } catch (Exception e) {
                throw new RuntimeException("Failed to get HBase table: " + tableName, e);
            }
        }
        return table;
    }

    public void init(String zkQuorum, String zkPort, String zkParent, String tableName) {

        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zkQuorum);
        configuration.set("hbase.zookeeper.property.clientPort", zkPort);
        configuration.set("zookeeper.znode.parent", zkParent);

        getConnection();
        getTable(tableName);
    }


    //批量查找数据
    public Result[] query(List<String> keys) throws IOException {

        List<Get> batch = new ArrayList<Get>();
        for (String key : keys) {
            Get get = new Get(Bytes.toBytes(key));
//            get.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(targetField));
            batch.add(get);
        }

        if (batch.size() > 0) {
            return table.get(batch);

        } else return null;

    }


    //插入数据
    public void insertPut(List<Put> puts) throws IOException {
/*
        int size = puts.size();
        int index = 0;

        while (index < size) {
            int from = index;
            int to = Math.min(from + 1000, size);
            table.put(puts.subList(from, to));
            index = to;
        }
*/
        table.put(puts);
    }

    public List<String> getSchema(String rowKey,String columnFamily ){

        List hbaseFileds = new ArrayList();
        Get get = new Get(Bytes.toBytes(rowKey));
        try {
            Result result = table.get(get);
            Set<byte[]> Fields = result.getNoVersionMap().get(Bytes.toBytes(columnFamily)).keySet();
            for(byte[] hbaseField : Fields){
                String hbaseFieldString = new String(hbaseField);
                System.out.println(hbaseFieldString);
                hbaseFileds.add(hbaseFieldString);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return  hbaseFileds;
    }

}
