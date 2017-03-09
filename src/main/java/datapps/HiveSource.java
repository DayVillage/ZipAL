package datapps;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.Serializable;
import java.util.*;

/**
 * Created by UrbanSy on 2017/1/16 21:24.
 */
public class HiveSource implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(HiveSource.class);

    public transient SparkContext sc;
    public transient SQLContext hiveContext;
    public transient SparkConf sparkConf;

    public String zkQuorum;
    public String zkPort;
    public String zkParent;

    public void init(String configFilePath, String hbaseTableName) {
        sparkConf = new SparkConf();
        sparkConf.setAppName("Zipper");
        sparkConf.setMaster("local");
        sc = new SparkContext(sparkConf);

//        sc = SparkContext.getOrCreate();

//        hiveContext = new HiveContext(sc);//实际用的是这句
        hiveContext = new HiveContext(sc);

        Properties properties = PropertiesUtil.loadFromFile(configFilePath);
        this.zkQuorum = properties.getProperty("hbase.zookeeper.quorum");
        this.zkPort = properties.getProperty("hbase.zookeeper.property.clientPort");
        this.zkParent = properties.getProperty("zookeeper.znode.parent");

    }

    public DataFrame readData(String sql) {
        System.out.println(sql);
        DataFrame dataFrame = hiveContext.sql("select * from ccp_corp_info_t limit 10");
        return dataFrame;
    }

    public void mapPartitions(DataFrame dataFrame, final String key, final String openZipTime, final String hbaseTableName, final String columnFamily, final String closeZipTime, final String startDateColumn, final String endDateColumn) {


        JavaRDD<Row> javaRDD = dataFrame.toJavaRDD();
        javaRDD.foreachPartition(new VoidFunction<Iterator<Row>>() {
            @Override
            public void call(Iterator<Row> rowIterator) throws Exception {

                HbaseSink hbaseSink = new HbaseSink();
                hbaseSink.init(zkQuorum, zkPort, zkParent, hbaseTableName);


                List<String> rowKeys = new ArrayList<String>();
                Row row = null;
                List<Put> batch = new ArrayList<Put>();
                String[] fields = null;
                while (rowIterator.hasNext()) {
                    row = rowIterator.next();

                    if (row.size() == 0) {
                        throw new RuntimeException("Hive row ");
                    }

                    int rowKeyIndex = row.fieldIndex(key);
                    String rowKey = ((String) row.get(rowKeyIndex)).concat("_" + openZipTime);
                    rowKeys.add(rowKey);

//                    System.out.println("rowKey : " + rowKey);

                    Put put = new Put(Bytes.toBytes(rowKey));
                    if (fields == null) {
                        fields = row.schema().fieldNames();
                    }
                    for (int i = 0; i < fields.length; i++) {

                        String key = fields[i];
                        String value = (String) row.get(i);

                        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(key), Bytes.toBytes(value));
                    }

                    put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(startDateColumn), Bytes.toBytes(closeZipTime));
                    put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(endDateColumn), Bytes.toBytes(openZipTime));
                    batch.add(put);

                }


//                System.out.println("start blurryScan Hbase" + new Date());
                Result[] results = hbaseSink.query(rowKeys);
//                System.out.println("stop blurryScan Hbase" + new Date());

                List<Put> batchUpdate = new ArrayList<Put>();

//                System.out.println("results.length : " + results.length);

                Put putUpdate = null;
                if (results != null && results.length > 0) {
                    for (Result result : results) {

                        if (!result.isEmpty()) {
                            String rowKeyHbase = new String(result.getRow());

                            /**
                             * 直接从Hbase获取schema
                             */
//                            System.out.println("rowKeyHbase : "+rowKeyHbase);
//                            Set<byte[]> hbaseFields = result.getNoVersionMap().get(Bytes.toBytes(columnFamily)).keySet();
//                            for(byte[] hbaseField : hbaseFields){
//                                System.out.println(new String(hbaseField));
//                            }

                            byte[] value_byte = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(startDateColumn));
                            String startDateColumnValue = new String(value_byte);

                            if (value_byte != null && !closeZipTime.equals(startDateColumnValue)) {
                                int length = rowKeyHbase.length() - openZipTime.length();
                                String rowKeyPrefix = rowKeyHbase.substring(0, length);
                                String rowKeyUpdate = rowKeyPrefix.concat(closeZipTime);

                                putUpdate = new Put(Bytes.toBytes(rowKeyUpdate));

//                                System.out.println("fields.length : " + fields.length);
                                for (int i = 0; i < fields.length; i++) {
                                    String keyUpdate = fields[i];

                                    putUpdate.add(Bytes.toBytes(columnFamily), Bytes.toBytes(keyUpdate), result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(keyUpdate)));
                                }

                                putUpdate.add(Bytes.toBytes(columnFamily), Bytes.toBytes(startDateColumn), result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(startDateColumn)));
                                putUpdate.add(Bytes.toBytes(columnFamily), Bytes.toBytes(endDateColumn), Bytes.toBytes(closeZipTime));
                                batchUpdate.add(putUpdate);
                            }
                        }
                    }
                }
                batchUpdate.addAll(batch);
                hbaseSink.insertPut(batchUpdate);

                hbaseSink.closeConnection();
            }
        });
    }


}
