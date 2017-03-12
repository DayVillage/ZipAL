package graduation;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * Created by UrbanSy on 2017/1/16 21:24.
 */
public class HiveSource implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(HiveSource.class);

    public transient SparkConf sparkConf;
    public transient SQLContext hiveContext;
    public transient SparkContext sparkContext;

    public String zkQuorum;
    public String zkPort;
    public String zkParent;

    public static String getRowKey(String primaryKey, String timeString) {
        return DigestUtils.md5Hex(primaryKey).substring(0,2) + "_" + primaryKey + "_" + timeString;
    }

    public static String updateRowKey(String rowKey, String timeString) {
        int index = rowKey.lastIndexOf("_");
        return rowKey.substring(0,index) + "_" + timeString;
    }

    public void init(String configFilePath) {
         sparkConf = new SparkConf();
         sparkConf.setAppName("Zipper");
         sparkConf.setMaster("local");
         sparkContext = new SparkContext(sparkConf);

//        sc = SparkContext.getOrCreate();

        hiveContext = new HiveContext(sparkContext);

        Properties properties = PropertiesUtil.loadFromFile(configFilePath);
        this.zkQuorum = properties.getProperty("hbase.zookeeper.quorum");
        this.zkPort = properties.getProperty("hbase.zookeeper.property.clientPort");
        this.zkParent = properties.getProperty("zookeeper.znode.parent");

    }

    public DataFrame readData(String sql) {
        System.out.println(sql);
        DataFrame dataFrame = hiveContext.sql(sql);
        return dataFrame;
    }

    public void mapPartitions(DataFrame dataFrame, final String key, final String openZipTime,
                              final String hbaseTableName, final String columnFamily, final String closeZipTime,
                              final String startDateColumn, final String endDateColumn) {
        final byte[] columnFamilyBytes = Bytes.toBytes(columnFamily);
        final byte[] startDateColumnBytes = Bytes.toBytes(startDateColumn);
        final byte[] endDateColumnBytes = Bytes.toBytes(endDateColumn);
        final byte[] openZipTimeBytes = Bytes.toBytes(openZipTime);
        final byte[] closeZipTimeBytes = Bytes.toBytes(closeZipTime);

        JavaRDD<Row> javaRDD = dataFrame.toJavaRDD();

        javaRDD.foreachPartition(new VoidFunction<Iterator<Row>>() {
            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Iterator<Row> rowIterator) throws Exception {

                List<String> rowKeys = new ArrayList<String>();
                Row row = null;
                List<Put> batch = new ArrayList<Put>();
                String[] fields = null;
                while (rowIterator.hasNext()) {
                    row = rowIterator.next();

                    if (row.size() == 0) {
                        continue;
                    }

                    int primaryKeyIndex = row.fieldIndex(key);
                    String rowKey = getRowKey((String) row.get(primaryKeyIndex), openZipTime);
                    rowKeys.add(rowKey);

                    // System.out.println("rowKey : " + rowKey);

                    Put put = new Put(Bytes.toBytes(rowKey));
                    if (fields == null) {
                        fields = row.schema().fieldNames();
                    }
                    for (int i = 0; i < fields.length; i++) {

                        String key = fields[i];
                        String value = (String) row.get(i);

                        put.add(columnFamilyBytes, Bytes.toBytes(key), Bytes.toBytes(value));
                    }

                    put.add(columnFamilyBytes, startDateColumnBytes, closeZipTimeBytes);
                    put.add(columnFamilyBytes, endDateColumnBytes, openZipTimeBytes);
                    batch.add(put);

                }

                System.out.println("UpdatePuts1");

                if (rowKeys.size() > 0) {

                    System.out.println("UpdatePuts2");

                    HbaseSink hbaseSink = new HbaseSink();
                    hbaseSink.init(zkQuorum, zkPort, zkParent, hbaseTableName);

                    Result[] results = hbaseSink.query(rowKeys);

                    List<Put> batchUpdate = new ArrayList<Put>();

                    // System.out.println("results.length : " + results.length);

                    // store duplicate key in this map for delete them from
                    // batch list
                    Map<byte[], Integer> duplicateKeys = new HashMap<byte[], Integer>();

                    Put putUpdate = null;
                    if (results != null && results.length > 0) {
                        for (Result result : results) {

                            if (!result.isEmpty()) {
                                byte[] rowkey = result.getRow();
                                String rowKeyString = new String(rowkey);

                                System.out.println("rowKeyString : "+rowKeyString);

                                byte[] valueBytes = result.getValue(columnFamilyBytes,
                                        startDateColumnBytes);

                                if (valueBytes != null) {
                                    String startDate = new String(valueBytes);

                                    System.out.println("startDate :"+startDate);

                                    if (closeZipTime.equals(startDate)) {
                                        // this row has already exist in the
                                        // table, no need to insert or update
                                        duplicateKeys.put(rowkey, 1);

                                        System.out.println("closeZipTime.equals(startDate)");

                                    } else {
                                        // this row has to be closed
                                        String rowKeyUpdate = updateRowKey(rowKeyString, closeZipTime);

                                        putUpdate = new Put(Bytes.toBytes(rowKeyUpdate));

                                        System.out.println("rowKeyUpdate : "+rowKeyUpdate);

                                        for (byte[] column : result.getFamilyMap(columnFamilyBytes).keySet()) {
                                            String columnName = new String(column);

                                            System.out.println("endDateColumn : "+endDateColumn);
                                            System.out.println("columnName : "+ columnName);

                                            if (endDateColumn.equals(columnName)) {
                                                //set column value for end date column
                                                putUpdate.add(columnFamilyBytes, endDateColumnBytes,
                                                        closeZipTimeBytes);
                                            }
                                            else {
                                                //copy other fields
                                                putUpdate.add(
                                                        columnFamilyBytes, column,
                                                        result.getValue(columnFamilyBytes, column));
                                            }

                                        }
                                        batchUpdate.add(putUpdate);
                                    }
                                }
                            }
                        }
                    }

                    if (duplicateKeys.size() > 0) {
                        for (Put put : batch) {
                            // if this put is not in inserted map, then add to real
                            // batch update list
                            byte[] rowkey = put.getRow();
                            if (duplicateKeys.get(rowkey) == null) {
                                batchUpdate.add(put);
                            }
                        }
                    }
                    else {
                        //no duplicate keys, add all new rows
                        batchUpdate.addAll(batch);
                    }

                    hbaseSink.insertPut(batchUpdate);

                    hbaseSink.closeConnection();
                }
            }
        });
    }

}

