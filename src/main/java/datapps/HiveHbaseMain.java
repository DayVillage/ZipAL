package datapps;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.SparkContext;

/**
 * Created by UrbanSy on 2017/1/13 11:15.
 */
public class HiveHbaseMain {

    //    private transient static HiveSource hiveSource;
    private transient static HiveSource hiveSource;

    public static void main(String[] args) {
        SparkTestEnv.init();
        System.out.println("Start -------------------------");

        if (args.length < 8){
            throw new RuntimeException("The number of args is less than 8.");
        }

        String configFilePath = new String(args[0]);
        String tableName = new String(args[1]);
        String key = new String(args[2]);


        String hbaseTableName = new String(args[3]);
        String columnFamily = new String(args[4]);
        String closeZipTime = new String(args[5]);
        String openZipTime = new String(args[6]);
        String startDateColumn = new String(args[7]);
        String endDateColumn = new String(args[8]);

        if (configFilePath == null || "".equals(configFilePath)) {
            System.out.println("configFilePath 不能为空");
            return;
        } else if (tableName == null || "".equals(tableName)) {
            System.out.println("tableName 不能为空");
            return;
        } else if (key == null || "".equals(key)) {
            System.out.println("key 不能为空");
            return;
        } else if (closeZipTime == null || "".equals(closeZipTime)) {
            System.out.println("closeZipTime 不能为空");
            return;
        } else if (openZipTime == null || "".equals(openZipTime)) {
            System.out.println("openZipTime 不能为空");
            return;
        } else if (hbaseTableName == null || "".equals(hbaseTableName)) {
            System.out.println("hbaseTableName 不能为空");
            return;
        } else if (columnFamily == null || "".equals(columnFamily)) {
            columnFamily = "cf";
        } else if (startDateColumn == null || "".equals(startDateColumn)) {
            startDateColumn = "start_dt";
        } else if (endDateColumn == null || "".equals(endDateColumn)) {
            endDateColumn = "end_dt";
        }

        hiveSource = new HiveSource();
        hiveSource.init(configFilePath, hbaseTableName);

        String sql = "select * from " + tableName + " limit 10";
        DataFrame dataFrame = hiveSource.readData(sql);

        hiveSource.mapPartitions(dataFrame, key, openZipTime, hbaseTableName, columnFamily, closeZipTime, startDateColumn, endDateColumn);

    }

}
