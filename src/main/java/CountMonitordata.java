import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class CountMonitordata {

    private static transient SparkSession sc ;
    private static transient SQLContext sqlContext;
    /**
     * 初始化Spark执行环境
     * @throws IOException
     */
    private void initSparkContext() throws IOException {
//    	SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster("local");
        sc = SparkSession.builder().master("local")
                .appName("Java Spark SQL")
                .getOrCreate();

        SparkConf sparkConf = new SparkConf().setAppName("SparkSql countMon").set("spark.port.maxRetries","100");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");//修改序列化方式
        sc = new JavaSparkContext(sparkConf);
        sqlContext = new SQLContext(sc);

        if (ConstUtil.GWFLAG.equals("1")) {
            inConf = HBaseConfiguration.create();
        } else {
            ////////////////////////////////////////// 这里加入国家电网验证////////////////////////////////////////////
            inConf = HBaseUtil.getConnectionGW();
            ////////////////////////////////////////// 本地测试时需要注释//////////////////////////////////////////////
        }

        inConf.setLong("hbase.client.keyvalue.maxsize", 52428800);   //主要应对超长列
        inConf.set("hbase.client.scanner.timeout.period", "1800000");

        inConf.set(TableInputFormat.INPUT_TABLE, inputTable);
        hbaseContext = new JavaHBaseContext(sc, inConf);

        System.out.println("initSparkContext() success!!!!");

        broadcastVariables(sc);

        System.out.println("broadcastVariables success!!!!");
    }

    public static void main(String[] args) {
        try {
            initSparkContext();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        prepareData();
        process();
    }
}
