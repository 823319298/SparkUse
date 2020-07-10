import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class CountMonitordata {

    private static transient SparkSession sc;
    private static transient Configuration inConf;
    private static transient Scan scan;

    /**
     * 初始化Spark执行环境
     * @throws IOException
     */
    private static void initSparkContext() throws IOException {
    	SparkConf sparkConf = new SparkConf().setAppName("SparkSql countMon").setMaster("local[*]");
//        SparkConf sparkConf = new SparkConf().setAppName("SparkSql countMon").set("spark.port.maxRetries","100");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");//修改序列化方式
        sc = SparkSession.builder().config(sparkConf).getOrCreate();//SparkSession替代JavaSparkContext

        inConf = HBaseConfiguration.create();
        scan = new Scan();

        //将scan编码
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        String scanToString = Base64.encodeBytes(proto.toByteArray());

        inConf.setLong("hbase.client.keyvalue.maxsize", 52428800);   //主要应对超长列
        inConf.set("hbase.client.scanner.timeout.period", "1800000");
        //把要查询的表视图化
        inConf.set(TableInputFormat.INPUT_TABLE, "his:DataCompression");
        inConf.set(TableInputFormat.SCAN,scanToString);

        System.out.println("initSparkContext() success!!!!");

    }

    /**
     * 准备数据
     */
    private static void prepareData(JavaSparkContext jsc) throws IOException {
        //hbase数据转RDD
        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = jsc.newAPIHadoopRDD(inConf,TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        JavaRDD<Row> dataRDD = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>,Row>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Row call(Tuple2<ImmutableBytesWritable, Result> tuple2) {
                Result result = tuple2._2;
                String[] values = new String[1441];
                values[0] = Bytes.toString(result.getRow());
                int sum=1;
                for(int i=0;i<24;++i){
                    for(int j=0;j<60&&sum<1441;++j){
                        values[sum] = Bytes.toString(result.getValue("A".getBytes(),(String.format("%02d",i)+String.format("%02d",j)).getBytes()));
                        ++sum;
                    }
                }
                return RowFactory.create((Object[]) values);
            }
        });

        List<StructField> structFields= new ArrayList<>();
        structFields.add(DataTypes.createStructField("rowKey", DataTypes.StringType, true));
        int sum=1;
        for(int i=0;i<24;++i){
            for(int j=0;j<60&&sum<1441;++j){
                structFields.add(DataTypes.createStructField("R"+String.format("%02d",i)+String.format("%02d",j), DataTypes.StringType, true));
                 ++sum;
            }
        }

        //构建schema
        StructType schema = DataTypes.createStructType(structFields);
        //生成DataFrame
        Dataset<Row> HBaseDF = sc.createDataFrame(dataRDD, schema);
        //df相关操作
        HBaseDF.printSchema();

        HBaseDF.createOrReplaceTempView("monitor");
        StringBuilder sql = new StringBuilder("select rowKey");
        sum=1;
        for(int i=0;i<24;++i){
            for(int j=0;j<60&&sum<1441;++j){
                sql.append(",R").append(String.format("%02d", i)).append(String.format("%02d", j));
                ++sum;
            }
        }
        Dataset<Row> newDF = sc.sql(sql + " from monitor");
        newDF.show();
//        String mysqlUrl = "jdbc:mysql://192.168.****";
//        String writeTable = "HBase_Spark_teacher";
//        Properties connectionProperties = new Properties();
//        connectionProperties.setProperty("user", "root");
//        connectionProperties.setProperty("password", "root");
//        newDF.write().jdbc(mysqlUrl,writeTable,connectionProperties);

    }

    public static void main(String[] args) throws IOException {
        try {
            initSparkContext();
            prepareData(JavaSparkContext.fromSparkContext(sc.sparkContext()));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
//        process();
    }
}
