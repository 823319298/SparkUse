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
        //修改序列化方式
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        //SparkSession替代JavaSparkContext
        sc = SparkSession.builder().config(sparkConf).getOrCreate();

        inConf = HBaseConfiguration.create();
        scan = new Scan();

        //将scan编码
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        String scanToString = Base64.encodeBytes(proto.toByteArray());

        inConf.setLong("hbase.client.keyvalue.maxsize", 52428800);   //主要应对超长列
        inConf.set("hbase.client.scanner.timeout.period", "1800000");
        //指定输入表名
        inConf.set(TableInputFormat.INPUT_TABLE, "his:DataCompression");
        //Base-64编码的Scanner
        inConf.set(TableInputFormat.SCAN,scanToString);

        System.out.println("initSparkContext() success!!!!");

    }

    /**
     * 准备数据
     */
    private static void prepareData(JavaSparkContext jsc) throws IOException {
        //newAPIHadoopRDD这个RDD用于读取存储在Hadoop中的数据，文件有新 API 输入格式和额外的配置选项，传递到输入格式
        //参数conf会被广播，不可以修改，所以最好一个RDD一个conf
        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = jsc.newAPIHadoopRDD(inConf,TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        //Scan.set这里可以加一些scan条件，读hbase
        //使用map函数将JavaPairRDD=>JavaRDD,反之使用mapToPair函数
        JavaRDD<Row> dataRDD = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>,Row>() {
            //序列化表示UID，用于类的版本控制
            private static final long serialVersionUID = 1L;
            //重写call（）函数，返回Row类型，这部分需要根据自己需求将result中的数据封装为Object[]
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
                //creat()方法参数（object... value）
                return RowFactory.create((Object[]) values);
            }
        });

        List<StructField> structFields= new ArrayList<>();
        //创建StructField,基本等同于list内有几个StructField就是几列，需要和上面的Row的Object[]对应
        structFields.add(DataTypes.createStructField("rowKey", DataTypes.StringType, true));
        int sum=1;
        for(int i=0;i<24;++i){
            for(int j=0;j<60&&sum<1441;++j){
                structFields.add(DataTypes.createStructField("R"+String.format("%02d",i)+String.format("%02d",j), DataTypes.StringType, true));
                 ++sum;
            }
        }

        //构建schema，可以把它理解为架子，结构
        StructType schema = DataTypes.createStructType(structFields);
        //生成DataFrame，把书放入架子，把数据放入结构里就变成了dataframe
        Dataset<Row> HBaseDF = sc.createDataFrame(dataRDD, schema);
        //展示schema
        HBaseDF.printSchema();

        //HBaseDF.createOrReplaceTempView("monitor");
        StringBuilder sql = new StringBuilder();
        sum=1;
        for(int i=0;i<24;++i){
            for(int j=0;j<60&&sum<1441;++j){
                sql.append(", 'R").append(String.format("%02d",i)).append(String.format("%02d",j)).append("', R").append(String.format("%02d",i)).append(String.format("%02d",j));
                ++sum;
            }
        }
        Dataset<Row>  ResultDF = HBaseDF.selectExpr("rowkey","stack(1440"+sql+") as (`times`,`values`)").orderBy("rowkey","times");
        ResultDF.show();

//        StringBuilder sql = new StringBuilder("select rowKey");
//        sum=1;
//        for(int i=0;i<24;++i){
//            for(int j=0;j<60&&sum<1441;++j){
//                sql.append(",R").append(String.format("%02d", i)).append(String.format("%02d", j));
//                ++sum;
//            }
//        }
//        Dataset<Row> newDF = sc.sql(sql + " from monitor");
//        newDF.show();
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
