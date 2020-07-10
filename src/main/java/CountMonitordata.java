import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
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

public abstract class CountMonitordata {

    private static Dataset<Row> ResultDF;
    private static transient SparkSession sc;
    private static transient Configuration inConf;
    private static transient Scan scan;

    public static Dataset<Row> getResultDF() {
        return ResultDF;
    }

    public static void setResultDF(Dataset<Row> resultDF) {
        ResultDF = resultDF;
    }

    public static SparkSession getSc() {
        return sc;
    }

    public static void setSc(SparkSession sc) {
        CountMonitordata.sc = sc;
    }

    public static Configuration getInConf() {
        return inConf;
    }

    public static void setInConf(Configuration inConf) {
        CountMonitordata.inConf = inConf;
    }

    public static Scan getScan() {
        return scan;
    }

    public static void setScan(Scan scan) {
        CountMonitordata.scan = scan;
    }

    /**
     * 初始化Spark执行环境
     * @throws IOException
     */
    private static void initSparkContext(String data) throws IOException {
    	SparkConf sparkConf = new SparkConf().setAppName("SparkSql countMon").setMaster("local[*]");
//        SparkConf sparkConf = new SparkConf().setAppName("SparkSql countMon").set("spark.port.maxRetries","100");
        //修改序列化方式
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.debug.maxToStringFields","52428800");
        //SparkSession替代JavaSparkContext
        sc = SparkSession.builder().config(sparkConf).getOrCreate();

        inConf = HBaseConfiguration.create();
        scan = new Scan();

        //rowkey匹配正则表达式
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(".{1}"+data));
        scan.setFilter(filter);
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

        StringBuilder sql = new StringBuilder();
        sum=1;
        for(int i=0;i<24;++i){
            for(int j=0;j<60&&sum<1441;++j){
                sql.append(", '").append(String.format("%02d",i)).append(String.format("%02d",j)).append("', R").append(String.format("%02d",i)).append(String.format("%02d",j));
                ++sum;
            }
        }
        //DataFrame 行列转换
        ResultDF = HBaseDF.selectExpr("rowkey","stack(1440"+sql+") as (`times`,`values`)").orderBy("rowkey","times");

        //给ResultDF视图命名，方便后续sql操作
        ResultDF.createOrReplaceTempView("monitor");
    }

    /**
     * 写SQL
     */
    private static void process(){

        //String sql = "Select SUBSTRING(rowkey,8,5),Count(values) As Count FROM monitor where values is not null group by SUBSTRING(rowkey,8,5)";
        String sql = "Select substring(rowkey,8,5) as monitor,count(times) as num FROM monitor where values is not null group by substring(rowkey,8,5)";

        Dataset<Row> newDF = sc.sql(sql);
        newDF.show();
//        String mysqlUrl = "jdbc:mysql://192.168.****";
//        String writeTable = "HBase_Spark_teacher";
//        Properties connectionProperties = new Properties();
//        connectionProperties.setProperty("user", "root");
//        connectionProperties.setProperty("password", "root");
//        ResultDF.write().jdbc(mysqlUrl,writeTable,connectionProperties);
    }

    public static void main(String[] args) throws IOException {
        try {
            initSparkContext("200624");
            prepareData(JavaSparkContext.fromSparkContext(sc.sparkContext()));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        process();
    }
}
