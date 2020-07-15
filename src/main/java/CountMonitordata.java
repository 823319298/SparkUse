import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.*;
import java.net.URL;
import java.util.*;

public abstract class CountMonitordata implements Serializable {
    private transient JavaSparkContext sc;
    private transient Configuration inConf;
    private transient JavaRDD<Tuple2<ImmutableBytesWritable, Result>> hbaseRDD = null;
    private transient DataFrame resultDF;
    private transient String date;
    private final transient String[] frequencyList = new String[] { "1", "2", "3", "4", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K",
            "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "a", "b", "c", "d", "e", "f",
            "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x" };

    /**
     * 初始化Spark执行环境
     * @throws IOException
     */
    private void initSparkContext() throws IOException {
        //InputStream in = CountMonitordata.class.getResourceAsStream("/config.properties");
        InputStream in = new FileInputStream(new File("conf"+File.separator+"config.properties"));
        Properties prop = new Properties();
        prop.load(in);
        date = prop.getProperty("date");

    	SparkConf sparkConf = new SparkConf().setAppName("SparkSql countMon");
//        SparkConf sparkConf = new SparkConf().setAppName("SparkSql countMon").set("spark.port.maxRetries","100");
        //修改序列化方式
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.debug.maxToStringFields","52428800");
        //SparkSession替代JavaSparkContext
        sc = new JavaSparkContext(sparkConf);

        inConf = HBaseConfiguration.create();
        URL hbaseurl = CountMonitordata.class.getResource("/hbase-site.xml");
        inConf.addResource(hbaseurl);
        //指定输入表名
        inConf.set(TableInputFormat.INPUT_TABLE, "his:DataCompression");
        inConf.setLong("hbase.client.keyvalue.maxsize", 52428800);   //主要应对超长列
        inConf.set("hbase.client.scanner.timeout.period", "1800000");
        JavaHBaseContext hbaseContext = new JavaHBaseContext(sc, inConf);
        Scan scan = new Scan();
        for (String str : frequencyList) {

            String startRow = str + date + "0";
            String endRow = str + date + "z";

            scan.setStartRow(startRow.getBytes());
            scan.setStopRow(endRow.getBytes());

            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            //将scan编码
            String scanToString = Base64.encodeBytes(proto.toByteArray());

            //Base-64编码的Scanner
            inConf.set(TableInputFormat.SCAN,scanToString);
            JavaRDD<Tuple2<ImmutableBytesWritable, Result>>  tempRDD = hbaseContext.hbaseRDD(TableName.valueOf("his:DataCompression"), scan);
            if (null != hbaseRDD) {
                hbaseRDD = tempRDD.union(hbaseRDD);
            } else {
                hbaseRDD = tempRDD;
            }

        }
        //rowkey匹配正则表达式
//        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(".{1}"+date));
//        scan.setFilter(filter);

    }

    /**
     * 准备数据
     */
    private void prepareData() throws IOException {
        //newAPIHadoopRDD这个RDD用于读取存储在Hadoop中的数据，文件有新 API 输入格式和额外的配置选项，传递到输入格式
        //参数conf会被广播，不可以修改，所以最好一个RDD一个conf
//        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = sc.newAPIHadoopRDD(inConf,TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        //Scan.set这里可以加一些scan条件，读hbase

        //使用map函数将JavaPairRDD=>JavaRDD,反之使用mapToPair函数
        JavaRDD<Row> adataRDD = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>,Row>() {
            //序列化表示UID，用于类的版本控制
            private static final long serialVersionUID = 1L;
            //重写call（）函数，返回Row类型，这部分需要根据自己需求将result中的数据封装为Object[]
            @Override
            public Row call(Tuple2<ImmutableBytesWritable, Result> tuple2) {
                Result result = tuple2._2;
                String rowkey = Bytes.toString(result.getRow());
                String family = null;
                StringBuilder kvalue = new StringBuilder();
                boolean first = true;
                for (Cell c : result.listCells()) {
                    if (Bytes.toString(CellUtil.cloneFamily(c)).equals("A")){
                        family = "A";
                    }else family = "B";
                    if(!first){
                            kvalue.append(',');
                    }
                    first = false;
                    kvalue.append(Bytes.toString(CellUtil.cloneQualifier(c)));
                    kvalue.append('=');
                    kvalue.append(Bytes.toString(CellUtil.cloneValue(c)));
                }
                return RowFactory.create(rowkey,kvalue.toString(),family);
            }
        });

        List<StructField> structFields= new ArrayList<>();
        //创建StructField,基本等同于list内有几个StructField就是几列，需要和上面的Row的Object[]对应
        structFields.add(DataTypes.createStructField("rowkey", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("kvalues", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("family", DataTypes.StringType, true));
        //构建schema，可以把它理解为架子，结构
        StructType schema = DataTypes.createStructType(structFields);

        SQLContext sqlContext = new SQLContext(sc);
        //生成DataFrame，把书放入架子，把数据放入结构里就变成了dataframe
        DataFrame HBaseDF = sqlContext.createDataFrame(adataRDD, schema);
//        DataFrame bHBaseDF = sqlContext.createDataFrame(bdataRDD, schema);
        //展示schema2.2.0
        //HBaseDF.printSchema();

        DataFrame temp = HBaseDF
                .withColumn("newkv",functions.explode(functions.split(HBaseDF.col("kvalues"),",")))
                .drop(HBaseDF.col("kvalues"));
        DataFrame temp1 = temp
                .withColumn("temp",functions.split(temp.col("newkv"),"="));

        resultDF = temp1
                .select(temp1.col("rowkey"),temp1.col("family"),temp1.col("temp").getItem(0).as("times"),temp1.col("temp").getItem(1).as("values"))
                .drop(temp1.col("temp"));

        resultDF.show();
        System.out.println("数据准备完成");
    }

    /**
     * 写入Hbase，MySQL
     */
    private void process(){
        List<Put> puts = new ArrayList<>();
        try {
            Connection connection = ConnectionFactory.createConnection(inConf);
            Table table = connection.getTable(TableName.valueOf("statictis:computeAgg"));
            List<Row> results =  resultDF.groupBy(resultDF.col("rowkey").substr(8,5).as("monitor")).count().collectAsList();
            for(Row result:results){
                Put put = new Put(Bytes.toBytes(result.getAs(0).toString()+date));
                put.addColumn(Bytes.toBytes("A"),Bytes.toBytes("total"),Bytes.toBytes(result.getAs(1).toString()));
                puts.add(put);
            }
            table.put(puts);
            connection.close();
            System.out.println("写入Hbase成功！");
        } catch (IOException e) {
            e.printStackTrace();
        }

//        String mysqlUrl = "jdbc:mysql://192.168.****";
//        String writeTable = "HBase_Spark_teacher";
//        Properties connectionProperties = new Properties();
//        connectionProperties.setProperty("user", "root");
//        connectionProperties.setProperty("password", "root");
//        ResultDF.write().jdbc(mysqlUrl,writeTable,connectionProperties);
    }

    public void invoke() {
        try {
            initSparkContext();
            prepareData();
            process();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
