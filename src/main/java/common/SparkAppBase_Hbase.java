package decloud.powergrid.statictis.ml.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.sysFuncNames_return;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SQLContext;

import decloud.powergrid.statictis.util.ConstUtil;
import decloud.powergrid.statictis.util.HBaseUtil;
import decloud.powergrid.statictis.util.ReferenceFunction;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author Felix
 * @version 1.0
 */
public abstract class SparkAppBase_Hbase implements Serializable {
    public String startTime;
    public String appName;
    protected transient JavaRDD<Result> unionRDD;
    protected transient JavaSparkContext sc;
    protected transient SQLContext sqlContext;
    protected transient JavaHBaseContext hbaseContext;
    protected transient Configuration inConf;
    protected transient String inputTable = "his:DataCompression"; // 从该表中读取所有数据;
    

    //监测点新旧编码映射
    protected Broadcast<Map<String, String>> monitorMapBroad = null;
    //值类型新旧编码映射
    protected Broadcast<Map<String, List<String>>> valueTypeMapBroad = null;
    //相别新旧编码映射
    protected Broadcast<Map<String, List<String>>> phaseMapBroad = null;
    //频率新旧编码映射
    protected Broadcast<Map<String, String>> frequencyMapBroad = null;
    //指标新旧编码映射
    protected Broadcast<Map<String, String>> indicatorMapBroad = null;
    
    //相关性分析用到的指标
    protected Broadcast<Map<String, String>> indicatorCorrelationBroad = null;
    
    protected Broadcast<String> startTimeB = null;
    
    protected Broadcast<HashSet<String>> monitorsBroad = null;
    
    public SparkAppBase_Hbase(String startTime, String appName) {
        this.startTime = startTime;
        this.appName = appName;
    }

    /**
     * 初始化Spark执行环境
     * @throws IOException 
     */
    protected void initSparkContext() throws IOException {
//    	SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster("local");
        SparkConf sparkConf = new SparkConf().setAppName(appName).set("spark.port.maxRetries","100");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//          sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
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

    /**
     * 广播变量
     *
     * @param jsc
     */
    protected void broadcastVariables(JavaSparkContext jsc) {
    	
        //监测点对应表
        Map<String, String> monitorMap = MappingInfo.getMonitorMapping();
        //值类型对应表
        Map<String, List<String>> valueTypeMap = MappingInfo.getValueTypeMapping();
        //相别对应表
        Map<String, List<String>> phaseMap = MappingInfo.getPhaseMapping();
        //频率对应表
        Map<String, String> frequencyMap = MappingInfo.getFrequencyMapping();
        //指标对应表
        Map<String, String> indicatorMap = MappingInfo.getIndicatorMapping();
        
       //需要计算相关性的指标列表
        Map<String, String> indicatorCorrelation = MappingInfo.getIndicatorCorrelation();
        
        monitorMapBroad = jsc.broadcast(monitorMap);
        indicatorMapBroad = jsc.broadcast(indicatorMap);
        valueTypeMapBroad = jsc.broadcast(valueTypeMap);
        phaseMapBroad = jsc.broadcast(phaseMap);
        frequencyMapBroad = jsc.broadcast(frequencyMap);
        indicatorCorrelationBroad = jsc.broadcast(indicatorCorrelation);
        startTimeB = jsc.broadcast(startTime);
     	
    	String date = startTime.substring(0, 4) + "-" + startTime.substring(4, 6) + "-" + startTime.substring(6, 8);
    	
    	//需要计算相关性的监测点列表
    	HashSet<String> monitorSet = ReferenceFunction.getMonitorIds(date); 
    	monitorsBroad = jsc.broadcast(monitorSet);
    }

    /**
     * 准备数据
     */
    protected void prepareData() {
        try {
			unionRDD = loadDataFromHbase(sc, inConf, hbaseContext, startTime, inputTable);
			
			System.out.println("prepareData() success!!!!");
			
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    /**
     * 从Hbase加载数据
     *
     * @param jsc
     * @param config
     * @param hBaseContext
     * @param startTime
     * @param tableName
     * @return
     */
    public JavaRDD<Result> loadDataFromHbase(JavaSparkContext jsc, final Configuration config, final JavaHBaseContext hBaseContext, final String startTime, String tableName) 
    		throws IOException, InterruptedException{
        
    	final TableName inputTable = TableName.valueOf(tableName);
        final String[] frequencyList = new String[] { "1", "2", "3", "4", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K",
				"L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "a", "b", "c", "d", "e", "f",
				"g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x" };
       
        
        
        UserGroupInformation.setConfiguration(inConf);
		UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI("JX000002",
				"/keytab/JX000002.keytab");
		UserGroupInformation.setLoginUser(ugi);
		JavaPairRDD<ImmutableBytesWritable, Result> unionRDD = ugi
				.doAs(new PrivilegedExceptionAction<JavaPairRDD<ImmutableBytesWritable, Result>>() {
					@Override
					public JavaPairRDD<ImmutableBytesWritable, Result> run() throws Exception {
					
						JavaRDD<Tuple2<ImmutableBytesWritable, Result>> resultJavaPairRDD = null;
						
						 Scan scan = new Scan();
						 
						 ReferenceFunction test = new ReferenceFunction();		
						 HashSet<String> set = new HashSet<String>(); // 监测点id
						 
						 for (String str : frequencyList) {
							 
							 String startRow = str + startTime.substring(2, 8) + "0";
							 String endRow = str + startTime.substring(2, 8) + "z";
					        
							 scan.setStartRow(startRow.getBytes());
							 scan.setStopRow(endRow.getBytes());
							 
				        	
							 ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
					           
							 String ScanToString = Base64.encodeBytes(proto.toByteArray());
							 config.set(TableInputFormat.SCAN, ScanToString);
							 JavaRDD<Tuple2<ImmutableBytesWritable, Result>> tempRDD = hBaseContext.hbaseRDD(inputTable, scan);
							 if (null != resultJavaPairRDD) {
				                resultJavaPairRDD = tempRDD.union(resultJavaPairRDD);
							 } else {
				                resultJavaPairRDD = tempRDD;
							 }
				        
						 	}
					        
						 JavaPairRDD<ImmutableBytesWritable, Result> unionRDD = JavaPairRDD.fromJavaRDD(resultJavaPairRDD);
						 return unionRDD;
					}
				});
       

        JavaRDD<Result> resultJavaRDD = unionRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Result>() {
            @Override
            public Result call(Tuple2<ImmutableBytesWritable, Result> record) throws Exception {
                return record._2;
            }
        }).filter(new Function<Result, Boolean>() {
			@Override
			public Boolean call(Result arg0) throws Exception {
				HashSet<String> set = new HashSet<String>();
				set = monitorsBroad.value();
				String monitorId = Bytes.toString(arg0.getRow()).substring(7, 12);
				if( set.contains(monitorId)) {
					return true;
				}else {
					return false;
				}
			
			}
        });
        
        return resultJavaRDD;
    }


    /**
     *
     */
    public void invoke() {
        try {
			initSparkContext();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        prepareData();
        process();
    }

    /**
     *
     */
    public abstract void process();
}
