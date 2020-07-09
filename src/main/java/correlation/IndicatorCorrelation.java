 package decloud.powergrid.statictis.ml.correlation;

import decloud.powergrid.statictis.ml.common.SparkAppBase_Hbase;
import decloud.powergrid.statictis.util.ConstUtil;
import decloud.powergrid.statictis.util.HBaseUtil;
import decloud.powergrid.statictis.util.ReferenceFunction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;

public class IndicatorCorrelation extends SparkAppBase_Hbase {
	
    public IndicatorCorrelation(String startTime, String appName) {
        super(startTime.substring(0, 8), appName);
    	
    }

    @Override
    public void process() {
    	
        //monitor,[<indicator,[<time,value>,...],...]>    0400M00531,[(xL13,{1300=0.032, 1301=0.031,...})]
        JavaPairRDD<String, List<Tuple2<String, TreeMap<Integer, Double>>>> pairRDD = unionRDD.mapToPair(new PairFunction<Result, String, List<Tuple2<String, TreeMap<Integer, Double>>>>() {
            @Override
            public Tuple2<String, List<Tuple2<String, TreeMap<Integer, Double>>>> call(Result result) throws Exception {
                String rowKey = Bytes.toString(result.getRow());
//                String monitorID = rowKey.substring(7, 12);
                String monitorID = monitorMapBroad.value().get(rowKey.substring(7, 12));
                String indicator = rowKey.substring(0, 1) + rowKey.substring(12);
                String subIndicator = rowKey.substring(12);
                List<Tuple2<String, TreeMap<Integer, Double>>> list = new ArrayList<>();
                Map <String,String> indicatorMap = indicatorCorrelationBroad.value();
               
                if(indicatorMap.containsKey(subIndicator)){
                	//取得原始数据
//                	String tag = indicatorMap.get(subIndicator);
                    String family = "A";
                    NavigableMap<byte[], byte[]> navigableMapA = result.getFamilyMap(family.getBytes());
                    
                    TreeMap<Integer, Double> treeMap = new TreeMap<>();
                    for (Map.Entry<byte[], byte[]> entry : navigableMapA.entrySet()) {
                    	try {
                    		Integer time = Integer.parseInt(Bytes.toString(entry.getKey()));
                            Double value = Double.parseDouble(Bytes.toString(entry.getValue()));
                            treeMap.put(time, value);
						} catch (Exception e) {
							return new Tuple2<>("p",list);             //placeholder
							// TODO: handle exception
						}
                        
                    }
                    list.add(new Tuple2<String, TreeMap<Integer, Double>>(indicator, treeMap));
                    return new Tuple2<>(monitorID, list);	
                }else
                	return new Tuple2<>("p",list);             //placeholder
              
            }
        }).reduceByKey(new Function2<List<Tuple2<String, TreeMap<Integer, Double>>>, List<Tuple2<String, TreeMap<Integer, Double>>>, List<Tuple2<String, TreeMap<Integer, Double>>>>() {
            @Override
            public List<Tuple2<String, TreeMap<Integer, Double>>> call(List<Tuple2<String, TreeMap<Integer, Double>>> list1, List<Tuple2<String, TreeMap<Integer, Double>>> list2) throws Exception {
                list1.addAll(list2);
                return list1;
            }
        });
        JavaRDD<List<Put>> putJavaRDD = pairRDD.map(new Function<Tuple2<String, List<Tuple2<String, TreeMap<Integer, Double>>>>, List<Put>>() {
            @Override
            public List<Put> call(Tuple2<String, List<Tuple2<String, TreeMap<Integer, Double>>>> record) throws Exception {
                List<Put> list = new ArrayList<>();
                String monitor = record._1;
                HashMap<String,TreeMap<Integer, Double>> currentMap = new HashMap<String,TreeMap<Integer, Double>>();
                HashMap<String,TreeMap<Integer, Double>> voltageMap = new HashMap<String,TreeMap<Integer, Double>>();
                HashMap<String,TreeMap<Integer, Double>> powerMap = new HashMap<String,TreeMap<Integer, Double>>();
                for (int i = 0; i < record._2.size(); i++) {
                	Tuple2<String, TreeMap<Integer, Double>> t1 = record._2.get(i);
                    String indicator = t1._1;
                    String ind = indicator.substring(1,2);
                    String frequency = frequencyMapBroad.value().get(indicator.substring(0,1));
                    String phase = indicator.substring(2,3);
                    
                    switch(ind){
	                    case "H" :
	                       //语句
	                    	powerMap.put(phase+frequency, t1._2);
	                       break; //可选
	                    case "Y" :
	                       //语句
	                    	voltageMap.put(phase+frequency, t1._2);
	                       break; //可选
	                    case "L" :
	                        //语句
	                    	currentMap.put(phase+frequency, t1._2);
	                        break; //可选
	                    case "b" :
	                        //语句
	                    	currentMap.put(phase+"0", t1._2);
	                        break; //可选
	                    case "Q" :
	                        //语句
	                    	voltageMap.put(phase+"0", t1._2);
	                        break; //可选
	                    default : //可选
	                       //语句
	                }
                }
             
// ====================计算电流与功率的相关系数 P-I 电流与电压的相关系数 U-I 以及 电流、电压与功率的相关系数  P-U-I  =====================
                	
                for(Entry<String,TreeMap<Integer, Double>> current:currentMap.entrySet()){
                	
            //  计算电流与功率的相关系数  P-I
                	String key = current.getKey();
                	String phase = key.substring(0, 1);
                	TreeMap<Integer, Double> treeMap1 = current.getValue();
                	
                	//负序电流与总有功   //谐波电流与三相有功
                	String indicator2 = phase.equals("N")?"T1":phase+"1";
                	TreeMap<Integer, Double> treeMap2 = powerMap.get(indicator2);
                	if(treeMap2==null){
                		System.out.println(monitor +"---"+indicator2+"谐波电流没有对应的功率数据！");
                		continue;
                	}
                	List<Integer> keyList1 = new ArrayList<>();
                    List<Integer> keyList2 = new ArrayList<>();

                    keyList1.addAll(treeMap1.keySet());
                    keyList2.addAll(treeMap2.keySet());
                    //求交集
                    keyList1.retainAll(keyList2);

                    Double[] values1 = new Double[keyList1.size()];
                    Double[] values2 = new Double[keyList1.size()];

                    for (int idx = 0; idx < keyList1.size(); idx++) {
                        values1[idx] = treeMap1.get(keyList1.get(idx));
                        values2[idx] = treeMap2.get(keyList1.get(idx));
                    }

                    double correlation = CorrelationHelper.getPearson(values1, values2);
                    
                    Put put = new Put((startTimeB.value()+monitor).getBytes());
                    put.addColumn("A".getBytes(), key.getBytes(), String.valueOf(correlation).getBytes());
                    list.add(put);
      
                    
          //   计算电流与电压的相关系数  U-I
                    String key1 = current.getKey();
                	TreeMap<Integer, Double> treeMap11 = current.getValue();
                	TreeMap<Integer, Double> treeMap21 = voltageMap.get(key);
                	List<Integer> keyList11 = new ArrayList<>();
                    List<Integer> keyList21 = new ArrayList<>();
                    if(treeMap21==null){
                		System.out.println(monitor+"----"+key+"谐波电流没有对应的谐波电压！");
                		continue;
                	}
                    keyList11.addAll(treeMap11.keySet());
                    keyList21.addAll(treeMap21.keySet());
                    //求交集
                    keyList11.retainAll(keyList21);

                    Double[] values11 = new Double[keyList11.size()];
                    Double[] values21 = new Double[keyList11.size()];

                    for (int idx1 = 0; idx1 < keyList11.size(); idx1++) {
                        values11[idx1] = treeMap11.get(keyList11.get(idx1));
                        values21[idx1] = treeMap21.get(keyList11.get(idx1));
                    }

                    double correlation1 = CorrelationHelper.getPearson(values11, values21);   //U-I
                    
                    Put put1 = new Put((startTimeB.value()+monitor).getBytes());
                    put1.addColumn("B".getBytes(), key.getBytes(), String.valueOf(correlation1).getBytes());
                    list.add(put1);
                }
                
                return list;
            }
        });

        final JavaRDD<Put> putRDD = putJavaRDD.flatMap(new FlatMapFunction<List<Put>, Put>() {
            @Override
            public Iterable<Put> call(List<Put> puts) throws Exception {
                return puts;
            }
        });
            
        Configuration outConf_Copy = null;
 		if(ConstUtil.GWFLAG.equals("1")){			
 			outConf_Copy = HBaseConfiguration.create();
 		}else{
 			//////////////////////////////////////////这里加入国家电网验证////////////////////////////////////////////
 			try {
				outConf_Copy = HBaseUtil.getConnectionGW();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
 			//////////////////////////////////////////本地测试时需要注释//////////////////////////////////////////////	
 		}

 		JavaHBaseContext hBaseContext = new JavaHBaseContext(sc, outConf_Copy);
 		
//        JavaHBaseContext hBaseContext = new JavaHBaseContext(sc, HBaseConfiguration.create());
        hBaseContext.bulkPut(putRDD, TableName.valueOf("statictis:indicatorCorrelation"), new Function<Put, Put>() {
            @Override
            public Put call(Put put) throws Exception {
            	return put;
            }
        });
        
        System.out.println("数据上传成功！！！！！");  
        sc.stop();
    }

}
