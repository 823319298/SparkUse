

# Spark读取Hbase数据转DataFrame（Java API）
最近刚开始学习Spark，没接触过Scala，发现网上Java的资料比Scala少很多，一边学习一边实现功能很多遇到的问题列在最后做个笔记。初学者，有理解错误还请指正。
&nbsp; 
## 环境
Hadoop2.7.7+Hbase1.3.5+Spark2.2.0
项目Maven管理
完整代码在github：[https://github.com/823319298/SparkUse](https://github.com/823319298/SparkUse)
帮到您的话可以去打个星！
&nbsp; 
## 过程
### pom文件引入相应依赖 
HBase依赖：
```xml
		<dependency>
            <groupId>org.apache.hbase</groupId>
            <artifactId>hbase-server</artifactId>
            <version>1.3.5</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hbase</groupId>
            <artifactId>hbase-client</artifactId>
            <version>1.3.5</version>
        </dependency>
```
Spark项目之后会用到SparkSql，一起引入了：
```xml
		<dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.11</artifactId>
            <version>2.2.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.11</artifactId>
            <version>2.2.0</version>
        </dependency>
```
先列出所有需要的包，大家可以导入的时候参考

```java
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
```
&nbsp; 
### 初始化Spark：
分为本地环境与集群环境，自己做相应修改

```java
	//本地环境
	SparkConf sparkConf = new SparkConf().setAppName("SparkSql countMon").setMaster("local[*]");
	//修改序列化方式
	sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
	//SparkSession替代JavaSparkContext
	SparkSession sc = SparkSession.builder().config(sparkConf).getOrCreate();
```
SparkSession是spark新增的替代了JavaSparkContext和弃用的~~SQLContext~~ 成为新的入口。序列化的配置是必需的，在网络间传输的时候对象需要序列化。
如果需要从SparkSession获取到JavaSparkContext：

```java
		JavaSparkContext.fromSparkContext(sc.sparkContext())
```
初始化Scan，需要对Scan进行Base64编码，Base64就是用来将非ASCII字符的数据转换成ASCII字符的一种方法。

```java
		//将scan编码
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        String scanToString = Base64.encodeBytes(proto.toByteArray());
```

初始化Hbase，配置HBaseConfiguration，初始化Scan
这里顺便记一句，之前遇到hbase-site.xml存放位置的问题，查过之后发现初始化的时候会从classpath开始扫描，找到的第一个hbase-site.xml读取。
```java
		Configuration hbConf = HBaseConfiguration.create();

        //指定输入表名
        hbConf.set(TableInputFormat.INPUT_TABLE, "你的表名字");
        //Base-64编码的Scanner
        hbConf.set(TableInputFormat.SCAN,scanToString);
```
至此初始化准备工作完成。
&nbsp; 
### 读取Hbase
```java
		//获取JavaSparkContext
		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc.sparkContext());
		//newAPIHadoopRDD这个RDD用于读取存储在Hadoop中的数据，文件有新 API 输入格式和额外的配置选项，传递到输入格式
		//参数conf会被广播，不可以修改，所以最好一个RDD一个conf
		JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = jsc.newAPIHadoopRDD(hbConf,TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
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
```
现在我们已经得到了JavaRDD（Row），这个RDD里有我们封装好的hbase里的数据，下面需要将它转化为DataFrame。他们之间的关系可以看下面遇到的问题里面的三者关系。

```java
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
```

```java
        //构建schema，可以把它理解为架子，结构
        StructType schema = DataTypes.createStructType(structFields);
        //生成DataFrame，把书放入架子，把数据放入结构里就变成了dataframe
        Dataset<Row> HBaseDF = sc.createDataFrame(dataRDD, schema);
```
想看一下架子长啥样：
```java
		HBaseDF.printSchema();
```
最后得到的dataframe就是HbaseDF
下面是遇到的一些问题和一些注意事项，有需要的可以看看，完整代码在github：[https://github.com/823319298/SparkUse](https://github.com/823319298/SparkUse)
&nbsp; 
## 遇到的问题
&nbsp; 
### 版本问题，报错：

```java
Exception in thread "main" java.lang.NoSuchMethodError: org.apache.hadoop.conf.Configuration.getPassword(Ljava/lang/String;)[C
```
之前Spark用的2.4.5，降到了2.2.0
&nbsp; 
### ImmutableBytesWritable类型
Hbase一般使用ImmutableBytesWritable作为rowkey的类型
&nbsp; 
### Tuple2
元组的概念
&nbsp; 
### lambda表达式
接口中只有一个需要被实现的方法时才能使用
Function接口：

```java
package org.apache.spark.api.java.function;

import java.io.Serializable;

@FunctionalInterface
public interface Function<T1, R> extends Serializable {
    R call(T1 var1) throws Exception;
}
```
流标识符，用于类的版本定义，不定义也会自动计算出一个。定义为1L的话类修改后反序列化不会报错，向上兼容。
```java
private static final long serialVersionUID = 1L;
```

Function实现了Serializable接口，因此可以被保存到文件中或者网络传输
&nbsp; 
### RDD和DataFrame和DataSet三者间的区别
这里看一下这个[https://blog.csdn.net/weixin_43087634/article/details/84398036](https://blog.csdn.net/weixin_43087634/article/details/84398036)       感谢大佬
Garbage Collection可以翻译为“垃圾收集” -- 一般主观上会认为做法是：找到垃圾，然后把垃圾扔掉。在VM中，GC的实现过程恰恰相反，GC的目的是为了追踪所有正在使用的对象，并且将剩余的对象标记为垃圾，随后标记为垃圾的对象会被清除，回收这些垃圾对象占据的内存，从而实现内存的自动管理。
Dataframe是Dataset的特列，DataFrame=Dataset[Row]
