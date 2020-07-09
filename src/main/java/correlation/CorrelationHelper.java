package decloud.powergrid.statictis.ml.correlation;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import decloud.powergrid.statictis.runningStatus.MonitorRunningStatusEntity;
import decloud.powergrid.statictis.util.ConstUtil;
import decloud.powergrid.statictis.util.HBaseUtil;
import decloud.powergrid.statictis.util.MySqlUtils;

public class CorrelationHelper {
	
	/*x,y 是两个随机变量，求x,y的皮尔逊相关系数*/
    public static double getPearson(Double datax[], Double datay[]) {
        double r = 0;
        double avg_x = getAvg(datax);
        double avg_y = getAvg(datay);
        double var_x = 0, var_y = 0, sumxy = 0;
        for (int i = 0; i < datax.length; i++) {
            var_x += ((datax[i] - avg_x) * (datax[i] - avg_x));
            var_y += ((datay[i] - avg_y) * (datay[i] - avg_y));
            sumxy += ((datax[i] - avg_x) * (datay[i] - avg_y));
        }
        
        
        if(var_x==0||var_y==0) {    //方差为0，则相关系数不存在
            return -2;
        }else{r = sumxy / (Math.sqrt(var_x) * Math.sqrt(var_y));
        	
        }

        return r;
    }

    private static double getAvg(Double datax[]) {
        double avg = 0;
        for (int i = 0; i < datax.length; i++) {
            avg += datax[i];
        }
        return avg / datax.length;
    }
    
    
    public static void readFromHBaseToMysql(String date) {

    	HashMap<String, int[]> map = new HashMap<String, int[]>();
    	
    	//首先删除当天数据
    	CorrelationHelper.deleteData(date);

    	ResultScanner rsHBase = HBaseUtil.query(date, ConstUtil.IndicatorCorrelationTABLE);// 日统计数据查询

		//连接数据库
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs=null;
		conn = MySqlUtils.getConnection();
		
		String sql1 = "insert into "
				+ "s_correlation_current_d"
				+ "(id,monitor_id,correlation_current_10_a,correlation_current_11_a,correlation_current_12_a,correlation_current_13_a,correlation_current_14_a,correlation_current_15_a,correlation_current_16_a,correlation_current_17_a,correlation_current_18_a,correlation_current_19_a,correlation_current_2_a,correlation_current_20_a,correlation_current_21_a,correlation_current_22_a,correlation_current_23_a,correlation_current_24_a,correlation_current_25_a,correlation_current_26_a,correlation_current_27_a,correlation_current_28_a,correlation_current_29_a,correlation_current_3_a,correlation_current_30_a,correlation_current_31_a,correlation_current_32_a,correlation_current_33_a,correlation_current_34_a,correlation_current_35_a,correlation_current_36_a,correlation_current_37_a,correlation_current_38_a,correlation_current_39_a,correlation_current_4_a,correlation_current_40_a,correlation_current_41_a,correlation_current_42_a,correlation_current_43_a,correlation_current_44_a,correlation_current_45_a,correlation_current_46_a,correlation_current_47_a,correlation_current_48_a,correlation_current_49_a,correlation_current_5_a,correlation_current_50_a,correlation_current_6_a,correlation_current_7_a,correlation_current_8_a,correlation_current_9_a,"
				+ 				 "correlation_current_10_b,correlation_current_11_b,correlation_current_12_b,correlation_current_13_b,correlation_current_14_b,correlation_current_15_b,correlation_current_16_b,correlation_current_17_b,correlation_current_18_b,correlation_current_19_b,correlation_current_2_b,correlation_current_20_b,correlation_current_21_b,correlation_current_22_b,correlation_current_23_b,correlation_current_24_b,correlation_current_25_b,correlation_current_26_b,correlation_current_27_b,correlation_current_28_b,correlation_current_29_b,correlation_current_3_b,correlation_current_30_b,correlation_current_31_b,correlation_current_32_b,correlation_current_33_b,correlation_current_34_b,correlation_current_35_b,correlation_current_36_b,correlation_current_37_b,correlation_current_38_b,correlation_current_39_b,correlation_current_4_b,correlation_current_40_b,correlation_current_41_b,correlation_current_42_b,correlation_current_43_b,correlation_current_44_b,correlation_current_45_b,correlation_current_46_b,correlation_current_47_b,correlation_current_48_b,correlation_current_49_b,correlation_current_5_b,correlation_current_50_b,correlation_current_6_b,correlation_current_7_b,correlation_current_8_b,correlation_current_9_b,"
				+ 				 "correlation_current_10_c,correlation_current_11_c,correlation_current_12_c,correlation_current_13_c,correlation_current_14_c,correlation_current_15_c,correlation_current_16_c,correlation_current_17_c,correlation_current_18_c,correlation_current_19_c,correlation_current_2_c,correlation_current_20_c,correlation_current_21_c,correlation_current_22_c,correlation_current_23_c,correlation_current_24_c,correlation_current_25_c,correlation_current_26_c,correlation_current_27_c,correlation_current_28_c,correlation_current_29_c,correlation_current_3_c,correlation_current_30_c,correlation_current_31_c,correlation_current_32_c,correlation_current_33_c,correlation_current_34_c,correlation_current_35_c,correlation_current_36_c,correlation_current_37_c,correlation_current_38_c,correlation_current_39_c,correlation_current_4_c,correlation_current_40_c,correlation_current_41_c,correlation_current_42_c,correlation_current_43_c,correlation_current_44_c,correlation_current_45_c,correlation_current_46_c,correlation_current_47_c,correlation_current_48_c,correlation_current_49_c,correlation_current_5_c,correlation_current_50_c,correlation_current_6_c,correlation_current_7_c,correlation_current_8_c,correlation_current_9_c,"
				+ "correlation_negati_current,correlation_current_all,statistics_date) "
				+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
				+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
				+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
				+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
				+ "?,?,?,?,?,?,?,?,?)";
		
		String sql2 = "insert into "
				+ "s_correlation_voltage_d"
				+ "(id,monitor_id,correlation_voltage_10_a,correlation_voltage_11_a,correlation_voltage_12_a,correlation_voltage_13_a,correlation_voltage_14_a,correlation_voltage_15_a,correlation_voltage_16_a,correlation_voltage_17_a,correlation_voltage_18_a,correlation_voltage_19_a,correlation_voltage_2_a,correlation_voltage_20_a,correlation_voltage_21_a,correlation_voltage_22_a,correlation_voltage_23_a,correlation_voltage_24_a,correlation_voltage_25_a,correlation_voltage_26_a,correlation_voltage_27_a,correlation_voltage_28_a,correlation_voltage_29_a,correlation_voltage_3_a,correlation_voltage_30_a,correlation_voltage_31_a,correlation_voltage_32_a,correlation_voltage_33_a,correlation_voltage_34_a,correlation_voltage_35_a,correlation_voltage_36_a,correlation_voltage_37_a,correlation_voltage_38_a,correlation_voltage_39_a,correlation_voltage_4_a,correlation_voltage_40_a,correlation_voltage_41_a,correlation_voltage_42_a,correlation_voltage_43_a,correlation_voltage_44_a,correlation_voltage_45_a,correlation_voltage_46_a,correlation_voltage_47_a,correlation_voltage_48_a,correlation_voltage_49_a,correlation_voltage_5_a,correlation_voltage_50_a,correlation_voltage_6_a,correlation_voltage_7_a,correlation_voltage_8_a,correlation_voltage_9_a,"
				+ 				 "correlation_voltage_10_b,correlation_voltage_11_b,correlation_voltage_12_b,correlation_voltage_13_b,correlation_voltage_14_b,correlation_voltage_15_b,correlation_voltage_16_b,correlation_voltage_17_b,correlation_voltage_18_b,correlation_voltage_19_b,correlation_voltage_2_b,correlation_voltage_20_b,correlation_voltage_21_b,correlation_voltage_22_b,correlation_voltage_23_b,correlation_voltage_24_b,correlation_voltage_25_b,correlation_voltage_26_b,correlation_voltage_27_b,correlation_voltage_28_b,correlation_voltage_29_b,correlation_voltage_3_b,correlation_voltage_30_b,correlation_voltage_31_b,correlation_voltage_32_b,correlation_voltage_33_b,correlation_voltage_34_b,correlation_voltage_35_b,correlation_voltage_36_b,correlation_voltage_37_b,correlation_voltage_38_b,correlation_voltage_39_b,correlation_voltage_4_b,correlation_voltage_40_b,correlation_voltage_41_b,correlation_voltage_42_b,correlation_voltage_43_b,correlation_voltage_44_b,correlation_voltage_45_b,correlation_voltage_46_b,correlation_voltage_47_b,correlation_voltage_48_b,correlation_voltage_49_b,correlation_voltage_5_b,correlation_voltage_50_b,correlation_voltage_6_b,correlation_voltage_7_b,correlation_voltage_8_b,correlation_voltage_9_b,"
				+ 				 "correlation_voltage_10_c,correlation_voltage_11_c,correlation_voltage_12_c,correlation_voltage_13_c,correlation_voltage_14_c,correlation_voltage_15_c,correlation_voltage_16_c,correlation_voltage_17_c,correlation_voltage_18_c,correlation_voltage_19_c,correlation_voltage_2_c,correlation_voltage_20_c,correlation_voltage_21_c,correlation_voltage_22_c,correlation_voltage_23_c,correlation_voltage_24_c,correlation_voltage_25_c,correlation_voltage_26_c,correlation_voltage_27_c,correlation_voltage_28_c,correlation_voltage_29_c,correlation_voltage_3_c,correlation_voltage_30_c,correlation_voltage_31_c,correlation_voltage_32_c,correlation_voltage_33_c,correlation_voltage_34_c,correlation_voltage_35_c,correlation_voltage_36_c,correlation_voltage_37_c,correlation_voltage_38_c,correlation_voltage_39_c,correlation_voltage_4_c,correlation_voltage_40_c,correlation_voltage_41_c,correlation_voltage_42_c,correlation_voltage_43_c,correlation_voltage_44_c,correlation_voltage_45_c,correlation_voltage_46_c,correlation_voltage_47_c,correlation_voltage_48_c,correlation_voltage_49_c,correlation_voltage_5_c,correlation_voltage_50_c,correlation_voltage_6_c,correlation_voltage_7_c,correlation_voltage_8_c,correlation_voltage_9_c,"
				+ "threephase_voltage_negati_c,correlation_voltage_current_all,statistics_date) "
				+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
				+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
				+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
				+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
				+ "?,?,?,?,?,?,?,?,?)";
    		
		// 统计处理
		String correlation_current_all = "0";  //判断这一天是否为电流相关,null表示不存在，0表示不相关，1表示相关
		boolean correlation_current_all_exist = false;
		String correlation_voltage_current_all = "0";//判断这一天是否为电压相关
		boolean correlation_voltage_current_all_exist = false;
		try {	
			for (Result r : rsHBase) {
				String row = Bytes.toString(r.getRow());// 获取行键信息
				if (row.length() != 18) {
					continue;
				}
				String currentDate = row.substring(0, 8);
				String monitorID = row.substring(8, 18); // 监测点ID
				String id = monitorID + currentDate;
				currentDate = currentDate.substring(0, 4) + "-" + currentDate.substring(4, 6) + "-" + currentDate.substring(6, 8);
			
				// 取出A列族 A:a，查询P-I的各项值。
				String columnFamily1 = "A";
				NavigableMap<byte[], byte[]> aColumn = r.getFamilyMap(columnFamily1.getBytes());
				
			
				ps = conn.prepareStatement(sql1);
				ps.setString(1,id);
				// 为sql语句中第一个问号赋值
				ps.setString(2, monitorID);
				
				for(int j=3; j<=152; j++) {
					ps.setString(j, null);
				}

				int h1 = 3;
				for (Entry<byte[], byte[]> e : aColumn.entrySet()) {
					String column = new String(e.getKey());
					String phase = column.substring(0,1);
					String frequency = column.substring(1);
					String value = new String(e.getValue());
//					System.out.println(currentDate+",监测点"+monitorID+"的谐波电流与有功功率的相关系数：频次："+frequency+"相别："+phase+",相关系数："+value);
				
					String value1 = null;
					double a = Double.parseDouble(value);
					value1 = String.format("%.3f", a).toString();
				
					ps.setString(h1, value1);
					h1++;
				
					//判断当日是否为电流相关
					if(a >= -1 && a <= 1) { //求出來的系数有-2和 -1到1，只有当系数在 -1到1之间时，电流相关存在
						correlation_current_all_exist = true;
					}
					if((a <= -0.8 && a >= -1) || (a >= 0.8 && a <= 1)) {	
						correlation_current_all = "1";	//系数为 -1到-0.8，0.8到1之间时，电流相关。
					}
				}
			
				if(correlation_current_all_exist == true) {   //电流相关存在时，存入变量的值，不然就是不存在，存入null
					ps.setString(151,correlation_current_all);
				}else {
					ps.setString(151,null);
				}
			
				ps.setString(152, currentDate);
				ps.execute();

				System.out.println("PI数据插入完毕！！！");
			
				// 取出B列族 B:b，查询U-I的各项值。
				String columnFamily2 = "B";
				NavigableMap<byte[], byte[]> bColumn = r.getFamilyMap(columnFamily2.getBytes());
				
				ps = conn.prepareStatement(sql2);
				ps.setString(1,id);
				// 为sql语句中第一个问号赋值
				ps.setString(2, monitorID);
				
				for(int j=3; j<=152; j++) {
					ps.setString(j, null);
				}

				int h2 = 3;
				for (Entry<byte[], byte[]> e : bColumn.entrySet()) {
					String column = new String(e.getKey());
					String phase = column.substring(0,1);
					String frequency = column.substring(1);
					String value = new String(e.getValue());
//					String valueLevel = value.substring(5,6);
					
//					System.out.println(currentDate+",监测点"+monitorID+"的谐波电流与谐波电压的相关系数：频次："+frequency+"相别："+phase+",相关系数："+value);
					
					String value1 = null;
					double a = Double.parseDouble(value);
					value1 = String.format("%.3f", a).toString();
					
					ps.setString(h2, value1);
					h2++;
						
					//判断当日是否为电压相关
					if(a >= -1 && a <= 1) {
						correlation_voltage_current_all_exist = true;  //求出來的系数有-2和 -1到1，只有当系数在 -1到1之间时，电压相关存在
					}
					if((a <= -0.8 && a >= -1) || (a >= 0.8 && a <= 1)) {	
						correlation_voltage_current_all = "1"; //系数为 -1到-0.8，0.8到1之间时，电压相关。
					}	
				}
			
				if(correlation_current_all.equals("1")) {
					if(correlation_voltage_current_all_exist == true) {
						ps.setString(151, correlation_voltage_current_all);
					}else {
						ps.setString(151, null);
					}
				}else {
					ps.setString(151, null);
				}
			
				ps.setString(152, currentDate);
				ps.execute();
			
				System.out.println("UI数据插入完毕！！！");
			}
			
			HBaseUtil.closeCollection();
			
		} catch (SQLException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			MySqlUtils.close(rs,ps, conn);
		}
}
public static void deleteData(String date) {
    	
    	//在插入数据之前，先把要插入的这一天的数据删除
    	//连接数据库
		Connection conn = null;
		PreparedStatement ps = null;
		Object deleteRs = null;
		String deleteSql = null;
		conn = MySqlUtils.getConnection();
		
		
		String deleteTime = date.substring(0, 4) + "-" + date.substring(4, 6) + "-"+ date.substring(6, 8);
		System.out.println(deleteTime);
		deleteSql = "delete from s_correlation_current_d where statistics_date = ?";
		try {
			ps = conn.prepareStatement(deleteSql);
			ps.setString(1, deleteTime);
			deleteRs = ps.execute();
    	} catch (SQLException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		deleteSql = "delete from s_correlation_voltage_d where statistics_date = ?";
		try {
			ps = conn.prepareStatement(deleteSql);
			ps.setString(1, deleteTime);
			deleteRs = ps.execute();
    	} catch (SQLException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} finally {
			MySqlUtils.closeConnection();
		}
    			
    }
    
    public static void main(String[] args) {
    	readFromHBaseToMysql("20180711");
    }
}
