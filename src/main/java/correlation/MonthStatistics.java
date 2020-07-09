package decloud.powergrid.statictis.ml.correlation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import decloud.powergrid.statictis.util.Config;
import decloud.powergrid.statictis.util.ConstUtil;
import decloud.powergrid.statictis.util.HBaseUtil;
import decloud.powergrid.statictis.util.MySqlUtils;

public class MonthStatistics {
	
	public static void monthStatistics(String statisticsMonth) {

    	HashMap<String, int[]> map = new HashMap<String, int[]>();
    	Set<String> monitorSet1 = new TreeSet<>();//用于存储monitorID
    	Set<String> monitorSet2 = new TreeSet<>();//用于存储monitorID

		//连接数据库
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs=null,rs1=null;
		conn = MySqlUtils.getConnection();
    	String deleteSql = null;
    	Boolean deleteRs = null;
		String sql = null;
    	String sql1 = null;
		String id = null;
		String monitorID = null;
    	String month_date = statisticsMonth.substring(0, 4) + "-" + statisticsMonth.substring(4, 6);
    	String startDay = month_date +"-01";
    	String endDay = month_date + "-31";
    	
    	
    	// P-I统计处理，日表s_correlation_current_d和月表s_correlation_current_days_m
    	
    	//先删掉表中所有该月的月统计数据，不然的话会出现id重复的异常，导致无法正常插入数据。
    	deleteSql = "DELETE FROM s_correlation_current_days_m WHERE month_date = ?";
    	try {
			ps = conn.prepareStatement(deleteSql);
			ps.setString(1, month_date);
			deleteRs = ps.execute();
    	} catch (SQLException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
    	
    	sql = "select monitor_id,"
    			+ "sum(CASE WHEN (correlation_current_2_a >= 0.8 OR (correlation_current_2_a <= -0.8 And correlation_current_2_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_2_b >= 0.8 OR (correlation_current_2_b <= -0.8 And correlation_current_2_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_2_c >= 0.8 OR (correlation_current_2_c <= -0.8 And correlation_current_2_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_3_a >= 0.8 OR (correlation_current_3_a <= -0.8 And correlation_current_3_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_3_b >= 0.8 OR (correlation_current_3_b <= -0.8 And correlation_current_3_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_3_c >= 0.8 OR (correlation_current_3_c <= -0.8 And correlation_current_3_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_4_a >= 0.8 OR (correlation_current_4_a <= -0.8 And correlation_current_4_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_4_b >= 0.8 OR (correlation_current_4_b <= -0.8 And correlation_current_4_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_4_c >= 0.8 OR (correlation_current_4_c <= -0.8 And correlation_current_4_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_5_a >= 0.8 OR (correlation_current_5_a <= -0.8 And correlation_current_5_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_5_b >= 0.8 OR (correlation_current_5_b <= -0.8 And correlation_current_5_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_5_c >= 0.8 OR (correlation_current_5_c <= -0.8 And correlation_current_5_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_6_a >= 0.8 OR (correlation_current_6_a <= -0.8 And correlation_current_6_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_6_b >= 0.8 OR (correlation_current_6_b <= -0.8 And correlation_current_6_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_6_c >= 0.8 OR (correlation_current_6_c <= -0.8 And correlation_current_6_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_7_a >= 0.8 OR (correlation_current_7_a <= -0.8 And correlation_current_7_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_7_b >= 0.8 OR (correlation_current_7_b <= -0.8 And correlation_current_7_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_7_c >= 0.8 OR (correlation_current_2_a <= -0.8 And correlation_current_2_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_8_a >= 0.8 OR (correlation_current_8_a <= -0.8 And correlation_current_8_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_8_b >= 0.8 OR (correlation_current_8_b <= -0.8 And correlation_current_8_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_8_c >= 0.8 OR (correlation_current_8_c <= -0.8 And correlation_current_8_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_9_a >= 0.8 OR (correlation_current_9_a <= -0.8 And correlation_current_9_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_9_b >= 0.8 OR (correlation_current_9_b <= -0.8 And correlation_current_9_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_9_c >= 0.8 OR (correlation_current_9_c <= -0.8 And correlation_current_9_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_10_a >= 0.8 OR (correlation_current_10_a <= -0.8 And correlation_current_10_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_10_b >= 0.8 OR (correlation_current_10_b <= -0.8 And correlation_current_10_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_10_c >= 0.8 OR (correlation_current_10_c <= -0.8 And correlation_current_10_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_11_a >= 0.8 OR (correlation_current_11_a <= -0.8 And correlation_current_11_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_11_b >= 0.8 OR (correlation_current_11_b <= -0.8 And correlation_current_11_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_11_c >= 0.8 OR (correlation_current_11_c <= -0.8 And correlation_current_11_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_12_a >= 0.8 OR (correlation_current_12_a <= -0.8 And correlation_current_12_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_12_b >= 0.8 OR (correlation_current_12_b <= -0.8 And correlation_current_12_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_12_c >= 0.8 OR (correlation_current_12_c <= -0.8 And correlation_current_12_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_13_a >= 0.8 OR (correlation_current_13_a <= -0.8 And correlation_current_13_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_13_b >= 0.8 OR (correlation_current_13_b <= -0.8 And correlation_current_13_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_13_c >= 0.8 OR (correlation_current_13_c <= -0.8 And correlation_current_13_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_14_a >= 0.8 OR (correlation_current_14_a <= -0.8 And correlation_current_14_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_14_b >= 0.8 OR (correlation_current_14_b <= -0.8 And correlation_current_14_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_14_c >= 0.8 OR (correlation_current_14_c <= -0.8 And correlation_current_14_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_15_a >= 0.8 OR (correlation_current_15_a <= -0.8 And correlation_current_15_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_15_b >= 0.8 OR (correlation_current_15_b <= -0.8 And correlation_current_15_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_15_c >= 0.8 OR (correlation_current_15_c <= -0.8 And correlation_current_15_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_16_a >= 0.8 OR (correlation_current_16_a <= -0.8 And correlation_current_16_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_16_b >= 0.8 OR (correlation_current_16_b <= -0.8 And correlation_current_16_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_16_c >= 0.8 OR (correlation_current_16_c <= -0.8 And correlation_current_16_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_17_a >= 0.8 OR (correlation_current_17_a <= -0.8 And correlation_current_17_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_17_b >= 0.8 OR (correlation_current_17_b <= -0.8 And correlation_current_17_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_17_c >= 0.8 OR (correlation_current_17_c <= -0.8 And correlation_current_17_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_18_a >= 0.8 OR (correlation_current_18_a <= -0.8 And correlation_current_18_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_18_b >= 0.8 OR (correlation_current_18_b <= -0.8 And correlation_current_18_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_18_c >= 0.8 OR (correlation_current_18_c <= -0.8 And correlation_current_18_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_19_a >= 0.8 OR (correlation_current_19_a <= -0.8 And correlation_current_19_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_19_b >= 0.8 OR (correlation_current_19_b <= -0.8 And correlation_current_19_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_19_c >= 0.8 OR (correlation_current_19_c <= -0.8 And correlation_current_19_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_20_a >= 0.8 OR (correlation_current_20_a <= -0.8 And correlation_current_20_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_20_b >= 0.8 OR (correlation_current_20_b <= -0.8 And correlation_current_20_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_20_c >= 0.8 OR (correlation_current_20_c <= -0.8 And correlation_current_20_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_21_a >= 0.8 OR (correlation_current_21_a <= -0.8 And correlation_current_21_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_21_b >= 0.8 OR (correlation_current_21_b <= -0.8 And correlation_current_21_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_21_c >= 0.8 OR (correlation_current_21_c <= -0.8 And correlation_current_21_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_22_a >= 0.8 OR (correlation_current_22_a <= -0.8 And correlation_current_22_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_22_b >= 0.8 OR (correlation_current_22_b <= -0.8 And correlation_current_22_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_22_c >= 0.8 OR (correlation_current_22_c <= -0.8 And correlation_current_22_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_23_a >= 0.8 OR (correlation_current_23_a <= -0.8 And correlation_current_23_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_23_b >= 0.8 OR (correlation_current_23_b <= -0.8 And correlation_current_23_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_23_c >= 0.8 OR (correlation_current_23_c <= -0.8 And correlation_current_23_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_24_a >= 0.8 OR (correlation_current_24_a <= -0.8 And correlation_current_24_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_24_b >= 0.8 OR (correlation_current_24_b <= -0.8 And correlation_current_24_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_24_c >= 0.8 OR (correlation_current_24_c <= -0.8 And correlation_current_24_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_25_a >= 0.8 OR (correlation_current_25_a <= -0.8 And correlation_current_25_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_25_b >= 0.8 OR (correlation_current_25_b <= -0.8 And correlation_current_25_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_25_c >= 0.8 OR (correlation_current_25_c <= -0.8 And correlation_current_25_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_26_a >= 0.8 OR (correlation_current_26_a <= -0.8 And correlation_current_26_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_26_b >= 0.8 OR (correlation_current_26_b <= -0.8 And correlation_current_26_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_26_c >= 0.8 OR (correlation_current_26_c <= -0.8 And correlation_current_26_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_27_a >= 0.8 OR (correlation_current_27_a <= -0.8 And correlation_current_27_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_27_b >= 0.8 OR (correlation_current_27_b <= -0.8 And correlation_current_27_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_27_c >= 0.8 OR (correlation_current_27_c <= -0.8 And correlation_current_27_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_28_a >= 0.8 OR (correlation_current_28_a <= -0.8 And correlation_current_28_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_28_b >= 0.8 OR (correlation_current_28_b <= -0.8 And correlation_current_28_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_28_c >= 0.8 OR (correlation_current_28_c <= -0.8 And correlation_current_28_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_29_a >= 0.8 OR (correlation_current_29_a <= -0.8 And correlation_current_29_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_29_b >= 0.8 OR (correlation_current_29_b <= -0.8 And correlation_current_29_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_29_c >= 0.8 OR (correlation_current_29_c <= -0.8 And correlation_current_29_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_30_a >= 0.8 OR (correlation_current_30_a <= -0.8 And correlation_current_30_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_30_b >= 0.8 OR (correlation_current_30_b <= -0.8 And correlation_current_30_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_30_c >= 0.8 OR (correlation_current_30_c <= -0.8 And correlation_current_30_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_31_a >= 0.8 OR (correlation_current_31_a <= -0.8 And correlation_current_31_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_31_b >= 0.8 OR (correlation_current_31_b <= -0.8 And correlation_current_31_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_31_c >= 0.8 OR (correlation_current_31_c <= -0.8 And correlation_current_31_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_32_a >= 0.8 OR (correlation_current_32_a <= -0.8 And correlation_current_32_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_32_b >= 0.8 OR (correlation_current_32_b <= -0.8 And correlation_current_32_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_32_c >= 0.8 OR (correlation_current_32_c <= -0.8 And correlation_current_32_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_33_a >= 0.8 OR (correlation_current_33_a <= -0.8 And correlation_current_33_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_33_b >= 0.8 OR (correlation_current_33_b <= -0.8 And correlation_current_33_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_33_c >= 0.8 OR (correlation_current_33_c <= -0.8 And correlation_current_33_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_34_a >= 0.8 OR (correlation_current_34_a <= -0.8 And correlation_current_34_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_34_b >= 0.8 OR (correlation_current_34_b <= -0.8 And correlation_current_34_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_34_c >= 0.8 OR (correlation_current_34_c <= -0.8 And correlation_current_34_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_35_a >= 0.8 OR (correlation_current_35_a <= -0.8 And correlation_current_35_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_35_b >= 0.8 OR (correlation_current_35_b <= -0.8 And correlation_current_35_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_35_c >= 0.8 OR (correlation_current_35_c <= -0.8 And correlation_current_35_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_36_a >= 0.8 OR (correlation_current_36_a <= -0.8 And correlation_current_36_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_36_b >= 0.8 OR (correlation_current_36_b <= -0.8 And correlation_current_36_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_36_c >= 0.8 OR (correlation_current_36_c <= -0.8 And correlation_current_36_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_37_a >= 0.8 OR (correlation_current_37_a <= -0.8 And correlation_current_37_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_37_b >= 0.8 OR (correlation_current_37_b <= -0.8 And correlation_current_37_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_37_c >= 0.8 OR (correlation_current_37_c <= -0.8 And correlation_current_37_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_38_a >= 0.8 OR (correlation_current_38_a <= -0.8 And correlation_current_38_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_38_b >= 0.8 OR (correlation_current_38_b <= -0.8 And correlation_current_38_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_38_c >= 0.8 OR (correlation_current_38_c <= -0.8 And correlation_current_38_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_39_a >= 0.8 OR (correlation_current_39_a <= -0.8 And correlation_current_39_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_39_b >= 0.8 OR (correlation_current_39_b <= -0.8 And correlation_current_39_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_39_c >= 0.8 OR (correlation_current_39_c <= -0.8 And correlation_current_39_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_40_a >= 0.8 OR (correlation_current_40_a <= -0.8 And correlation_current_40_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_40_b >= 0.8 OR (correlation_current_40_b <= -0.8 And correlation_current_40_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_40_c >= 0.8 OR (correlation_current_40_c <= -0.8 And correlation_current_40_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_41_a >= 0.8 OR (correlation_current_41_a <= -0.8 And correlation_current_41_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_41_b >= 0.8 OR (correlation_current_41_b <= -0.8 And correlation_current_41_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_41_c >= 0.8 OR (correlation_current_41_c <= -0.8 And correlation_current_41_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_42_a >= 0.8 OR (correlation_current_42_a <= -0.8 And correlation_current_42_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_42_b >= 0.8 OR (correlation_current_42_b <= -0.8 And correlation_current_42_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_42_c >= 0.8 OR (correlation_current_42_c <= -0.8 And correlation_current_42_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_43_a >= 0.8 OR (correlation_current_43_a <= -0.8 And correlation_current_43_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_43_b >= 0.8 OR (correlation_current_43_b <= -0.8 And correlation_current_43_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_43_c >= 0.8 OR (correlation_current_43_c <= -0.8 And correlation_current_43_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_44_a >= 0.8 OR (correlation_current_44_a <= -0.8 And correlation_current_44_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_44_b >= 0.8 OR (correlation_current_44_b <= -0.8 And correlation_current_44_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_44_c >= 0.8 OR (correlation_current_44_c <= -0.8 And correlation_current_44_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_45_a >= 0.8 OR (correlation_current_45_a <= -0.8 And correlation_current_45_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_45_b >= 0.8 OR (correlation_current_45_b <= -0.8 And correlation_current_45_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_45_c >= 0.8 OR (correlation_current_45_c <= -0.8 And correlation_current_45_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_46_a >= 0.8 OR (correlation_current_46_a <= -0.8 And correlation_current_46_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_46_b >= 0.8 OR (correlation_current_46_b <= -0.8 And correlation_current_46_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_46_c >= 0.8 OR (correlation_current_46_c <= -0.8 And correlation_current_46_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_47_a >= 0.8 OR (correlation_current_47_a <= -0.8 And correlation_current_47_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_47_b >= 0.8 OR (correlation_current_47_b <= -0.8 And correlation_current_47_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_47_c >= 0.8 OR (correlation_current_47_c <= -0.8 And correlation_current_47_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_48_a >= 0.8 OR (correlation_current_48_a <= -0.8 And correlation_current_48_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_48_b >= 0.8 OR (correlation_current_48_b <= -0.8 And correlation_current_48_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_48_c >= 0.8 OR (correlation_current_48_c <= -0.8 And correlation_current_48_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_49_a >= 0.8 OR (correlation_current_49_a <= -0.8 And correlation_current_49_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_49_b >= 0.8 OR (correlation_current_49_b <= -0.8 And correlation_current_49_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_49_c >= 0.8 OR (correlation_current_49_c <= -0.8 And correlation_current_49_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_50_a >= 0.8 OR (correlation_current_50_a <= -0.8 And correlation_current_50_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_50_b >= 0.8 OR (correlation_current_50_b <= -0.8 And correlation_current_50_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_50_c >= 0.8 OR (correlation_current_50_c <= -0.8 And correlation_current_50_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_negati_current >= 0.8 OR (correlation_negati_current <= -0.8 And correlation_negati_current >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_current_all >= 0.8 OR (correlation_current_all <= -0.8 And correlation_current_all >= -1)) THEN 1 ELSE 0 END) "
    			+ "FROM s_correlation_current_d "
    			+ "WHERE statistics_date BETWEEN ? AND ? "
    			+ "GROUP BY monitor_id";
    	
    	sql1 = "insert into s_correlation_current_days_m "
				+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
				+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
				+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
				+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
				+ "?,?,?,?,?,?,?,?,?)";
    	
    	try {
			ps = conn.prepareStatement(sql);
			ps.setString(1, startDay);
			ps.setString(2, endDay);
			rs = ps.executeQuery();
			while(rs.next()) {
				monitorID = rs.getString("monitor_id");
				id = monitorID + statisticsMonth;
				ps = conn.prepareStatement(sql1);
				ps.setString(1, id);
				ps.setString(2, monitorID);
				for(int i=3; i<=151; i++) {
					int j = i-1;
					ps.setString(i, rs.getString(j));
				}
				ps.setString(152, month_date);
				ps.execute();
			}
    	} catch (SQLException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
    	
    	
    	
    	// U-I统计处理，日表s_correlation_voltage_d和月表s_correlation_voltage_days_m
    	
    	//先删掉表中所有该月的月统计数据，不然的话会出现id重复的异常，导致无法正常插入数据。
    	deleteSql = "DELETE FROM s_correlation_voltage_days_m WHERE month_date = ?";
    	try {
			ps = conn.prepareStatement(deleteSql);
			ps.setString(1, month_date);
			deleteRs = ps.execute();
    	} catch (SQLException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
    	
    	sql = "select monitor_id,"
    			+ "sum(CASE WHEN (correlation_voltage_2_a >= 0.8 OR (correlation_voltage_2_a <= -0.8 And correlation_voltage_2_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_2_b >= 0.8 OR (correlation_voltage_2_b <= -0.8 And correlation_voltage_2_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_2_c >= 0.8 OR (correlation_voltage_2_c <= -0.8 And correlation_voltage_2_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_3_a >= 0.8 OR (correlation_voltage_3_a <= -0.8 And correlation_voltage_3_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_3_b >= 0.8 OR (correlation_voltage_3_b <= -0.8 And correlation_voltage_3_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_3_c >= 0.8 OR (correlation_voltage_3_c <= -0.8 And correlation_voltage_3_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_4_a >= 0.8 OR (correlation_voltage_4_a <= -0.8 And correlation_voltage_4_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_4_b >= 0.8 OR (correlation_voltage_4_b <= -0.8 And correlation_voltage_4_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_4_c >= 0.8 OR (correlation_voltage_4_c <= -0.8 And correlation_voltage_4_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_5_a >= 0.8 OR (correlation_voltage_5_a <= -0.8 And correlation_voltage_5_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_5_b >= 0.8 OR (correlation_voltage_5_b <= -0.8 And correlation_voltage_5_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_5_c >= 0.8 OR (correlation_voltage_5_c <= -0.8 And correlation_voltage_5_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_6_a >= 0.8 OR (correlation_voltage_6_a <= -0.8 And correlation_voltage_6_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_6_b >= 0.8 OR (correlation_voltage_6_b <= -0.8 And correlation_voltage_6_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_6_c >= 0.8 OR (correlation_voltage_6_c <= -0.8 And correlation_voltage_6_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_7_a >= 0.8 OR (correlation_voltage_7_a <= -0.8 And correlation_voltage_7_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_7_b >= 0.8 OR (correlation_voltage_7_b <= -0.8 And correlation_voltage_7_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_7_c >= 0.8 OR (correlation_voltage_2_a <= -0.8 And correlation_voltage_2_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_8_a >= 0.8 OR (correlation_voltage_8_a <= -0.8 And correlation_voltage_8_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_8_b >= 0.8 OR (correlation_voltage_8_b <= -0.8 And correlation_voltage_8_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_8_c >= 0.8 OR (correlation_voltage_8_c <= -0.8 And correlation_voltage_8_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_9_a >= 0.8 OR (correlation_voltage_9_a <= -0.8 And correlation_voltage_9_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_9_b >= 0.8 OR (correlation_voltage_9_b <= -0.8 And correlation_voltage_9_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_9_c >= 0.8 OR (correlation_voltage_9_c <= -0.8 And correlation_voltage_9_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_10_a >= 0.8 OR (correlation_voltage_10_a <= -0.8 And correlation_voltage_10_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_10_b >= 0.8 OR (correlation_voltage_10_b <= -0.8 And correlation_voltage_10_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_10_c >= 0.8 OR (correlation_voltage_10_c <= -0.8 And correlation_voltage_10_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_11_a >= 0.8 OR (correlation_voltage_11_a <= -0.8 And correlation_voltage_11_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_11_b >= 0.8 OR (correlation_voltage_11_b <= -0.8 And correlation_voltage_11_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_11_c >= 0.8 OR (correlation_voltage_11_c <= -0.8 And correlation_voltage_11_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_12_a >= 0.8 OR (correlation_voltage_12_a <= -0.8 And correlation_voltage_12_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_12_b >= 0.8 OR (correlation_voltage_12_b <= -0.8 And correlation_voltage_12_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_12_c >= 0.8 OR (correlation_voltage_12_c <= -0.8 And correlation_voltage_12_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_13_a >= 0.8 OR (correlation_voltage_13_a <= -0.8 And correlation_voltage_13_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_13_b >= 0.8 OR (correlation_voltage_13_b <= -0.8 And correlation_voltage_13_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_13_c >= 0.8 OR (correlation_voltage_13_c <= -0.8 And correlation_voltage_13_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_14_a >= 0.8 OR (correlation_voltage_14_a <= -0.8 And correlation_voltage_14_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_14_b >= 0.8 OR (correlation_voltage_14_b <= -0.8 And correlation_voltage_14_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_14_c >= 0.8 OR (correlation_voltage_14_c <= -0.8 And correlation_voltage_14_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_15_a >= 0.8 OR (correlation_voltage_15_a <= -0.8 And correlation_voltage_15_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_15_b >= 0.8 OR (correlation_voltage_15_b <= -0.8 And correlation_voltage_15_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_15_c >= 0.8 OR (correlation_voltage_15_c <= -0.8 And correlation_voltage_15_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_16_a >= 0.8 OR (correlation_voltage_16_a <= -0.8 And correlation_voltage_16_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_16_b >= 0.8 OR (correlation_voltage_16_b <= -0.8 And correlation_voltage_16_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_16_c >= 0.8 OR (correlation_voltage_16_c <= -0.8 And correlation_voltage_16_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_17_a >= 0.8 OR (correlation_voltage_17_a <= -0.8 And correlation_voltage_17_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_17_b >= 0.8 OR (correlation_voltage_17_b <= -0.8 And correlation_voltage_17_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_17_c >= 0.8 OR (correlation_voltage_17_c <= -0.8 And correlation_voltage_17_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_18_a >= 0.8 OR (correlation_voltage_18_a <= -0.8 And correlation_voltage_18_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_18_b >= 0.8 OR (correlation_voltage_18_b <= -0.8 And correlation_voltage_18_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_18_c >= 0.8 OR (correlation_voltage_18_c <= -0.8 And correlation_voltage_18_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_19_a >= 0.8 OR (correlation_voltage_19_a <= -0.8 And correlation_voltage_19_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_19_b >= 0.8 OR (correlation_voltage_19_b <= -0.8 And correlation_voltage_19_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_19_c >= 0.8 OR (correlation_voltage_19_c <= -0.8 And correlation_voltage_19_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_20_a >= 0.8 OR (correlation_voltage_20_a <= -0.8 And correlation_voltage_20_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_20_b >= 0.8 OR (correlation_voltage_20_b <= -0.8 And correlation_voltage_20_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_20_c >= 0.8 OR (correlation_voltage_20_c <= -0.8 And correlation_voltage_20_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_21_a >= 0.8 OR (correlation_voltage_21_a <= -0.8 And correlation_voltage_21_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_21_b >= 0.8 OR (correlation_voltage_21_b <= -0.8 And correlation_voltage_21_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_21_c >= 0.8 OR (correlation_voltage_21_c <= -0.8 And correlation_voltage_21_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_22_a >= 0.8 OR (correlation_voltage_22_a <= -0.8 And correlation_voltage_22_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_22_b >= 0.8 OR (correlation_voltage_22_b <= -0.8 And correlation_voltage_22_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_22_c >= 0.8 OR (correlation_voltage_22_c <= -0.8 And correlation_voltage_22_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_23_a >= 0.8 OR (correlation_voltage_23_a <= -0.8 And correlation_voltage_23_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_23_b >= 0.8 OR (correlation_voltage_23_b <= -0.8 And correlation_voltage_23_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_23_c >= 0.8 OR (correlation_voltage_23_c <= -0.8 And correlation_voltage_23_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_24_a >= 0.8 OR (correlation_voltage_24_a <= -0.8 And correlation_voltage_24_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_24_b >= 0.8 OR (correlation_voltage_24_b <= -0.8 And correlation_voltage_24_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_24_c >= 0.8 OR (correlation_voltage_24_c <= -0.8 And correlation_voltage_24_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_25_a >= 0.8 OR (correlation_voltage_25_a <= -0.8 And correlation_voltage_25_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_25_b >= 0.8 OR (correlation_voltage_25_b <= -0.8 And correlation_voltage_25_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_25_c >= 0.8 OR (correlation_voltage_25_c <= -0.8 And correlation_voltage_25_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_26_a >= 0.8 OR (correlation_voltage_26_a <= -0.8 And correlation_voltage_26_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_26_b >= 0.8 OR (correlation_voltage_26_b <= -0.8 And correlation_voltage_26_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_26_c >= 0.8 OR (correlation_voltage_26_c <= -0.8 And correlation_voltage_26_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_27_a >= 0.8 OR (correlation_voltage_27_a <= -0.8 And correlation_voltage_27_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_27_b >= 0.8 OR (correlation_voltage_27_b <= -0.8 And correlation_voltage_27_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_27_c >= 0.8 OR (correlation_voltage_27_c <= -0.8 And correlation_voltage_27_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_28_a >= 0.8 OR (correlation_voltage_28_a <= -0.8 And correlation_voltage_28_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_28_b >= 0.8 OR (correlation_voltage_28_b <= -0.8 And correlation_voltage_28_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_28_c >= 0.8 OR (correlation_voltage_28_c <= -0.8 And correlation_voltage_28_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_29_a >= 0.8 OR (correlation_voltage_29_a <= -0.8 And correlation_voltage_29_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_29_b >= 0.8 OR (correlation_voltage_29_b <= -0.8 And correlation_voltage_29_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_29_c >= 0.8 OR (correlation_voltage_29_c <= -0.8 And correlation_voltage_29_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_30_a >= 0.8 OR (correlation_voltage_30_a <= -0.8 And correlation_voltage_30_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_30_b >= 0.8 OR (correlation_voltage_30_b <= -0.8 And correlation_voltage_30_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_30_c >= 0.8 OR (correlation_voltage_30_c <= -0.8 And correlation_voltage_30_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_31_a >= 0.8 OR (correlation_voltage_31_a <= -0.8 And correlation_voltage_31_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_31_b >= 0.8 OR (correlation_voltage_31_b <= -0.8 And correlation_voltage_31_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_31_c >= 0.8 OR (correlation_voltage_31_c <= -0.8 And correlation_voltage_31_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_32_a >= 0.8 OR (correlation_voltage_32_a <= -0.8 And correlation_voltage_32_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_32_b >= 0.8 OR (correlation_voltage_32_b <= -0.8 And correlation_voltage_32_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_32_c >= 0.8 OR (correlation_voltage_32_c <= -0.8 And correlation_voltage_32_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_33_a >= 0.8 OR (correlation_voltage_33_a <= -0.8 And correlation_voltage_33_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_33_b >= 0.8 OR (correlation_voltage_33_b <= -0.8 And correlation_voltage_33_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_33_c >= 0.8 OR (correlation_voltage_33_c <= -0.8 And correlation_voltage_33_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_34_a >= 0.8 OR (correlation_voltage_34_a <= -0.8 And correlation_voltage_34_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_34_b >= 0.8 OR (correlation_voltage_34_b <= -0.8 And correlation_voltage_34_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_34_c >= 0.8 OR (correlation_voltage_34_c <= -0.8 And correlation_voltage_34_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_35_a >= 0.8 OR (correlation_voltage_35_a <= -0.8 And correlation_voltage_35_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_35_b >= 0.8 OR (correlation_voltage_35_b <= -0.8 And correlation_voltage_35_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_35_c >= 0.8 OR (correlation_voltage_35_c <= -0.8 And correlation_voltage_35_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_36_a >= 0.8 OR (correlation_voltage_36_a <= -0.8 And correlation_voltage_36_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_36_b >= 0.8 OR (correlation_voltage_36_b <= -0.8 And correlation_voltage_36_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_36_c >= 0.8 OR (correlation_voltage_36_c <= -0.8 And correlation_voltage_36_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_37_a >= 0.8 OR (correlation_voltage_37_a <= -0.8 And correlation_voltage_37_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_37_b >= 0.8 OR (correlation_voltage_37_b <= -0.8 And correlation_voltage_37_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_37_c >= 0.8 OR (correlation_voltage_37_c <= -0.8 And correlation_voltage_37_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_38_a >= 0.8 OR (correlation_voltage_38_a <= -0.8 And correlation_voltage_38_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_38_b >= 0.8 OR (correlation_voltage_38_b <= -0.8 And correlation_voltage_38_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_38_c >= 0.8 OR (correlation_voltage_38_c <= -0.8 And correlation_voltage_38_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_39_a >= 0.8 OR (correlation_voltage_39_a <= -0.8 And correlation_voltage_39_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_39_b >= 0.8 OR (correlation_voltage_39_b <= -0.8 And correlation_voltage_39_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_39_c >= 0.8 OR (correlation_voltage_39_c <= -0.8 And correlation_voltage_39_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_40_a >= 0.8 OR (correlation_voltage_40_a <= -0.8 And correlation_voltage_40_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_40_b >= 0.8 OR (correlation_voltage_40_b <= -0.8 And correlation_voltage_40_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_40_c >= 0.8 OR (correlation_voltage_40_c <= -0.8 And correlation_voltage_40_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_41_a >= 0.8 OR (correlation_voltage_41_a <= -0.8 And correlation_voltage_41_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_41_b >= 0.8 OR (correlation_voltage_41_b <= -0.8 And correlation_voltage_41_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_41_c >= 0.8 OR (correlation_voltage_41_c <= -0.8 And correlation_voltage_41_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_42_a >= 0.8 OR (correlation_voltage_42_a <= -0.8 And correlation_voltage_42_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_42_b >= 0.8 OR (correlation_voltage_42_b <= -0.8 And correlation_voltage_42_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_42_c >= 0.8 OR (correlation_voltage_42_c <= -0.8 And correlation_voltage_42_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_43_a >= 0.8 OR (correlation_voltage_43_a <= -0.8 And correlation_voltage_43_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_43_b >= 0.8 OR (correlation_voltage_43_b <= -0.8 And correlation_voltage_43_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_43_c >= 0.8 OR (correlation_voltage_43_c <= -0.8 And correlation_voltage_43_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_44_a >= 0.8 OR (correlation_voltage_44_a <= -0.8 And correlation_voltage_44_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_44_b >= 0.8 OR (correlation_voltage_44_b <= -0.8 And correlation_voltage_44_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_44_c >= 0.8 OR (correlation_voltage_44_c <= -0.8 And correlation_voltage_44_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_45_a >= 0.8 OR (correlation_voltage_45_a <= -0.8 And correlation_voltage_45_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_45_b >= 0.8 OR (correlation_voltage_45_b <= -0.8 And correlation_voltage_45_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_45_c >= 0.8 OR (correlation_voltage_45_c <= -0.8 And correlation_voltage_45_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_46_a >= 0.8 OR (correlation_voltage_46_a <= -0.8 And correlation_voltage_46_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_46_b >= 0.8 OR (correlation_voltage_46_b <= -0.8 And correlation_voltage_46_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_46_c >= 0.8 OR (correlation_voltage_46_c <= -0.8 And correlation_voltage_46_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_47_a >= 0.8 OR (correlation_voltage_47_a <= -0.8 And correlation_voltage_47_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_47_b >= 0.8 OR (correlation_voltage_47_b <= -0.8 And correlation_voltage_47_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_47_c >= 0.8 OR (correlation_voltage_47_c <= -0.8 And correlation_voltage_47_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_48_a >= 0.8 OR (correlation_voltage_48_a <= -0.8 And correlation_voltage_48_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_48_b >= 0.8 OR (correlation_voltage_48_b <= -0.8 And correlation_voltage_48_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_48_c >= 0.8 OR (correlation_voltage_48_c <= -0.8 And correlation_voltage_48_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_49_a >= 0.8 OR (correlation_voltage_49_a <= -0.8 And correlation_voltage_49_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_49_b >= 0.8 OR (correlation_voltage_49_b <= -0.8 And correlation_voltage_49_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_49_c >= 0.8 OR (correlation_voltage_49_c <= -0.8 And correlation_voltage_49_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_50_a >= 0.8 OR (correlation_voltage_50_a <= -0.8 And correlation_voltage_50_a >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_50_b >= 0.8 OR (correlation_voltage_50_b <= -0.8 And correlation_voltage_50_b >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_50_c >= 0.8 OR (correlation_voltage_50_c <= -0.8 And correlation_voltage_50_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (threephase_voltage_negati_c >= 0.8 OR (threephase_voltage_negati_c <= -0.8 And threephase_voltage_negati_c >= -1)) THEN 1 ELSE 0 END), "
    			+ "sum(CASE WHEN (correlation_voltage_current_all >= 0.8 OR (correlation_voltage_current_all <= -0.8 And correlation_voltage_current_all >= -1)) THEN 1 ELSE 0 END) "
    			+ "FROM s_correlation_voltage_d "
    			+ "WHERE statistics_date BETWEEN ? AND ? "
    			+ "GROUP BY monitor_id";
    	
    	sql1 = "insert into s_correlation_voltage_days_m "
				+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
				+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
				+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
				+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
				+ "?,?,?,?,?,?,?,?,?)";
    	
    	try {
			ps = conn.prepareStatement(sql);
			ps.setString(1, startDay);
			ps.setString(2, endDay);
			rs = ps.executeQuery();
			while(rs.next()) {
				monitorID = rs.getString("monitor_id");
				id = monitorID + statisticsMonth;
				ps = conn.prepareStatement(sql1);
				ps.setString(1, id);
				ps.setString(2, monitorID);
				for(int i=3; i<=151; i++) {
					int j = i-1;
					ps.setString(i, rs.getString(j));
				}
				ps.setString(152, month_date);
				ps.execute();
			}
    	} catch (SQLException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
    	
    	
    	System.out.println(statisticsMonth + "月份统计执行完毕");
		MySqlUtils.close(rs,ps, conn);
		
	} 
	
	public static void main(String[] args) {

		try{
			Properties prop = Config.getConfig();
			if(prop!=null){
				String statisticsMonth = prop.getProperty("statisticsMonth");
				MonthStatistics test =	new MonthStatistics();
				test.monthStatistics(statisticsMonth);
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}		
	}
}