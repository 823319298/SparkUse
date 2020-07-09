package decloud.powergrid.statictis.ml.common;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import decloud.powergrid.statictis.util.MySqlUtils;

/**
 * @author Felix
 * @version 1.0
 */
public class MappingInfo {

    /**
     * 频率新旧编码映射
     *
     * @return
     */
    public static Map<String, String> getFrequencyMapping() {
        Map<String, String> frequencyMap = new HashMap<String, String>();
        String sql = "select code,time from f_mapping_reverse_frequency";
        Connection conn = MySqlUtils.getConnection();
        PreparedStatement pst = null;
        ResultSet rs = null;
        try {
            pst = conn.prepareStatement(sql);
            rs = pst.executeQuery();
            while (rs.next()) {
    			frequencyMap.put(rs.getString(1), rs.getString(2));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally{
        	MySqlUtils.close(rs, pst, conn);
        }
        
        return frequencyMap;
    }

    /**
     * 指标新旧编码映射
     *
     * @return
     */
    public static Map<String, String> getIndicatorMapping() {
        Map<String, String> indicatorMap = new HashMap<String, String>();
        String sql = "select oldCode,newCode from f_mapping_indicator";
        Connection conn = MySqlUtils.getConnection();
        PreparedStatement pst = null;
        ResultSet resultSet = null;
        try {
            pst = conn.prepareStatement(sql);
            resultSet = pst.executeQuery();
            while (resultSet.next()) {
                indicatorMap.put(resultSet.getString("newCode"), resultSet.getString("oldCode"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        MySqlUtils.close(resultSet, pst, conn);
        return indicatorMap;
    }

    /**
     * 相别新旧编码映射
     *
     * @return
     */
    public static Map<String, List<String>> getPhaseMapping() {
        Map<String, List<String>> phaseMap = new HashMap<String, List<String>>();
        String sql = "select oldCode,newCode from f_mapping_phase";
        Connection conn = MySqlUtils.getConnection();
        PreparedStatement pst = null;
        ResultSet resultSet = null;
        try {
            pst = conn.prepareStatement(sql);
            resultSet = pst.executeQuery();
            while (resultSet.next()) {
                if (phaseMap.containsKey(resultSet.getString("newCode"))) {
                    List<String> list = phaseMap.get(resultSet.getString("newCode"));
                    list.add(resultSet.getString("oldCode"));
                    phaseMap.put(resultSet.getString("newCode"), list);
                } else {
                    List<String> list = new ArrayList<String>();
                    list.add(resultSet.getString("oldCode"));
                    phaseMap.put(resultSet.getString("newCode"), list);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        MySqlUtils.close(resultSet, pst,conn );
        return phaseMap;
    }

    /**
     * 值类型新旧编码映射
     *
     * @return
     */
    public static Map<String, List<String>> getValueTypeMapping() {
        Map<String, List<String>> valueTypeMap = new HashMap<String, List<String>>();
        String sql = "select oldCode,newCode from f_mapping_valtype";
        Connection conn = MySqlUtils.getConnection();
        PreparedStatement pst = null;
        ResultSet resultSet = null;
        try {
            pst = conn.prepareStatement(sql);
            resultSet = pst.executeQuery();
            while (resultSet.next()) {
                if (valueTypeMap.containsKey(resultSet.getString("newCode"))) {
                    List<String> list = valueTypeMap.get(resultSet.getString("newCode"));
                    list.add(resultSet.getString("oldCode"));
                    valueTypeMap.put(resultSet.getString("newCode"), list);
                } else {
                    List<String> list = new ArrayList<String>();
                    list.add(resultSet.getString("oldCode"));
                    valueTypeMap.put(resultSet.getString("newCode"), list);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        MySqlUtils.close(resultSet, pst, conn);
        return valueTypeMap;
    }

    /**
     * 监测点新旧编码映射
     *
     * @return
     */
    public static Map<String, String> getMonitorMapping() {
        Connection conn = MySqlUtils.getConnection();
        PreparedStatement pst = null;
        ResultSet resultSet = null;
        Map<String, String> monitorMap = new HashMap<String, String>();
        String sql = "select oldCode,newCode from f_mapping_monitor";
        try {
            pst = conn.prepareStatement(sql);
            resultSet = pst.executeQuery();
            while (resultSet.next()) {
                monitorMap.put(resultSet.getString("newCode"), resultSet.getString("oldCode"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        MySqlUtils.close(resultSet, pst,conn );
        return monitorMap;
    }
    
    public static Map<String,String> getIndicatorCorrelation(){
    	
    	 //只需要各次谐波电压Y13,Y23,Y33，各次谐波电流L13,L23,L33，各相有功功率H13,H23,H33,HT3，
        //负序电流bN3以及三相电压不平衡（负序电压不平衡）的均值QN3
    	Map <String,String> indicatorMap = new HashMap<String,String>();
        indicatorMap.put("Y13", "Y");
        indicatorMap.put("Y23", "Y");
        indicatorMap.put("Y33", "Y");
        indicatorMap.put("L13", "L");
        indicatorMap.put("L23", "L");
        indicatorMap.put("L33", "L");
        indicatorMap.put("H13", "H");
        indicatorMap.put("H23", "H");
        indicatorMap.put("H33", "H");
        indicatorMap.put("HT3", "H");
      
        indicatorMap.put("bN3", "b");         //负序电流没有相别，频率开头为2.

        indicatorMap.put("QN3", "Q");         //负序电压不平衡没有相别，频率开头为1.
    	return indicatorMap;
    }
}
