package decloud.powergrid.statictis.ml.correlation;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import decloud.powergrid.statictis.dailyStatistics.DailySNew;
import decloud.powergrid.statictis.sql.DataLog;
import decloud.powergrid.statictis.station.basic.MonitorIfRunJudge;
import decloud.powergrid.statictis.station.basic.MonitorTypeJudgeNew;
import decloud.powergrid.statictis.station.basic.TwoMonitorTypeJudge;
import decloud.powergrid.statictis.util.Config;
import decloud.powergrid.statictis.util.ConstUtil;
import decloud.powergrid.statictis.util.DateUtil;

public class CorrelationMain {
	
	public static String startTime = "";
	public static String endTime = "";  
	public static String statisticsMonth="";
	
    public static void main(String[] args) throws ClassNotFoundException, IOException, SQLException, InterruptedException {
    	
    	if(ConstUtil.GWFLAG.equals("2")){
			endTime = DateUtil.getToday();		
			startTime = DateUtil.getSpecifiedDayBefore(endTime);
			statisticsMonth = startTime.substring(0, 6);

		}else{				
			Properties prop = Config.getConfig();
			startTime = prop.getProperty("startTime");
			endTime = prop.getProperty("endTime");
			statisticsMonth = prop.getProperty("statisticsMonth");
		}
		if(startTime.equals("") || endTime.equals("")) {
			endTime = DateUtil.getToday();		
			startTime = DateUtil.getSpecifiedDayBefore(endTime);
		}
		
//		MonitorTypeJudgeNew typeJudge = new MonitorTypeJudgeNew();
//		MonitorIfRunJudge runJudge = new MonitorIfRunJudge();
		TwoMonitorTypeJudge typeJudge1 = new TwoMonitorTypeJudge();
		
		System.out.println("startTime : " + startTime);
		
//		typeJudge.monitorTypeJudge(startTime);
//		runJudge.monitorIfRunJudge(startTime);
		typeJudge1.monitorTypeJudge(startTime);
		
        IndicatorCorrelation correlation = new IndicatorCorrelation(startTime, "correlationAPP");
        
        correlation.invoke();  
        
        CorrelationHelper.readFromHBaseToMysql(startTime);
  
        //月统计
        MonthStatistics statistics = new MonthStatistics();
        
		statistics.monthStatistics(statisticsMonth);

    }
}
