package decloud.powergrid.statictis.ml.correlation;

import java.io.IOException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

import decloud.powergrid.statictis.after.impl.AfterRunningStatis;
import decloud.powergrid.statictis.after.main.Invoker;
import decloud.powergrid.statictis.alarm.AlarmStatistic;
import decloud.powergrid.statictis.dailyStatistics.DailyS;
import decloud.powergrid.statictis.dailyStatistics.DailySDianyaNew;
import decloud.powergrid.statictis.dailyStatistics.DailySNew;
import decloud.powergrid.statictis.indicator.SingleMonitorIndicatorToMysql_12;
import decloud.powergrid.statictis.indicator.SingleMonitorIndicatorToMysql_22;
import decloud.powergrid.statictis.indicatorHGL.MeasureHGL_2;
import decloud.powergrid.statictis.indicatorOver.HarmonicCurrentRatio;
import decloud.powergrid.statictis.indicatorOver.HarmonicCurrentRatioNew;
import decloud.powergrid.statictis.indicatorOver.HarmonicVoltageRatio;
import decloud.powergrid.statictis.indicatorOver.HarmonicVoltageRatioNew;
import decloud.powergrid.statictis.indicatorOver.IndicatorMYSQLWriter;
import decloud.powergrid.statictis.indicatorOver.IndicatorMYSQLWriterUpdate;
import decloud.powergrid.statictis.indicatorOver.NHarmonicExceed;
import decloud.powergrid.statictis.indicatorOver.insertRateToMysql;
import decloud.powergrid.statictis.runningStatus.AggregationRunningStatus;
import decloud.powergrid.statictis.runningStatus.AggregationRunningStatus_Copy;
import decloud.powergrid.statictis.runningStatus.NeedReceiveCount;
import decloud.powergrid.statictis.runningStatus.NeedReceiveCount_Copy;
import decloud.powergrid.statictis.runningStatus.RunningStatusMYSQLWriter;
import decloud.powergrid.statictis.runningStatus.RunningStatusMYSQLWriter_Copy;
import decloud.powergrid.statictis.sql.DataLog;
import decloud.powergrid.statictis.station.basic.MonitorTypeJudgeNew;
import decloud.powergrid.statictis.test.indicatorNum.IndicatorNumToMysql;
import decloud.powergrid.statictis.util.Config;
import decloud.powergrid.statictis.util.ConstUtil;
import decloud.powergrid.statictis.util.DateUtil;

public class CorrelationMain20191202 {

	public static String startTime = "";
	public static String endTime = "";    
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException, InterruptedException {
		
		Calendar start = Calendar.getInstance();  
	    start.set(2019, 11, 2);                     //startTime 20180101
	    Long startTIme = start.getTimeInMillis();  
	  
	    Calendar end = Calendar.getInstance();      //startTime 20180425
	    end.set(2019, 11, 3);  
	    Long endTimeLong = end.getTimeInMillis();  
	  
	    Long oneDay = 1000 * 60 * 60 * 24l;  
	  
	    Long time = startTIme;  
	    while (time <= endTimeLong) {  
	    	
	    	   Date d = new Date(time);  
		        DateFormat df = new SimpleDateFormat("yyyyMMdd");  
//		        System.out.println(df.format(d));  
		        String startTime = df.format(d); 
		        time += oneDay;  
		   
				String endTime = df.format(time);
	    	
	    	//========================write to log===========================
	    	
			
					
			System.out.println("startTime: " + startTime);
			System.out.println("endTime  : " + endTime);
			new DataLog().insertLog("99", "correlationStatisticStarted", startTime);
			MonitorTypeJudgeNew typeJudge = new MonitorTypeJudgeNew();
			
			System.out.println("startTime : " + startTime);
			
			typeJudge.monitorTypeJudge(startTime);
			
	        IndicatorCorrelation correlation = new IndicatorCorrelation(startTime, "correlationAPP");
	        
	        correlation.invoke();  
	        
	        CorrelationHelper.readFromHBaseToMysql(startTime);
			//========================write to log===========================
			new DataLog().insertLog("99", "correlatioStatisticFinished",startTime);
	    }


    }

}
