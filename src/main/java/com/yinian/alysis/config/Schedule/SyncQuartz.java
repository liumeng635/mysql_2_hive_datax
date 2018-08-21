package com.yinian.alysis.config.Schedule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.yinian.alysis.tool.RemoteShellTool;
import com.yinian.alysis.transData.Mysql2HiveUtil;
import com.yinian.alysis.transData.MysqlUpdate2HiveUtil;
import com.yinian.alysis.transData.jdbc.HiveJdbc;
import com.yinian.alysis.transData.jdbc.MysqlJdbc;
import com.yinian.alysis.util.PropertiesUtil;

@Component
@Configurable
@EnableScheduling
@EnableAsync
public class SyncQuartz {
	private static final Logger LOGGER =  LoggerFactory.getLogger(SyncQuartz.class);
	
	@Async
  	@Scheduled(cron = "${quartz.scheduler.cron}") // 每小时执行一次
    public void work() throws Exception {
  		LOGGER.info("增量同步任务开始");
		Mysql2HiveUtil util2 = Mysql2HiveUtil.newInstance();
    	try {
    		LOGGER.info("开始执行");
			util2.addSynMysql2Hive(PropertiesUtil.getDataParam("bi.mysql.schema"));
			LOGGER.info("执行结束");
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.error("业务数据同步出现故障",e);
		}finally {
//			MysqlJdbc.getInstance().releaseConn();
//			HiveJdbc.getInstance().releaseConn();
//			RemoteShellTool.getInstance().releaseConn();
		}
    }
  	
	@Async
	@Scheduled(cron = "${quartz.scheduler.cron}") // 每小时执行一次
    public void work1() throws Exception { 
  		LOGGER.info("更新数据同步任务开始");
		MysqlUpdate2HiveUtil util = MysqlUpdate2HiveUtil.newInstance();
    	try {
    		LOGGER.info("开始执行");
    		util.addSynUpdateDataMysql2Hive(PropertiesUtil.getDataParam("bi.mysql.schema"));
    		LOGGER.info("执行结束");
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.error("业务数据同步出现故障",e);
		}finally {
//			MysqlJdbc.getInstance().releaseConn();
//			HiveJdbc.getInstance().releaseConn();
//			RemoteShellTool.getInstance().releaseConn();
		}
    }
}
