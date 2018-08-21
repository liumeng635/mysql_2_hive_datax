package com.yinian.alysis.config;

import java.sql.SQLException;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;

import com.yinian.alysis.transData.Mysql2HiveUtil;
import com.yinian.alysis.transData.MysqlUpdate2HiveUtil;
import com.yinian.alysis.transData.jdbc.HiveJdbc;
import com.yinian.alysis.transData.jdbc.SyncInfoJdbc;
import com.yinian.alysis.util.PropertiesUtil;

/** 
 * @ClassName: ApplicationEventListener 
 * @Description: TODO(这里用一句话描述这个类的作用) 
 * @author 刘猛
 * @date 2018年8月21日 下午4:11:03
 */
@Configuration
public class ApplicationEventListener implements ApplicationListener<ContextRefreshedEvent>{

	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		try {
			//检测同步信息记录表是否存在
			SyncInfoJdbc.getInstance().checkSyncTableExists();
			//检测对应的hive数据库是否是存在的，不存在新建
			HiveJdbc.getInstance().excuteSql("CREATE DATABASE IF NOT EXISTS "+PropertiesUtil.getDataParam("bi.mysql.schema"));
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
