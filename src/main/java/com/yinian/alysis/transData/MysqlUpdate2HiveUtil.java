package com.yinian.alysis.transData;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.log4j.Logger;

import com.yinian.alysis.exception.ExceptionHandle;
import com.yinian.alysis.mail.SendMail;
import com.yinian.alysis.tool.RemoteShellTool;
import com.yinian.alysis.transData.jdbc.HiveJdbc;
import com.yinian.alysis.transData.jdbc.MysqlJdbc;
import com.yinian.alysis.transData.jdbc.SyncInfoJdbc;
import com.yinian.alysis.util.PropertiesUtil;

public class MysqlUpdate2HiveUtil {
	private static Logger log = Logger.getLogger(MysqlUpdate2HiveUtil.class);
	
	private MysqlUpdate2HiveUtil(){}
	
	public static MysqlUpdate2HiveUtil newInstance(){
		return new MysqlUpdate2HiveUtil();
	}
	
	public static String SEPARATOR = "/";
	
	/**
	 * 单表创建datax json配置文件
	 * @Title: generateTableDataxJsonCfg 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param dir
	 * @param @param schema
	 * @param @return
	 * @param @throws Exception    设定文件 
	 * @return List<Map<String,Object>>    返回类型 
	 * @throws
	 */
	public static List<Map<String,Object>> generateTableDataxJsonCfg(String dir,String schema,String tableName,Map<String,Object> syncInfo,String now,String[] partions) throws Exception{
		File dieF = FileUtils.getFile(dir);
		if(!dieF.exists()){
			FileUtils.forceMkdir(dieF);
		}
		List<Map<String,Object>> rsList = new ArrayList<>();
		String path = PropertiesUtil.getDataParam("datax.json.template");
		String temStr = FileUtils.readFileToString(new File(path), "UTF-8");
		MysqlJdbc dbUtil = MysqlJdbc.getInstance();
		HiveJdbc jdbc = HiveJdbc.getInstance();
		Map<String, List<Map<String,Object>>> rsMap = dbUtil.descTableStruct(tableName);
		try {
			List<Map<String,Object>> listCol = null;
			Map<String,String> colMap = null;
			String jsonContent = "";//datax json配置内容
			String dataxJsonPath = "";
			Map<String,Object> tMap = null;
			String partition = "day="+partions[0]+"/hour="+partions[1];
			String colName = (String)syncInfo.get("basisc_col_name");
			String fetchTime = (String)syncInfo.get("last_fetch_time");
			String condition = colName + ">" + "'"+fetchTime+"' and "+colName+"<="+"'"+now+"' and create_time <> update_time";
			jdbc.excuteAddPartion(schema, tableName, partions);//创建分区
			
			for(String key : rsMap.keySet()) {
				tMap = new HashMap<>();
				tableName = key;//表名
				listCol = rsMap.get(key);//表字段信息
				colMap = generateHiveColInfo(listCol);
				jsonContent = temStr;
				jsonContent = jsonContent.replaceAll("\\$\\{schema\\}", SyncCommUtil.trimTou(schema));
				jsonContent = jsonContent.replaceAll("\\$\\{table\\}", SyncCommUtil.trimTou(tableName));
				jsonContent = jsonContent.replaceAll("\\$\\{mysql_table\\}", tableName);
				jsonContent = jsonContent.replaceAll("\\$\\{mysql_cols\\}", colMap.get("mysql_cols"));
				jsonContent = jsonContent.replaceAll("\\$\\{hive_cols\\}", colMap.get("hive_cols"));
				jsonContent = jsonContent.replaceAll("\\$\\{condition\\}",condition);//按照更新时间同步的
				jsonContent = jsonContent.replaceAll("\\$\\{partion\\}", partition);//分区
				dataxJsonPath = dir+SEPARATOR+SyncCommUtil.trimTou(tableName)+"_update"+System.currentTimeMillis()+".json";
				//生成JSON配置文件
				File file = FileUtils.getFile(dataxJsonPath);
				FileUtils.writeStringToFile(file, jsonContent, "UTF-8");
				tMap.put("tableName", tableName);
				tMap.put("jobPath", dataxJsonPath);
				rsList.add(tMap);
			}
		} catch (Exception e) {
			log.error(e);
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
		}
		return rsList;
	}
	
	
	/**
	 * 生成mysql对应字段和hive字段信息
	 * @Title: generateHiveColInfo 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param cols
	 * @param @return    设定文件 
	 * @return String    返回类型 
	 * @throws
	 */
	public static Map<String,String> generateHiveColInfo(List<Map<String, Object>> cols){
		StringBuilder sb1 = new StringBuilder();
		StringBuilder sb2 = new StringBuilder();
		Map<String,String> rsMap = new HashMap<>();
		for(Map<String, Object> map : cols){
			sb1.append("{\"name\":\""+SyncCommUtil.trimTou(map.get("code"))+"\",\"type\":\""+map.get("valueType")+"\"},");
			sb2.append("\""+SyncCommUtil.trimTou(map.get("code"))+"\",");
		}
		String rs1 = sb1.toString();
		rs1 = rs1.substring(0,rs1.lastIndexOf(","));//mysql字段信息
		String rs2 = sb2.toString();
		rs2 = rs2.substring(0,rs2.lastIndexOf(","));//hive字段信息
		rsMap.put("mysql_cols", rs2);
		rsMap.put("hive_cols", rs1);
		return rsMap;
	}
	
	
	/**
	 * 单表增量导入更新数据
	 * @Title: syncMysqlData2Hive 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param schema
	 * @param @param tabName
	 * @param @param syncInfo
	 * @param @throws Exception    设定文件 
	 * @return void    返回类型 
	 * @throws
	 */
	public void syncMysqlUpdateData2Hive(String schema,String tabName,Map<String,Object> syncInfo){
		RemoteShellTool tool = RemoteShellTool.getInstance();
		//生成的文件拷贝到linux服务器上
		List<Map<String,Object>> jsonFiles =  null;
		
		String nowTime = DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss");
		//增量同步
		try {
			jsonFiles = MysqlUpdate2HiveUtil.generateTableDataxJsonCfg(PropertiesUtil.getDataParam("datax.local.path"), schema,tabName,syncInfo,nowTime,SyncCommUtil.getNowDayAndHour());
			if(!new File(PropertiesUtil.getDataParam("datax.local.path")).exists()){
				FileUtils.forceMkdir(new File(PropertiesUtil.getDataParam("datax.local.path")));
			}
		} catch (IOException e) {
			log.error(e);
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
		} catch (Exception e) {
			log.error(e);
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
		}
		String now = System.currentTimeMillis()+"";
		String dir = PropertiesUtil.getDataParam("datax.job.dir")+SEPARATOR+now;
		boolean exec = tool.exec("mkdir "+dir);
		
		if(exec){
			SyncInfoJdbc hiveSync = SyncInfoJdbc.getInstance();
			String tableName = "";//主表
			String jsonP = "";
			for(Map<String, Object> json : jsonFiles){
				try {
					jsonP = dir+SEPARATOR+new File(String.valueOf(json.get("jobPath"))).getName();
					tableName = SyncCommUtil.trimTou((String)json.get("tableName"));
					tool.transFile2Linux(String.valueOf(json.get("jobPath")), dir);
					log.info("正在同步更新数据："+tableName+"到hive上==================");
					tool.excuteDataxTransData(jsonP);
					hiveSync.saveOrUpdateUpdateSyncInfo("update_time", nowTime, tableName, schema);
					log.info("同步更新数据到到hive上结束==================");
				} catch (SQLException e) {
					log.error(e);
					SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
				}
			}
			 //最后删除掉中间的过程文件
			 tool.exec("rm -rf "+dir);
			 try {
				FileUtils.cleanDirectory(new File(PropertiesUtil.getDataParam("datax.local.path")));
			} catch (IOException e) {
				log.error(e);
				SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
			}
		}
	}
	
	/**
	 * 增量同步更新数据到hive
	 * @Title: addSynMysql2Hive 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param hiveIpAddr
	 * @param @param hiveUser
	 * @param @param hivePwd
	 * @param @param schema
	 * @param @throws Exception    设定文件 
	 * @return void    返回类型 
	 * @throws
	 */
	public void addSynUpdateDataMysql2Hive(String schema){
		MysqlJdbc yinan = MysqlJdbc.getInstance();
		//检测mysql上的所有表
		List<Map<String, Object>> allMysqlTables = null;
		try {
			allMysqlTables = MysqlJdbc.getInstance().findAllTables(schema);
		} catch (SQLException e1) {
			log.error(e1);
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e1), null);
		}
		Map<String,Object> syncTbInfo = null;
		String tbName = "";
		for(Map<String, Object> tMap : allMysqlTables){
			tbName = (String)tMap.get("TABLE_NAME");
			try {
				if(!yinan.isContainUpdate(tbName) || !yinan.hasPk(tbName)){//不是含更新字段的表 或者是没有主键的表
					continue;
				}
			} catch (Exception e) {
				continue;
			}
			
			/*if(!hive.checkTableExists(tbName,schema)){//更新表如果不存在
				this.createMysqlTable2Hive(schema,tbName);//创建hive表
			}*/
			//同步数据
			try {
				syncTbInfo = SyncCommUtil.returnHiveTUpdateableSynInfo(tbName,schema);
				if(syncTbInfo == null){
					return;
				}
				this.syncMysqlUpdateData2Hive(schema, tbName,syncTbInfo);
			} catch (Exception e) {
				log.error(e);
				SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
			}
		}
	}
	
	
	/**
	 * mysql单表hive创建
	 * @Title: createMysqlTable2Hive 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param schema
	 * @param @param table
	 * @param @throws Exception    设定文件 
	 * @return void    返回类型 
	 * @throws
	 */
	public void createMysqlTable2Hive(String schema,String table) throws Exception{
		List<String> list = MysqlUpdate2HiveUtil.generateCreateTableSql(schema,table);
		HiveJdbc dbh = HiveJdbc.getInstance();
		try {
			for(String sql : list){
				log.info(sql);
				dbh.createTable(sql); 
			}
		} catch (Exception e) {
			log.error(e);
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
		}finally {
			dbh.releaseConn();
		}
	}
	
	/**
	 * 单表hive创建sql
	 * @Title: generateCreateTableSql 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param schema
	 * @param @param table
	 * @param @return
	 * @param @throws Exception    设定文件 
	 * @return List<String>    返回类型 
	 * @throws
	 */
	public static List<String> generateCreateTableSql(String schema,String table) throws Exception {
		MysqlJdbc dbUtil = MysqlJdbc.getInstance();
		Map<String, List<Map<String,Object>>> rsMap = dbUtil.descTableStruct(table);
		String tableName = "";
		List<Map<String,Object>> listCol = null;
		List<String> rs = new ArrayList<>();
		for(String key : rsMap.keySet()) {
			tableName = key;//表名
			listCol = rsMap.get(key);//表字段信息
			rs.add(SyncCommUtil.gerateCreateTableStoreAsTextSql(SyncCommUtil.trimTou(tableName), listCol, schema));
		}
		return rs;
	}
	
	public static void main(String[] args) throws Exception {
		MysqlUpdate2HiveUtil util = new MysqlUpdate2HiveUtil();
		util.addSynUpdateDataMysql2Hive("yinian");
		
		MysqlJdbc.getInstance().releaseConn();
	}
	
	public static void clear() throws SQLException{
		RemoteShellTool tool = RemoteShellTool.getInstance();
		HiveJdbc hive = HiveJdbc.getInstance();
		hive.excuteSql(" drop database yinian cascade");
		tool.exec(" hadoop fs -rmr .Trash/Current/user/hive/warehouse/yinian.db");
		hive.excuteSql(" create database yinian");
	}
}
