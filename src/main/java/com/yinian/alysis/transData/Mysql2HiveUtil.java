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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.log4j.Logger;
import com.yinian.alysis.exception.ExceptionHandle;
import com.yinian.alysis.mail.SendMail;
import com.yinian.alysis.tool.GenerateHiveView;
import com.yinian.alysis.tool.RemoteShellTool;
import com.yinian.alysis.transData.jdbc.HiveJdbc;
import com.yinian.alysis.transData.jdbc.MysqlJdbc;
import com.yinian.alysis.transData.jdbc.SyncInfoJdbc;
import com.yinian.alysis.util.PropertiesUtil;

public class Mysql2HiveUtil {
	private static Logger log = Logger.getLogger(Mysql2HiveUtil.class);
	
	private Mysql2HiveUtil(){};
	
	public static Mysql2HiveUtil newInstance(){
		return new Mysql2HiveUtil();
	}
	
	public static String SEPARATOR = "/";
	
	public static void generateCreateTableSqlFiles(String schema,String path) throws Exception {
		MysqlJdbc dbUtil =  MysqlJdbc.getInstance();
		StringBuilder sb = new StringBuilder();
		Map<String, List<Map<String,Object>>> rsMap = dbUtil.descTableTableStruct();
		String tableName = "";
		List<Map<String,Object>> listCol = null;
		for(String key : rsMap.keySet()) {
			tableName = key;//表名
			listCol = rsMap.get(key);//表字段信息
			sb.append(SyncCommUtil.gerateCreateTableStoreAsTextSql(tableName, listCol, schema)+"\n\n");
		}
		FileUtils.writeStringToFile(new File(path), sb.toString(), "UTF-8");
	}
	
	/**
	 * 所有表的hive创建sql
	 * @Title: generateCreateTableSqlList 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param schema
	 * @param @return
	 * @param @throws Exception    设定文件 
	 * @return List<String>    返回类型 
	 * @throws
	 */
	public static List<Map<String,String>> generateCreateTableSqlList(String schema) throws Exception {
		MysqlJdbc dbUtil = MysqlJdbc.getInstance();
		Map<String, List<Map<String,Object>>> rsMap = dbUtil.descTableTableStruct();
		String tableName = "";
		List<Map<String,Object>> listCol = null;
		List<Map<String,String>> rs = new ArrayList<Map<String,String>>();
		Map<String,String> sqlmap = null;
		for(String key : rsMap.keySet()) {
			tableName = key;//表名
			listCol = rsMap.get(key);//表字段信息
			sqlmap = new HashMap<String,String>();
			sqlmap.put("sql", SyncCommUtil.gerateCreateTableStoreAsTextSql(tableName, listCol, schema));
			sqlmap.put("table", tableName);
			rs.add(sqlmap);
			/*if(dbUtil.isContainUpdate(SyncCommUtil.trimTou(tableName))){
				rs.add(SyncCommUtil.gerateCreateTableStoreAsTextSql(SyncCommUtil.trimTou(tableName)+"_update", listCol, schema));//更新从表  约定记录更新记录的从表后缀在主表基础上加上_update
			}*/
		}
		return rs;
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
			rs.add(SyncCommUtil.gerateCreateTableStoreAsTextSql(tableName, listCol, schema));
		}
		return rs;
	}
	
	
	/**
	 * 全部表创建datax json配置文件
	 * @Title: generateDataxJsonCfg 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param dir
	 * @param @param schema
	 * @param @return
	 * @param @throws Exception    设定文件 
	 * @return List<Map<String,Object>>    返回类型 
	 * @throws
	 */
	public static List<Map<String,Object>> generateDataxJsonCfg(String dir,String schema,String[] partions) throws Exception{
		File dieF = FileUtils.getFile(dir);
		if(!dieF.exists()){
			FileUtils.forceMkdir(dieF);
		}
		List<Map<String,Object>> rsList = new ArrayList<>();
		String path = PropertiesUtil.getDataParam("datax.json.template");
		String temStr = FileUtils.readFileToString(new File(path), "UTF-8");
		MysqlJdbc dbUtil = MysqlJdbc.getInstance();
		HiveJdbc jdbc = HiveJdbc.getInstance();
		Map<String, List<Map<String,Object>>> rsMap = dbUtil.descTableTableStruct();
		try {
			String tableName = "";
			List<Map<String,Object>> listCol = null;
			Map<String,String> colMap = null;
			String jsonContent = "";//datax json配置内容
			String dataxJsonPath = "";
			Map<String,Object> tMap = null;
			
			//组装分区信息
			String partition = "day="+partions[0]+"/hour="+partions[1];
			for(String key : rsMap.keySet()) {
				tMap = new HashMap<>();
				tableName = key;//表名
				jdbc.excuteAddPartion(schema, tableName, partions);//创建分区
				listCol = rsMap.get(key);//表字段信息
				colMap = generateHiveColInfo(listCol);
				jsonContent = temStr;
				jsonContent = jsonContent.replaceAll("\\$\\{schema\\}", SyncCommUtil.trimTou(schema));
				jsonContent = jsonContent.replaceAll("\\$\\{table\\}", SyncCommUtil.trimTou(tableName));
				jsonContent = jsonContent.replaceAll("\\$\\{mysql_table\\}",tableName);
				jsonContent = jsonContent.replaceAll("\\$\\{mysql_cols\\}", colMap.get("mysql_cols"));
				jsonContent = jsonContent.replaceAll("\\$\\{hive_cols\\}", colMap.get("hive_cols"));
				jsonContent = jsonContent.replaceAll("\\$\\{condition\\}", "");//全量同步的
				jsonContent = jsonContent.replaceAll("\\$\\{partion\\}", partition);//分区
				dataxJsonPath = dir+SEPARATOR+SyncCommUtil.trimTou(tableName)+System.currentTimeMillis()+".json";
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
	public static List<Map<String,Object>> generateTableDataxJsonCfg(String dir,String schema,String tableName,String[] partions) throws Exception{
		File dieF = FileUtils.getFile(dir);
		if(!dieF.exists()){
			FileUtils.forceMkdir(dieF);
		}
		List<Map<String,Object>> rsList = new ArrayList<>();
		String path = PropertiesUtil.getDataParam("datax.json.template");
		String temStr = FileUtils.readFileToString(new File(path), "UTF-8");
		MysqlJdbc dbUtil = MysqlJdbc.getInstance();
		Map<String, List<Map<String,Object>>> rsMap = dbUtil.descTableStruct(tableName);
		try {
			List<Map<String,Object>> listCol = null;
			Map<String,String> colMap = null;
			String jsonContent = "";//datax json配置内容
			String dataxJsonPath = "";
			HiveJdbc jdbc = HiveJdbc.getInstance();
			String partition = "day="+partions[0]+"/hour="+partions[1];
			Map<String,Object> tMap = null;
			for(String key : rsMap.keySet()) {
				tMap = new HashMap<>();
				tableName = key;//表名
				listCol = rsMap.get(key);//表字段信息
				jdbc.excuteAddPartion(schema, tableName, partions);//创建分区
				colMap = generateHiveColInfo(listCol);
				jsonContent = temStr;
				jsonContent = jsonContent.replaceAll("\\$\\{schema\\}", SyncCommUtil.trimTou(schema));
				jsonContent = jsonContent.replaceAll("\\$\\{table\\}", SyncCommUtil.trimTou(tableName));
				jsonContent = jsonContent.replaceAll("\\$\\{mysql_table\\}", tableName);
				jsonContent = jsonContent.replaceAll("\\$\\{mysql_cols\\}", colMap.get("mysql_cols"));
				jsonContent = jsonContent.replaceAll("\\$\\{hive_cols\\}", colMap.get("hive_cols"));
				jsonContent = jsonContent.replaceAll("\\$\\{condition\\}", "");//全量同步的
				jsonContent = jsonContent.replaceAll("\\$\\{partion\\}", partition);//分区
				dataxJsonPath = dir+SEPARATOR+SyncCommUtil.trimTou(tableName)+System.currentTimeMillis()+".json";
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
	 * 单表增量同步
	 * @Title: generateTableIncreaseDataxJsonCfg 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param dir
	 * @param @param schema
	 * @param @param tableName
	 * @param @param syncTbInfo
	 * @param @return
	 * @param @throws Exception    设定文件 
	 * @return List<Map<String,Object>>    返回类型 
	 * @throws
	 */
	public static List<Map<String,Object>> generateTableIncreaseDataxJsonCfg(String dir,String schema,String tableName,Map<String,Object> syncTbInfo,String[] partions) throws Exception{
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
			String pkName = (String)syncTbInfo.get("pk_name");
			String lastMaxVal = (String)syncTbInfo.get("last_max_val");
			String condition = pkName+">"+lastMaxVal;
			for(String key : rsMap.keySet()) {
				tMap = new HashMap<>();
				tableName = key;//表名
				listCol = rsMap.get(key);//表字段信息
				jdbc.excuteAddPartion(schema, tableName, partions);//创建分区
				colMap = generateHiveColInfo(listCol);
				jsonContent = temStr;
				jsonContent = jsonContent.replaceAll("\\$\\{schema\\}", SyncCommUtil.trimTou(schema));
				jsonContent = jsonContent.replaceAll("\\$\\{table\\}", SyncCommUtil.trimTou(tableName));
				jsonContent = jsonContent.replaceAll("\\$\\{mysql_table\\}", tableName);
				jsonContent = jsonContent.replaceAll("\\$\\{mysql_cols\\}", colMap.get("mysql_cols"));
				jsonContent = jsonContent.replaceAll("\\$\\{hive_cols\\}", colMap.get("hive_cols"));
				jsonContent = jsonContent.replaceAll("\\$\\{condition\\}",condition);//全量同步的
				jsonContent = jsonContent.replaceAll("\\$\\{partion\\}", partition);//分区
				dataxJsonPath = dir+SEPARATOR+SyncCommUtil.trimTou(tableName)+System.currentTimeMillis()+".json";
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
	 * 将mysql表全量同步到hive上
	 * @Title: syncMysqlData2Hive 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param hiveIpAddr
	 * @param @param hiveUser
	 * @param @param hivePwd
	 * @param @param schema
	 * @param @throws Exception    设定文件 
	 * @return void    返回类型 
	 * @throws
	 */
	public void syncMysqlData2Hive(String schema,boolean all,String tabName) throws Exception{
		 RemoteShellTool tool = RemoteShellTool.getInstance();
    	 //生成的文件拷贝到linux服务器上
    	 List<Map<String,Object>> jsonFiles =  null;
    	 if(all){
    		 jsonFiles = Mysql2HiveUtil.generateDataxJsonCfg(PropertiesUtil.getDataParam("datax.local.path"), schema,SyncCommUtil.getNowDayAndHour());
    	 }else{
    		 jsonFiles = Mysql2HiveUtil.generateTableDataxJsonCfg(PropertiesUtil.getDataParam("datax.local.path"), schema,tabName,SyncCommUtil.getNowDayAndHour());
    	 }
    	 if(!new File(PropertiesUtil.getDataParam("datax.local.path")).exists()){
    		 FileUtils.forceMkdir(new File(PropertiesUtil.getDataParam("datax.local.path")));
    	 }
    	 String now = System.currentTimeMillis()+"";
    	 String dir = PropertiesUtil.getDataParam("datax.job.dir")+SEPARATOR+now;
    	 boolean exec = tool.exec("mkdir "+dir);
    	 if(exec){
    		 MysqlJdbc mysql = MysqlJdbc.getInstance();
    		 try {
				SyncInfoJdbc hiveSync = SyncInfoJdbc.getInstance();
				 for(Map<String,Object> json : jsonFiles){
					 String jsonP = dir+SEPARATOR+new File(String.valueOf(json.get("jobPath"))).getName();
					 String tableName = SyncCommUtil.trimTou((String)json.get("tableName"));
					 tool.transFile2Linux(String.valueOf(json.get("jobPath")), dir);
					 log.info("正在同步表："+tableName+"的数据到hive上。。。。。。。。。。。。。。。。。。");
					 tool.excuteDataxTransData(jsonP);
					 if(mysql.hasPk(tableName)){//如果是存在主键的话记录信息
						 Map<String,Object> valMap = mysql.selectPkIdMax(tableName);//同步的最大id记录
						 String recdIdMax = String.valueOf(valMap.get("val"));
						 if(StringUtils.isEmpty(recdIdMax) || StringUtils.equals("null", recdIdMax)){
								recdIdMax = "0";
						 }
						 String pkName = (String)valMap.get("pkName");
						 hiveSync.saveOrUpdateMaxRecd(pkName, recdIdMax, tableName,schema);//记录下最大值
					 }
					 if(mysql.isContainUpdate(tableName)) {//含有更新字段
						 hiveSync.saveOrUpdateUpdateSyncInfo("update_time", DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss"), tableName, schema);//创建更新从表
					 }
					 log.info("同步表："+tableName+"的数据到hive上完成。。。。。。。。。。。。。。。。。。");
				 }
				//最后删除掉中间的过程文件
				 tool.exec("rm -rf "+dir);
				 FileUtils.cleanDirectory(new File(PropertiesUtil.getDataParam("datax.local.path")));
			} catch (Exception e) {
				log.error(e);
				SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
			}
    	 }
	}
	
	/**
	 * 新表不创建只是记录下同步信息
	 * @param tableName
	 * @param schema
	 * @throws Exception
	 */
	public static void recordNewTablesSync(String tableName,String schema) throws Exception {
		MysqlJdbc mysql = MysqlJdbc.getInstance();
		SyncInfoJdbc hiveSync = SyncInfoJdbc.getInstance();
		if(mysql.isContainUpdate(tableName)) {
			hiveSync.saveOrUpdateUpdateSyncInfo("update_time", DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss"), tableName, schema);//创建更新从表
		}
		if(mysql.hasPk(tableName)){//如果是存在主键的话记录信息
			 String pkName = mysql.getTablePk(tableName);
			 hiveSync.saveOrUpdateMaxRecd(pkName, "0", tableName,schema);//记录下最大值0
		}
	}
	
	/**
	 * 单表增量导数
	 * @Title: syncMysqlData2Hive 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param schema
	 * @param @param tabName
	 * @param @param syncInfo
	 * @param @throws Exception    设定文件 
	 * @return void    返回类型 
	 * @throws
	 */
	public void syncMysqlIncreaseData2Hive(String schema,String tabName,Map<String,Object> syncInfo){
		RemoteShellTool tool = RemoteShellTool.getInstance();
		MysqlJdbc mysql = MysqlJdbc.getInstance();
		//生成的文件拷贝到linux服务器上
		List<Map<String,Object>> jsonFiles =  null;
		//判断是否是有主键表 有增量 无全量
		try {
			if(mysql.hasPk(tabName)){//如果有 增量
				jsonFiles = Mysql2HiveUtil.generateTableIncreaseDataxJsonCfg(PropertiesUtil.getDataParam("datax.local.path"), schema,tabName,syncInfo,SyncCommUtil.getNowDayAndHour());
			}else{//全量同步
				jsonFiles = Mysql2HiveUtil.generateTableDataxJsonCfg(PropertiesUtil.getDataParam("datax.local.path"), schema,tabName,SyncCommUtil.getNowDayAndHour());
			}
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
			for(Map<String, Object> json : jsonFiles){
				try {
					String jsonP = dir+SEPARATOR+new File(String.valueOf(json.get("jobPath"))).getName();
					String tableName = SyncCommUtil.trimTou((String)json.get("tableName"));
					tool.transFile2Linux(String.valueOf(json.get("jobPath")), dir);
					if(mysql.hasPk(tableName)){
						Map<String,Object> valMap = mysql.selectPkIdMax(tableName);//同步的最大id记录
						String recdIdMax = String.valueOf(valMap.get("val"));
						if(StringUtils.isEmpty(recdIdMax) || StringUtils.equals("null", recdIdMax)){
							recdIdMax = "0";
						}
						String pkName = (String)valMap.get("pkName");
						log.info("正在增量同步表："+tableName+"到hive上==================");
						tool.excuteDataxTransData(jsonP);
						log.info("增量同步到hive上结束==================");
						hiveSync.saveOrUpdateMaxRecd(pkName, recdIdMax, tableName,schema);//记录下最大值
					}else{//无主键  先删除再全量同步
						//将hive上表的数据删除掉
						tool.truncateHiveTableData(tabName,schema,SyncCommUtil.getPreDayAndHour());
						//将hive在hdfs上的垃圾清除掉
						tool.truncateHiveHdfsRubish(tableName,schema);
						//删掉hive表的上个分区数据
						HiveJdbc.getInstance().dropTablePartiton(schema, tabName, SyncCommUtil.getPreDayAndHour());
						log.info("正在增量同步表："+tableName+"到hive上==================");
						tool.excuteDataxTransData(jsonP);
						log.info("增量同步到hive上结束==================");
					}
				} catch (SQLException e) {
					log.error(e);
					SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
				} catch (Exception e) {
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
	 * 将mysql所有表创建到hive上
	 * @Title: createListAllMysqlTable2Hive 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param schema
	 * @param @throws Exception    设定文件 
	 * @return void    返回类型 
	 * @throws
	 */
	public void createListAllMysqlTable2Hive(String schema) throws Exception{
		List<Map<String,String>> list = Mysql2HiveUtil.generateCreateTableSqlList(schema);
		HiveJdbc dbh = HiveJdbc.getInstance();
		MysqlJdbc mysql = MysqlJdbc.getInstance();
		SyncInfoJdbc hiveSync = SyncInfoJdbc.getInstance();
		try {
			String sql = null;
			String tableName = null;
			 Map<String,Object> valMap = null;
			for(Map<String,String> sqMap : list){
				sql = sqMap.get("sql");
				tableName = sqMap.get("table");
				log.info(sql);
				dbh.createTable(sql);
				GenerateHiveView.gerateView(schema,tableName);
				if(mysql.hasPk(tableName)){//如果是存在主键的话记录信息
					 valMap = mysql.selectPkIdMax(tableName);
					 String pkName = (String)valMap.get("pkName");
					 hiveSync.saveOrUpdateMaxRecd(pkName, "0", tableName,schema);//记录下最大值
				 }
				 if(mysql.isContainUpdate(tableName)) {//含有更新字段
					 hiveSync.saveOrUpdateUpdateSyncInfo("update_time", DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss"), tableName, schema);//创建更新从表
				 }
			}
		} catch (Exception e) {
			log.error(e);
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
		}finally {
			dbh.releaseConn();
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
		List<String> list = Mysql2HiveUtil.generateCreateTableSql(schema,table);
		HiveJdbc dbh = HiveJdbc.getInstance();
		try {
			for(String sql : list){
				log.info(sql);
				dbh.createTable(sql);//建表
				GenerateHiveView.gerateView(schema,table);
			}
		} catch (Exception e) {
			log.error(e);
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
		}finally {
			dbh.releaseConn();
		}
	}
	
	/**
	 * 增量同步到hive
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
	public void addSynMysql2Hive(String schema){
		HiveJdbc hive = HiveJdbc.getInstance();
		//检测mysql上的所有表
		List<Map<String, Object>> allMysqlTables = null;
		try {
			allMysqlTables = MysqlJdbc.getInstance().findAllTables(schema);
		} catch (SQLException e1) {
			log.error(e1);
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e1), null);
		}
		Map<String,Object> syncTbInfo = null;
		for(Map<String, Object> tMap : allMysqlTables){
			try {
				String tbName = (String)tMap.get("TABLE_NAME");
				if(hive.checkTableExists(tbName,schema)){//如果存在
					syncTbInfo = SyncCommUtil.returnHiveTableSynInfo(tbName);
					//增量同步（有主键的增量去同步，无主键的先删除记录然后全量同步）
					this.syncMysqlIncreaseData2Hive(schema, tbName, syncTbInfo);
				}else{//如果不存在
					this.createMysqlTable2Hive(schema,tbName);//创建hive表
					this.syncMysqlData2Hive(schema,false,tbName);//全量同步
					recordNewTablesSync(tbName, schema);//记录同步信息
				}
			} catch (SQLException e) {
				log.error(e);
				SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
			} catch (Exception e) {
				log.error(e);
				SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
			}
		}
	}
	
//	public static void main(String[] args) throws Exception {
//		Mysql2HiveUtil util = new Mysql2HiveUtil();
//		SyncInfoJdbc jdbc = SyncInfoJdbc.getInstance();
//		try {
//			clear();
//			jdbc.truncateTables();
//			util.createListAllMysqlTable2Hive("yinian");
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		MysqlJdbc.getInstance().releaseConn();
//	}
//	
//	public static void clear() throws SQLException{
//		RemoteShellTool tool = RemoteShellTool.getInstance();
//		HiveJdbc hive = HiveJdbc.getInstance();
//		hive.excuteSql(" drop database yinian cascade");
//		tool.exec(" hadoop fs -rmr .Trash/Current/user/hive/warehouse/yinian.db");
//		hive.excuteSql(" create database yinian");
//	}
}
