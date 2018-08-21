package com.yinian.alysis.tool;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import com.yinian.alysis.transData.SyncCommUtil;
import com.yinian.alysis.transData.jdbc.HiveJdbc;
import com.yinian.alysis.transData.jdbc.MysqlJdbc;

/** 
 * @ClassName: GenerateHiveView 
 * @Description: TODO(这里用一句话描述这个类的作用) 
 * @author 刘猛
 * @date 2018年6月12日 下午5:59:31
 */
public class GenerateHiveView {
	public static void gerateView(String schema) throws Exception{
		HiveJdbc jdbc = HiveJdbc.getInstance();
		MysqlJdbc yinian = MysqlJdbc.getInstance();
    	List<Map<String, Object>> maplist = jdbc.findResult("show tables", null);
    	String sql = "";
    	for(Map<String, Object> map : maplist){
    		String tabName = (String)map.get("tab_name");
    		if(!StringUtils.endsWith(tabName,"_view")){//&& !StringUtils.endsWith(tabName,"_update")
    			if(yinian.isContainUpdate(tabName) && yinian.hasPk(tabName)) {//如果有主键和update字段则创建视图
    				sql = generateViewSql(tabName, schema,yinian.getTablePk(tabName));
        			jdbc.excuteSql(sql);
    			}
    		}
    	}
	}
	
	/**
	 * 单表创建视图
	 * @param schema
	 * @param tabName
	 * @throws Exception
	 */
	public static void gerateView(String schema,String tabName) throws Exception{
		HiveJdbc jdbc = HiveJdbc.getInstance();
		MysqlJdbc yinian = MysqlJdbc.getInstance();
		String sql = "";
			if(!StringUtils.endsWith(tabName,"_view")){//&& !StringUtils.endsWith(tabName,"_update")
				if(yinian.isContainUpdate(tabName) && yinian.hasPk(tabName)) {//如果有主键和update字段则创建视图
					sql = generateViewSql(tabName, schema,yinian.getTablePk(tabName));
					jdbc.excuteSql(sql);
			}
		}
	}
	
	/**
	 * @Title: generateViewSql 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param tableName
	 * @param @return    设定文件 
	 * @return String    返回类型 
	 * @throws
	 */
	public static String generateViewSql(String tableName,String schema,String pkName){
		tableName = SyncCommUtil.trimTou(tableName);
		String tabName = schema+".`"+tableName+"`";
		String viewTabName = schema+"."+tableName+"_view";
		return "CREATE VIEW IF NOT EXISTS "+viewTabName+" as  select t1.* from "+tabName+" t1, (select "+pkName+",max(update_time) as update_time from "+tabName+" group by "+pkName+") t2 where t1."+pkName+" = t2."+pkName+" and t1.update_time = t2.update_time";
	}
}
