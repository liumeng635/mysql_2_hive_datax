package com.yinian.alysis.transData.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.yinian.alysis.transData.SyncCommUtil;
import com.yinian.alysis.util.PropertiesUtil;


public class SyncInfoJdbc {
	// 表示定义数据库的用户名  
    private static String USERNAME ;  
  
    // 定义数据库的密码  
    private static String PASSWORD;  
  
    // 定义数据库的驱动信息  
    private static String DRIVER;  
  
    // 定义访问数据库的地址  
    private static String URL;  
  
    // 定义sql语句的执行对象  
    private PreparedStatement pstmt;  
  
    // 定义查询返回的结果集合  
    private ResultSet resultSet;
    
    private static Connection connection = null;
    //单利模式 --懒汉式(双重锁定)保证线程的安全性
    public static SyncInfoJdbc db = null;
    
    //数据同步信息表
    private static final String SYNC_TABLE_NAME = "hive_sync_table_info";
    
    //数据更新信息表
    private static final String SYNC_UPDATE_TABLE_NAME = "hive_sync_for_update_table_info";
      
    static{  
        //加载数据库配置信息，并给相关的属性赋值  
        loadConfig();  
    }
    
    /** 
     * 加载数据库配置信息，并给相关的属性赋值 
     */  
    public static void loadConfig() {  
        try {  
        	URL  = PropertiesUtil.getDataParam("hive.mysql.sync.url");
        	USERNAME  = PropertiesUtil.getDataParam("hive.mysql.sync.username");
            DRIVER= PropertiesUtil.getDataParam("hive.mysql.sync.driver"); 
            PASSWORD  = PropertiesUtil.getDataParam("hive.mysql.sync.password");
        } catch (Exception e) {  
            throw new RuntimeException("读取数据库配置文件异常！", e);  
        }  
    }  
  
    private SyncInfoJdbc() {}
    
    public static SyncInfoJdbc getInstance(){
        if(db == null){
            synchronized(SyncInfoJdbc.class){
                if(db == null){
                	init();
                    db = new SyncInfoJdbc();
                }            
            }        
        }
        try {
			if(connection == null || connection.isClosed()){
				init();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
        return db;
    }
    
    
    private static void init(){
	 try {  
            Class.forName(DRIVER); // 注册驱动  
            connection = DriverManager.getConnection(URL, USERNAME, PASSWORD); // 获取连接  
        } catch (Exception e) {  
            throw new RuntimeException("get connection error!", e);  
        }  
    }
    
    /**
     * @throws SQLException 
     * 检测同步信息表是否存在不存在新建
     * @Title: checkSyncTableExists 
     * @Description: TODO(这里用一句话描述这个方法的作用) 
     * @param     设定文件 
     * @return void    返回类型 
     * @throws
     */
    public void checkSyncTableExists() throws SQLException{
    	String schema = URL.substring(URL.lastIndexOf("/")+1);
    	List<Map<String,Object>> rs = null;
    	rs = this.findResult("SELECT table_name FROM information_schema.TABLES WHERE table_name ='"+SYNC_TABLE_NAME+"' and table_schema = '"+schema+"'", null);
    	String sql = null;
    	if(rs == null || rs.isEmpty()){//没有建表 则新建
    		sql = "CREATE TABLE `"+SYNC_TABLE_NAME+"` (\n"
    			+ "`id` int(11) NOT NULL AUTO_INCREMENT,\n"
    			+ "`pk_name` varchar(128) DEFAULT NULL,\n"
    			+ "`last_max_val` varchar(64) DEFAULT NULL,\n"
    			+ "`table_name` varchar(255) DEFAULT NULL,\n"
    			+ "`schema_name` varchar(64) DEFAULT NULL COMMENT '所属schema',\n"
    			+ "`sync_time` datetime DEFAULT NULL COMMENT '同步时间',\n"
    			+ "PRIMARY KEY (`id`)\n"
    			+ ") ENGINE=InnoDB AUTO_INCREMENT=202 DEFAULT CHARSET=utf8";
    		this.exeSql(sql);
    	}
    	
    	rs = this.findResult("SELECT table_name FROM information_schema.TABLES WHERE table_name ='"+SYNC_UPDATE_TABLE_NAME+"' and table_schema = '"+schema+"'", null);
    	if(rs == null || rs.isEmpty()){//没有建表 则新建
    		sql = "CREATE TABLE `"+SYNC_UPDATE_TABLE_NAME+"` (\n"
    				+ "`id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',\n"
    				+ "`last_fetch_time` varchar(64) DEFAULT NULL COMMENT '最后一次获取时间',\n"
    				+ "`table_name` varchar(64) DEFAULT NULL COMMENT '表名',\n"
    				+ "`basisc_col_name` varchar(128) DEFAULT NULL COMMENT '同步依据原表的字段名',\n"
    				+ "`schema_name` varchar(64) DEFAULT NULL COMMENT '所属schema',\n"
    				+ "`sync_time` datetime DEFAULT NULL COMMENT '同步时间',\n"
    				+ "PRIMARY KEY (`id`)\n"
    				+ ") ENGINE=InnoDB AUTO_INCREMENT=133 DEFAULT CHARSET=utf8";
    		this.exeSql(sql);
    	}
    }
  
    /** 
     * 获取数据库连接 
     *  
     * @return 数据库连接 
     */  
    public Connection getConnection() {  
        return connection;  
    }  
  
    /** 
     * 执行更新操作 
     *  
     * @param sql 
     *            sql语句 
     * @param params 
     *            执行参数 
     * @return 执行结果 
     * @throws SQLException 
     */  
    public boolean updateByPreparedStatement(String sql, List<?> params)  
            throws SQLException {  
        boolean flag = false;  
        int result = -1;// 表示当用户执行添加删除和修改的时候所影响数据库的行数  
        pstmt = connection.prepareStatement(sql);  
        int index = 1;  
        // 填充sql语句中的占位符  
        if (params != null && !params.isEmpty()) {  
            for (int i = 0; i < params.size(); i++) {  
                pstmt.setObject(index++, params.get(i));  
            }  
        }  
        result = pstmt.executeUpdate();  
        flag = result > 0 ? true : false;  
        return flag;  
    }
    
  
    /** 
     * 执行查询操作 
     *  
     * @param sql 
     *            sql语句 
     * @param params 
     *            执行参数 
     * @return 
     * @throws SQLException 
     */  
    public List<Map<String, Object>> findResult(String sql, List<?> params)  
            throws SQLException {  
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();  
        int index = 1;  
        pstmt = connection.prepareStatement(sql);  
        if (params != null && !params.isEmpty()) {  
            for (int i = 0; i < params.size(); i++) {  
                pstmt.setObject(index++, params.get(i));  
            }  
        }  
        resultSet = pstmt.executeQuery();  
        ResultSetMetaData metaData = resultSet.getMetaData();  
        int cols_len = metaData.getColumnCount();  
        while (resultSet.next()) {  
            Map<String, Object> map = new HashMap<String, Object>();  
            for (int i = 0; i < cols_len; i++) {  
                String cols_name = metaData.getColumnName(i + 1);  
                Object cols_value = resultSet.getObject(cols_name);  
                if (cols_value == null) {  
                    cols_value = "";  
                }  
                map.put(cols_name, cols_value);  
            }  
            list.add(map);  
        }  
        return list;  
    }
    
    /**
     * 获取所有需要同步数据的表信息
     * @Title: listAllSynctTables 
     * @Description: TODO(这里用一句话描述这个方法的作用) 
     * @param @return
     * @param @throws SQLException    设定文件 
     * @return List<Map<String,Object>>    返回类型 
     * @throws
     */
    public List<Map<String,Object>> listAllSynctTables() throws SQLException{
    	String sql = "select * from "+SYNC_TABLE_NAME;
    	return this.findResult(sql, null);
    }
    
    /**
     * 插入记录
     * @Title: insertMaxRecd 
     * @Description: TODO(这里用一句话描述这个方法的作用) 
     * @param @param colName
     * @param @param max
     * @param @param table
     * @param @return
     * @param @throws SQLException    设定文件 
     * @return boolean    返回类型 
     * @throws
     */
    public boolean insertMaxRecd(String colName,String max,String table,String schema) throws SQLException{
    	table = SyncCommUtil.trimTou(table);
    	String sql = "insert into  "+SYNC_TABLE_NAME+"(pk_name,last_max_val,table_name,schema_name,sync_time) values('"+colName+"','"+max+"','"+table+"','"+schema+"',now())";
    	return this.updateByPreparedStatement(sql, null);
    }
    
    /**
     * 更新最大记录
     * @Title: updateMaxRecd 
     * @Description: TODO(这里用一句话描述这个方法的作用) 
     * @param @param colName
     * @param @param max
     * @param @param table
     * @param @return
     * @param @throws SQLException    设定文件 
     * @return boolean    返回类型 
     * @throws
     */
    public boolean updateMaxRecd(String colName,String max,String table,String schema) throws SQLException{
    	table = SyncCommUtil.trimTou(table);
    	String sql = "update  "+SYNC_TABLE_NAME+" set last_max_val = '"+max+"',sync_time=now() where table_name = '"+table+"' and pk_name = '"+colName+"' and schema_name = '"+schema+"'";
    	return this.updateByPreparedStatement(sql, null);
    }
    
    
    
    public boolean saveOrUpdateMaxRecd(String colName,String max,String table,String schema) throws SQLException{
    	table = SyncCommUtil.trimTou(table);
    	boolean is = false;
    	String sqlCheck = "select 1 from "+SYNC_TABLE_NAME+" where table_name = '"+table+"' and pk_name = '"+colName+"' and schema_name = '"+schema+"'";
    	List<Map<String,Object>> rlist =  this.findResult(sqlCheck, null);
    	if(rlist == null || rlist.isEmpty()){
    		this.insertMaxRecd(colName, max, table, schema);
    		is = true;
    	}else{
    		this.updateMaxRecd(colName, max, table, schema);
    	}
    	return is;
    }
    
    public boolean resetMaxRecd(String table,String schema) throws SQLException{
    	table = SyncCommUtil.trimTou(table);
    	String sql = "update  "+SYNC_TABLE_NAME+" set last_max_val = '"+0+"',sync_time=now() where table_name = '"+table+"' and schema_name = '"+schema+"'";
    	return this.updateByPreparedStatement(sql, null);
    }
    
    
    public List<Map<String,Object>> listAllSyncUpdateTables(String schema) throws SQLException{
    	String sql = "select * from "+SYNC_UPDATE_TABLE_NAME+" t where t.schema_name = '"+schema+"'";
    	return this.findResult(sql, null);
    }
    
    /**
     * @Title: insertUpdateSyncInfo 
     * @Description: update表的同步信息（插入）
     * @param @param colName
     * @param @param fetchTime
     * @param @param table
     * @param @return
     * @param @throws SQLException    设定文件 
     * @return boolean    返回类型 
     * @throws
     */
    public boolean insertUpdateSyncInfo(String colName,String fetchTime,String table,String schema) throws SQLException{
    	table = SyncCommUtil.trimTou(table);
    	String sql = "insert into  "+SYNC_UPDATE_TABLE_NAME+"(last_fetch_time,table_name,basisc_col_name,schema_name,sync_time) values('"+fetchTime+"','"+table+"','"+colName+"','"+schema+"',now())";
    	return this.updateByPreparedStatement(sql, null);
    }
    
    
    public boolean updateUpdateSyncInfo(String colName,String fetchTime,String table,String schema) throws SQLException{
    	table = SyncCommUtil.trimTou(table);
    	String sql = "update "+SYNC_UPDATE_TABLE_NAME+" set last_fetch_time = '"+fetchTime+"',sync_time=now() where basisc_col_name = '"+colName+"' and table_name = '"+table+"' and schema_name='"+schema+"' ";
    	return this.updateByPreparedStatement(sql, null);
    }
    
    
    /**
     * @Title: saveOrUpdateUpdateSyncInfo 
     * @Description: TODO(这里用一句话描述这个方法的作用) 
     * @param @param colName
     * @param @param fetchTime
     * @param @param table
     * @param @return
     * @param @throws SQLException    设定文件 
     * @return boolean    返回类型 
     * @throws
     */
    public boolean saveOrUpdateUpdateSyncInfo(String colName,String fetchTime,String table,String schema) throws SQLException{
    	table = SyncCommUtil.trimTou(table);
    	boolean is = false;
    	String sqlCheck = "select 1 from "+SYNC_UPDATE_TABLE_NAME+" where basisc_col_name = '"+colName+"' and table_name = '"+table+"' and schema_name='"+schema+"' ";
    	List<Map<String,Object>> rlist =  this.findResult(sqlCheck, null);
    	try {
			if(rlist == null || rlist.isEmpty()){
				this.insertUpdateSyncInfo(colName, fetchTime, table, schema);
			}else{
				this.updateUpdateSyncInfo(colName, fetchTime, table, schema);
			}
			is = true;
		} catch (Exception e) {
			is = false;
			e.printStackTrace();
		}
    	return is;
    }
    
    public void truncateTables() {
    	try {
			String sql = "truncate table "+SYNC_UPDATE_TABLE_NAME;
			this.exeSql(sql);
			sql = "truncate table "+SYNC_TABLE_NAME;
			this.exeSql(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    
    public boolean exeSql(String sql)  
            throws SQLException {  
        boolean flag = false;  
        int result = -1;// 表示当用户执行添加删除和修改的时候所影响数据库的行数  
        pstmt = connection.prepareStatement(sql);  
        result = pstmt.executeUpdate();  
        flag = result > 0 ? true : false;  
        return flag;  
    } 

    /** 
     * 释放资源 
     */  
    public void releaseConn() {  
        if (resultSet != null) {  
            try {  
                resultSet.close();  
            } catch (SQLException e) {  
                e.printStackTrace();  
            }  
        }  
        if (pstmt != null) {  
            try {  
                pstmt.close();  
            } catch (SQLException e) {  
                e.printStackTrace();  
            }  
        }  
        if (connection != null) {
            try {  
                connection.close();  
            } catch (SQLException e) {  
                e.printStackTrace();  
            }  
        }  
    }
}
