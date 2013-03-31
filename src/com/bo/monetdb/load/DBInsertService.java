/**
 * Function description:
 * Parse data that export from altibase to sql that meet monetdb.
 * Example:
 * Input data: 12345,"abcd,efgh",67890,
 * Output sql: insert into table_name(c1,c2,c3,c4) values(12345,'abcd,efgh',67890,NULL)
 * 
 * Further improvement: using java.sql.PreparedStatement
 * 
 */
package com.bo.monetdb.load;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DBInsertService {
	static String tableName = "";
	static String[] actColmunNames = null;
	static String fileName = "";
	static Connection conn = null;
	static Statement stmt = null;
	static String prefixSql = null;
	public static void dbInit() throws ClassNotFoundException, SQLException {
		if(conn==null) {
			Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
			conn = DriverManager.getConnection("jdbc:monetdb://localhost/demo", "voc", "voc");
			conn.setAutoCommit(false);
			stmt = conn.createStatement();
//			DatabaseMetaData md = conn.getMetaData();// 得到数据库的元数据              
//			System.out.println("supportBatchUpdate = " + md.supportsBatchUpdates());   //获取此数据库是否支持批量更新，并返回结果，支持则为true 

		}
		if(prefixSql==null) {
			prefixSql = "insert into " + tableName +" (";
			for(String columnName : actColmunNames) {
				prefixSql += columnName +",";
			}
			prefixSql = prefixSql.substring(0, prefixSql.length()-1) + ") values (";
		}
	}

	public static void fileToDb(String fileName) throws IOException {
		File file = new File(fileName);
		InputStream in = new FileInputStream(file);
		Reader reader = new InputStreamReader(in,Charset.forName("GBK"));
		try {
			boolean isDoubleQuotes = false;
			boolean isComma = false;
			boolean inDoubleQuotes = false;
			boolean isCommaSeparator = false;
			String word = "";
			String line = "";
			int _char = reader.read();
			while(_char!=-1) {
				if(_char == '\"' && inDoubleQuotes) {
					word += "\"";
				} else if(_char == '\\' && inDoubleQuotes) {
					word += "\\\\";
				} else {					
					word += (char)_char;
				}
				if(_char == ',' && !inDoubleQuotes) {
					isCommaSeparator = true;
				} else if(_char == '\"' && isComma) {
					inDoubleQuotes = true;
					isCommaSeparator = false;
				} else if(_char == ',' && isDoubleQuotes) {
					inDoubleQuotes = false;
					isCommaSeparator = true;
				} else {
					isCommaSeparator = false;
				}
				
				
				
				if(isCommaSeparator) {
					if(word.length() == 1) {
						word = "NULL,";
					}
					line += word;
					word = "";
				}
				if(_char == '\n') {
					if(word.length() == 1) {
						word = "NULL";
					}
					line += word.trim();
					line = line.replace("'", "\\'").replace('\"', '\'');
					insert(line); //replace with batch
//					insertBatch(line,false);
					line = "";
					word = "";
					isCommaSeparator = false;
				}
				
				if(_char==',') {
					isComma = true;
					isDoubleQuotes = false;
				} else if(_char=='\"') {
					isComma = false;
					isDoubleQuotes = true;
				} else {
					isComma = false;
					isDoubleQuotes = false;
				}
				_char = reader.read();
				
			}
			insertBatch(line,true);
		} finally {
			if(reader!=null) {
				reader.close();
			}
			try {
				if(stmt!=null) {
						stmt.close();
				}
				if(conn!=null) {
					conn.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	static void insert(String line) {
		String sql = prefixSql + line + ")";
//		System.out.println("sql = " + sql);
		try {
			if(insertCount>970000) {
				stmt.execute(sql);
				conn.commit();
			}
			insertCount++;
			if(insertCount>970000) {
				System.out.println("execute ok. " + insertCount);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println(sql);
			try {
				conn.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}
	
	
	static int insertCount = 0;
	static long st = System.currentTimeMillis();
	static void insertBatch(String line,boolean isLastTime) {
		String sql = prefixSql + line + ")";
		try {
			stmt.addBatch(sql);
			insertCount++;
			if(insertCount%5000==0 || isLastTime) {
				int[] batch = stmt.executeBatch();
				conn.commit();
				System.out.println("execute bache ok. " + insertCount + ", costs = " + (System.currentTimeMillis()-st) + "ms");
				st = System.currentTimeMillis();
				stmt.clearBatch();
			}
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println(sql);
			try {
				conn.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
	}

	/**
	 * @param args
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException {
//		tableName = "voc.TAI_NEGRP_DEFINE";
//		actColmunNames = new String[]{"GROUP_ID","INT_ID","OBJECT_CLASS","VENDOR_ID","TYPE","USERLABEL","PROVINCE_ID"};
//		fileName = "E:\\nosql\\filter\\data\\201301\\ACDB_TAI_NEGRP_DEFINE.dat";
		
		tableName = "voc.tfa_alarm_act";
		actColmunNames = new String[]{"ALARM_ID","REC_VERSION","FP0","FP1","FP2","FP3","C_FP0","C_FP1","C_FP2","C_FP3","EVENT_TIME","TIME_STAMP","CANCEL_TIME","ACTIVE_STATUS","ORG_SEVERITY","PROVINCE_NAME","REGION_ID","REGION_NAME","CITY_NAME","NETWORK_TYPE","ALARM_SOURCE","OBJECT_CLASS","PROFESSIONAL_TYPE","SHEET_NO","SHEET_STATUS","SHEET_SEND_STATUS","ORG_TYPE","LOGIC_ALARM_TYPE","LOGIC_SUB_ALARM_TYPE","SUB_ALARM_TYPE","VENDOR_TYPE","OMC_ID","OMC_ALARM_ID","VENDOR_ID","PROBABLE_CAUSE","PROBABLE_CAUSE_TXT","ALARM_ORIGIN","ACK_FLAG","ACK_USER","ACK_TIME","NE_LABEL","SITE_NO","NE_LOCATION","CIRCUIT_NO","CHANNEL_TYPE","EQP_LABEL","EQP_ALIAS","NE_ALIAS","NE_STATUS","EQP_INT_ID","NE_ADMIN_STATUS","ALARM_NE_STATUS","STANDARD_ALARM_NAME","STANDARD_ALARM_ID","EFFECT_NE","EFFECT_SERVICE","SEND_JT_FLAG","GCSS_CLIENT","GCSS_CLIENT_NAME","GCSS_CLIENT_LEVEL","GCSS_CLIENT_NUM","GCSS_SERVICE","GCSS_SERVICE_TYPE","GCSS_SERVICE_LEVEL","GCSS_SERVICE_NUM","DEADLINE_TIME","EQP_OBJECT_CLASS","EQP_VERSION","INT_ID","NE_IP","REMOTE_EQP_LABEL","REMOTE_RESOURCE_STATUS","REMOTE_PROJ_SUB_STATUS","PROJ_OA_FILE_ID","OBJECT_LEVEL","INFECT_CUSTOMER_COUNT","FAILED_REASON","UNSEND_REASON","LAYER_RETE","RELATION_TYPE","AC_RULE_NAME","ALARM_RESOURCE_STATUS","REMARK_EXIST","ALARM_REASON","IS_ROOT","HAS_CHILD","TITLE_TEXT","DEAL_TIME_LIMIT","SHEET_DEAL_DEP","SPECIAL_FIELD23","INSERT_TIME","ALARM_ACT_COUNT","RESOURCE_STATUS","CITY_ID","NMS_NAME","CIRCUIT_ID","ALARM_TITLE","REDEFINE_TYPE","REDEFINE_SEVERITY","VENDOR_SEVERITY","STANDARD_FLAG","EXTRA_ID1","EXTRA_STRING3","GCSS_CLIENT_GRADE","GCSS_CLIENT_GRADE_MGT","TMSC_CAT","PREPROCESS_MANNER","PREPROCESS_FLAG","EXTRA_ID3"};
		fileName = "E:\\nosql\\filter\\data\\201301\\ACDB_TFA_ALARM_ACT.dat";
		
//		tableName = "voc.tfa_alarm_act_txt";
//		actColmunNames = new String[]{"ALARM_ID","ALARM_TEXT","REC_VERSION"};
//		fileName = "E:\\nosql\\filter\\data\\201301\\ACDB_TFA_ALARM_ACT_TXT.dat";
		try {
			dbInit();
			fileToDb(fileName);
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
