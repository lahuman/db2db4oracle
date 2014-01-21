package kr.pe.lahuman.dbcopy;

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

import oracle.jdbc.internal.OraclePreparedStatement;


public class DBCopy {

	/**
	 * 주의 대상 디비 백업 없이 삭제 합니다.
	 * 잘못써도 책임 못져요.
	 * @param args
	 */
	public static void main(String[] args)  {
	
		
		DBCopy dbcopy;
		try {
			dbcopy = new DBCopy();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return;
		}
		
		dbcopy.run();
	}

	
	private void run() {
		//원본 접속 정보
		String ourl = "jdbc:oracle:thin:@IP:PORT/SID";
		String oid = "ID";
		String opw = "PW";
		
		//타겟 접속 정보
		String turl = "jdbc:oracle:thin:@IP:PORT/SID";
		String tid = "ID";
		String tpw = "PW";
		
		
		//테이블 키워드
		String tableLikeKeyword = "TABLE_NAME";
		
		//최대 갯수
		int maxRownum = 30000;
		
		Connection original = null;
		Connection target = null;
		try {
			original = getConnection(ourl, oid, opw);
			target = getConnection(turl, tid, tpw);
			
			Map<String, List<String>> tableInfo = getTableInfo(original,  tableLikeKeyword);
			System.out.println(tableInfo);
			//TODO : target에 테이블 추가
			dbCreate(target, tableInfo);
			
			//TODO : insert
			dbCopy(original, target, tableInfo, maxRownum);
			
			
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			conClose(original);
			conClose(target);
			
		}		
	}


	private void conClose(Connection original) {
		if(original != null){
			try {
				original.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}


	private void dbCreate(Connection target, Map<String, List<String>> tableInfo) {
		PreparedStatement tps = null;
		
		for(String tableName : tableInfo.keySet()){
			String createSQL = " CREATE TABLE " + tableName + " ( ";
			try {
				boolean isFirst = true;
				for(String column : tableInfo.get(tableName)){
					if(!isFirst){
						createSQL +=", "; 
					}else{
						isFirst = false;
					}
					String[] columns = column.split("\\|");
					createSQL +=columns[0] + " "+columns[1];	
				}
				createSQL +=")"; 
				System.out.println(createSQL);
				//DROP
				tps = target.prepareStatement("DROP TABLE  "+tableName);
				tps.executeUpdate();
				psClose(tps);
				
				//CREATE
				tps = target.prepareStatement(createSQL);
				tps.executeUpdate();
			} catch (Exception e) {
				e.printStackTrace();
			}finally{
				psClose(tps);
			}
		}
	}




	private Map<String, List<String>> getTableInfo(Connection original, String tableLikeKeyword) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		Map<String, List<String>> tableInfo = new HashMap<String, List<String>>();
		
		try {
			ps = original.prepareStatement("SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH FROM ALL_TAB_COLUMNS WHERE TABLE_NAME LIKE ? ORDER BY TABLE_NAME, COLUMN_NAME");
			ps.setString(1, tableLikeKeyword);
			rs = ps.executeQuery();
			
			while(rs.next()){
				String tableName = rs.getString("TABLE_NAME");
				String columnName = rs.getString("COLUMN_NAME")+"|";
				String dataType = rs.getString("DATA_TYPE");
				String dataLength = rs.getString("DATA_LENGTH");
				if(!tableInfo.containsKey(tableName)){
					tableInfo.put(tableName, new ArrayList<String>());
				}
				columnName +=dataType;
				if(!"DATE".equals(dataType)){
					columnName +="("+dataLength+")";
				}
				tableInfo.get(tableName).add(columnName);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			rsClose(rs);
			psClose(ps);
		}
		
		return tableInfo;
	}


	private void psClose(PreparedStatement ps) {
		if(ps != null){
			try {
				ps.close();
				ps = null;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}


	private void rsClose(ResultSet rs) {
		if(rs != null){
			try {
				rs.close();
				rs = null;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}


	private void dbCopy(Connection original, Connection target,
			Map<String, List<String>> tableInfo, int maxRownum) {
		for(String tableName : tableInfo.keySet()){
			//SQL 만들기
			String insertSQLAfter = "INSERT INTO "+tableName + "( ";
			String insertSQLBefore = ") values ( ";
			String selectSQL = "SELECT ";
			
			boolean isFirst = true;
			for(String column : tableInfo.get(tableName)){
				column = column.split("\\|")[0];
				if(!isFirst){ 
					insertSQLAfter +=", ";
					selectSQL += ", ";
					insertSQLBefore += ", ";
				}else{
					isFirst = false;
				}
				insertSQLAfter += column;
				insertSQLBefore +="?";
				selectSQL += column;
			}
			insertSQLBefore +=")";
			insertSQLAfter += insertSQLBefore ;
			selectSQL += " FROM KCDC." +tableName + " WHERE ROWNUM < "+maxRownum;
			
			System.out.println("["+tableName+"] SQL : \""+selectSQL+"\"" );
			System.out.println("["+tableName+"] SQL : \""+insertSQLAfter+"\"" );
			
			//실제 insert를 위한 데이터 가져오기
			PreparedStatement ops = null;
			PreparedStatement tps = null;
			ResultSet ors = null;
			try {
				ops = original.prepareStatement(selectSQL);
				tps = target.prepareStatement(insertSQLAfter);
				ors = ops.executeQuery();
				while(ors.next()){
					ResultSetMetaData rsmd = ors.getMetaData();
					int columnsNumber = rsmd.getColumnCount();
					for(int i=0; i<columnsNumber; i++){
						//java.sql.Types
						switch (rsmd.getColumnType((i+1))) {
						case 2005:
							((OraclePreparedStatement)tps).setStringForClob((i+1), ors.getString((i+1)));
							break;

						default:
							tps.setObject((i+1), ors.getObject((i+1)));
							break;
						}
//						
					}
					tps.execute();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				rsClose(ors);
				psClose(ops);
				psClose(tps);
			}
			
			
		}
		
	}


	public DBCopy() throws ClassNotFoundException {
		Class.forName("oracle.jdbc.driver.OracleDriver");
	}
	private Connection getConnection(String url, String id, String pw) throws SQLException{
		return DriverManager.getConnection(url,id,pw);
	}
	
	
}
