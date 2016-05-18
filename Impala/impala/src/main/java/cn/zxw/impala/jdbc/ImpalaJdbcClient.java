package cn.zxw.impala.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

public class ImpalaJdbcClient {
 
	private static final String IMPALAD_HOST = "10.10.25.202";
	// port 21050 is the default impalad JDBC port 
	private static final String IMPALAD_JDBC_PORT = "21050";
 
	private static final String CONNECTION_URL = "jdbc:hive2://" + IMPALAD_HOST + ':' + IMPALAD_JDBC_PORT + "/;auth=noSasl";
 
	private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
	
	public int getCntBytags(List<String> list) {
		StringBuffer tags = new StringBuffer();
		Connection conn = null;
		try {
 
			Class.forName(JDBC_DRIVER_NAME);
			conn = DriverManager.getConnection(CONNECTION_URL);
			StringBuffer sb = new StringBuffer();
			sb.append("select count(1)  from ("
					+ " select COUNT(DISTINCT tag_value) c from dmp.game_tags_interest_aggr_1d "
					+ " where tag_value in (" + tags.toString() + ") "
					+ " group by cuid) tags where tags.c=" + 1);
		    PreparedStatement pstm = conn.prepareStatement(sb.toString());
		    //pstm.setString(1, tags);
		    long time_begin = System.currentTimeMillis();
	        ResultSet rs = pstm.executeQuery();
			if (rs.next()) {
				int cnt = rs.getInt(1);
				System.out.println(cnt);
				long time_end = System.currentTimeMillis();
				System.out.println("jdbc查询所用时间="+(time_end-time_begin));
				return cnt;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (Exception e) {
				// swallow
			}
		}
		return 0;
	}
}