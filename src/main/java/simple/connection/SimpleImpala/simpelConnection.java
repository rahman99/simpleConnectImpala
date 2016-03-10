package simple.connection.SimpleImpala;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class simpelConnection {

	private static final String SQL_STATEMENT = "SELECT * FROM aaa limit 20";
	
	// set the impalad host
	private static final String IMPALAD_HOST = "localhost";
	
	// port 21050 is the default impalad JDBC port 
	private static final String IMPALAD_JDBC_PORT = "21050";

	private static final String CONNECTION_URL = "jdbc:hive2://" + IMPALAD_HOST + ':' + IMPALAD_JDBC_PORT + "/zz_testing;auth=noSasl";

	public static final String IMPALA_CONNECTION_URL = "jdbc:hive2://" + IMPALAD_HOST + ':' + IMPALAD_JDBC_PORT + "/zz_testing;auth=noSasl";
	
	private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
	
	public static void main(String[] args) {

		System.out.println("\n=============================================");
		System.out.println("Cloudera Impala JDBC Example");
		System.out.println("Using Connection URL: " + CONNECTION_URL);
		System.out.println("Running Query: " + SQL_STATEMENT);

		Connection con = null;

		try {

			Class.forName(JDBC_DRIVER_NAME);

			con = DriverManager.getConnection(IMPALA_CONNECTION_URL);
			Statement stmt = con.createStatement();
			
//			stmt.setPoolable(true);
			/*boolean res = stmt.execute("CREATE TABLE IF NOT EXISTS tmp_segment_tb (id STRING) STORED AS PARQUET");
			System.out.println("NUM: " + res);
			res = stmt.execute("INSERT OVERWRITE tmp_segment_tb select cookie_id FROM external_system_cookie_id");
			System.out.println("NUM: " + res);
			System.out.println("NUM: " + stmt.getWarnings());*/
//			stmt.executeUpdate("");
			
//			stmt.setPoolable(true);
			
			ResultSet rs = stmt.executeQuery(SQL_STATEMENT);

			System.out.println("\n== Begin Query Results ======================");

			// print the results to the console
			while (rs.next()) {
				// the example query returns one String column
				System.out.println(rs.getString(1));
			}

			System.out.println("== End Query Results =======================\n\n");

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				// swallow
			}
		}
	}
}
