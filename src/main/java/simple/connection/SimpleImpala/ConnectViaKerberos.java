package simple.connection.SimpleImpala;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public class ConnectViaKerberos {

	public static void main(String args[]) {
		try {
			System.out.println("set conf");
			Configuration conf = new Configuration();
			conf.set("hadoop.security.authentication", "Kerberos");
			System.out.println("set user group info");
			UserGroupInformation.setConfiguration(conf);
			UserGroupInformation.loginUserFromKeytab("rahman@TEST.COMPANY.CO.ID",
					"C:\\Users\\1207\\rahman.keytab");
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			System.out.println("getting connection");
			Connection con = DriverManager.getConnection(
					"jdbc:hive2://test4:10000/;principal=impala/rahman@TEST.COMPANY.CO.ID");
//			System.out.println("got connection "+con.getCatalog());
			con.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
