package tai.foundation.db;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.ResourceBundle;

public class DataSourceBroker {
	private static DataSourceBroker broker = null;
	private java.util.HashMap sources = null;

	public static java.sql.Connection getConnection(String dsName)
		throws Exception {
		boolean activeConnection = false;
		int count = 0; 
		java.sql.Statement st = null;
		java.sql.Connection conn = null;
		
		while (!activeConnection) {
			try {
				conn = getBroker().getDataSource(dsName).getConnection();
				st = conn.createStatement();
				activeConnection = true;			
				try {
					st.close();
					st = null;
				} catch (Exception e) {
					st = null;
				}
			} catch (com.ibm.websphere.ce.cm.StaleConnectionException e) {
				count++;
				if (count>3) {
					throw e;
				}
			}
		}
		return conn;
	}

	private javax.sql.DataSource getDataSource(String dsName) {
		return (javax.sql.DataSource) sources.get(dsName);
	}

	private static synchronized DataSourceBroker getBroker() throws Exception {
		if (broker == null) {
			broker = new DataSourceBroker();
		}
		return broker;
	}

	public DataSourceBroker() throws Exception {
		ResourceBundle rb = ResourceBundle.getBundle("dataSources");
		Enumeration dsNames = rb.getKeys();
		try {
			if (dsNames != null) {
				String dsName = null;
				String dsLookupString = null;
				javax.naming.InitialContext ctx = new javax.naming.InitialContext();
				sources = new HashMap();
				while (dsNames.hasMoreElements()) {
					dsName = (String) dsNames.nextElement();
					dsLookupString = rb.getString(dsName);
					Object obj = ctx.lookup(dsLookupString);
					javax.sql.DataSource ds = (javax.sql.DataSource) obj;
					sources.put(dsName, ds);
				}
			} else {
				System.err.println("NO DATA SOURCE FOUND");
				throw new Exception("NO DATA SOURCE FOUND");
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
			throw e;
		}
	}
}