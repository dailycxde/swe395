package tai.foundation.db;

import java.sql.*;
/**
 * Insert the type's description here.
 * Creation date: (06.11.2001 03:59:52)
 * @author: Yusuf Donmez
 */
public class EasyPreparedStatement{
	private int paramCount;
	PreparedStatement statement;
	
/**
 * SqlStatement constructor comment.
 */
public EasyPreparedStatement(Connection conn, String sqlStatement) throws SQLException{
	super();
	statement = conn.prepareStatement(sqlStatement);
}
/**
 * Insert the method's description here.
 * Creation date: (30.07.2002 18:09:01)
 * @param value byte
 */
public void addParameter(byte value) throws java.sql.SQLException{
	paramCount += 1;
	statement.setByte(paramCount,value);
}
/**
 * Insert the method's description here.
 * Creation date: (30.07.2002 18:19:38)
 * @param param double
 */
public void addParameter(double param) throws SQLException{
	paramCount += 1;
	statement.setDouble(paramCount,param);
}
/**
 * Insert the method's description here.
 * Creation date: (30.07.2002 18:20:09)
 * @param param float
 */
public void addParameter(float param) throws SQLException{
	paramCount += 1;
	statement.setFloat(paramCount,param);
}
/**
 * Insert the method's description here.
 * Creation date: (30.07.2002 18:21:05)
 * @param param int
 */
public void addParameter(int param) throws SQLException{
	paramCount += 1;
	statement.setInt(paramCount,param);
}
/**
 * Insert the method's description here.
 * Creation date: (30.07.2002 18:21:49)
 * @param param long
 */
public void addParameter(long param) throws SQLException {
	paramCount += 1;
	statement.setLong(paramCount,param);
}
/**
 * Insert the method's description here.
 * Creation date: (30.07.2002 18:23:01)
 * @param param java.lang.String
 */
public void addParameter(String param) throws SQLException {
	paramCount += 1;
	statement.setString(paramCount,param);
}
/**
 * Insert the method's description here.
 * Creation date: (30.07.2002 18:18:53)
 * @param param java.sql.Date
 */
public void addParameter(java.sql.Date param) throws SQLException{
	paramCount += 1;
	statement.setDate(paramCount,param);
}
/**
 * Insert the method's description here.
 * Creation date: (30.07.2002 18:23:50)
 * @param param java.sql.Time
 */
public void addParameter(java.sql.Time param) throws SQLException {
	paramCount += 1;
	statement.setTime(paramCount,param);
}
/**
 * Insert the method's description here.
 * Creation date: (30.07.2002 18:24:23)
 * @param param java.sql.Timestamp
 */
public void addParameter(java.sql.Timestamp param) throws SQLException {
	paramCount += 1;
	statement.setTimestamp(paramCount,param);
}
/**
 * Insert the method's description here.
 * Creation date: (30.07.2002 18:14:51)
 * @param value java.util.Date
 */
public void addParameter(java.util.Date value) throws SQLException{
	if (value==null) {
		paramCount += 1;
		statement.setDate(paramCount,null);
	} else {
		paramCount += 1;
		statement.setDate(paramCount,new java.sql.Date(value.getTime()));		
	}
}
/**
 * Insert the method's description here.
 * Creation date: (30.07.2002 18:22:22)
 * @param param short
 */
public void addParameter(short param) throws SQLException {
	paramCount += 1;
	statement.setShort(paramCount,param);
}
/**
 * Insert the method's description here.
 * Creation date: (30.07.2002 18:30:21)
 */
public void clearParameters() throws SQLException{
	statement.clearParameters();
	paramCount = 0;
}
/**
 * Insert the method's description here.
 * Creation date: (31.07.2002 18:19:08)
 * @return java.sql.PreparedStatement
 */
public PreparedStatement getPreparedStatement() {
	return statement;
}

/**
 * Insert the method's description here.
 * Creation date: (07.08.2002 15:12:52)
 * @exception java.sql.SQLException The exception description.
 */
public void close() throws java.sql.SQLException {
	statement.close();
}
}
