package tai.foundation.db;

import java.sql.*;
/**
 * Insert the type's description here.
 * Creation date: (07.01.2002 02:05:49)
 * @author: Yusuf Donmez
 */
public class SqlQuery {
	private ResultSet resultSet;
	private Statement statement;
/**
 * SQLResult constructor comment.
 */
public SqlQuery(Connection conn, String sqlStatement) throws SQLException{
	super();
	statement = conn.createStatement();
	resultSet = statement.executeQuery(sqlStatement);
}
/**
 * SQLResult constructor comment.
 */
public SqlQuery(PreparedStatement st) throws SQLException{
	super();
	statement = st;
	resultSet = st.executeQuery();
}
/**
 * Insert the method's description here.
 * Creation date: (07.01.2002 02:09:53)
 * @exception java.sql.SQLException The exception description.
 */
public void close() {
	try {
		resultSet.close();
	} catch (SQLException ex) {
		resultSet = null;
	}
	try {
		statement.close();
	} catch (SQLException ex) {
		statement = null;
	}	
}
/**
 * Insert the method's description here.
 * Creation date: (31.07.2002 18:34:16)
 * @return java.util.Calendar
 * @param fieldName java.lang.String
 * @exception java.sql.SQLException The exception description.
 */
public java.util.Calendar getCalendar(String fieldName) throws java.sql.SQLException {
	java.sql.Date d = resultSet.getDate(fieldName);
	if (d==null) {
		return null;
	} else  {
		java.util.Calendar cal = java.util.Calendar.getInstance();
		cal.setTime(new java.util.Date(d.getTime()));
		return cal;
	}
}
/**
 * Insert the method's description here.
 * Creation date: (31.07.2002 18:28:01)
 * @return double
 * @param fieldName java.lang.String
 * @exception java.sql.SQLException The exception description.
 */
public double getDouble(String fieldName) throws java.sql.SQLException {
	return resultSet.getDouble(fieldName);
}
/**
 * Insert the method's description here.
 * Creation date: (31.07.2002 18:25:13)
 * @return float
 * @param fieldName java.lang.String
 * @exception java.sql.SQLException The exception description.
 */
public float getFloat(String fieldName) throws java.sql.SQLException {
	return resultSet.getFloat(fieldName);
}
/**
 * Insert the method's description here.
 * Creation date: (31.07.2002 18:24:27)
 * @return int
 * @param fieldName java.lang.String
 * @exception java.sql.SQLException The exception description.
 */
public int getInt(String fieldName) throws java.sql.SQLException {
	return resultSet.getInt(fieldName);
}
/**
 * Insert the method's description here.
 * Creation date: (31.07.2002 18:29:00)
 * @return long
 * @param fieldName java.lang.String
 * @exception java.sql.SQLException The exception description.
 */
public long getLong(String fieldName) throws java.sql.SQLException {
	return resultSet.getLong(fieldName);
}
/**
 * Insert the method's description here.
 * Creation date: (31.07.2002 18:31:27)
 * @return java.sql.Date
 * @param fieldName java.lang.String
 * @exception java.sql.SQLException The exception description.
 */
public java.sql.Date getSqlDate(String fieldName) throws java.sql.SQLException {
	return resultSet.getDate(fieldName);
}
/**
 * Insert the method's description here.
 * Creation date: (31.07.2002 18:29:37)
 * @return java.lang.String
 * @param fieldName java.lang.String
 * @exception java.sql.SQLException The exception description.
 */
public String getString(String fieldName) throws java.sql.SQLException {
	return resultSet.getString(fieldName);
}
/**
 * Insert the method's description here.
 * Creation date: (31.07.2002 18:30:56)
 * @return java.sql.Date
 * @param fieldName java.lang.String
 * @exception java.lang.Exception The exception description.
 */
public java.util.Date getUtilDate(String fieldName) throws java.lang.Exception {
	java.sql.Date d = resultSet.getDate(fieldName);
	if (d==null) {
		return null;
	} else  {
		return new java.util.Date(d.getTime());
	}
}
/**
 * Insert the method's description here.
 * Creation date: (07.01.2002 02:07:54)
 * @return boolean
 * @exception java.lang.Exception The exception description.
 */
public boolean next() throws java.lang.Exception {
    return resultSet.next();
}

/**
 * Insert the method's description here.
 * Creation date: (31.07.2002 18:34:16)
 * @return java.util.Calendar
 * @param fieldName java.lang.String
 * @exception java.sql.SQLException The exception description.
 */
public java.util.Calendar getCalendar(int fieldIndex) throws java.sql.SQLException {
	java.sql.Date d = resultSet.getDate(fieldIndex);
	if (d==null) {
		return null;
	} else  {
		java.util.Calendar cal = java.util.Calendar.getInstance();
		cal.setTime(new java.util.Date(d.getTime()));
		return cal;
	}
}

/**
 * Insert the method's description here.
 * Creation date: (31.07.2002 18:28:01)
 * @return double
 * @param fieldName java.lang.String
 * @exception java.sql.SQLException The exception description.
 */
public double getDouble(int fieldIndex) throws java.sql.SQLException {
	return resultSet.getDouble(fieldIndex);
}

/**
 * Insert the method's description here.
 * Creation date: (31.07.2002 18:25:13)
 * @return float
 * @param fieldName java.lang.String
 * @exception java.sql.SQLException The exception description.
 */
public float getFloat(int fieldIndex) throws java.sql.SQLException {
	return resultSet.getFloat(fieldIndex);
}

/**
 * Insert the method's description here.
 * Creation date: (31.07.2002 18:24:27)
 * @return int
 * @param fieldName java.lang.String
 * @exception java.sql.SQLException The exception description.
 */
public int getInt(int fieldIndex) throws java.sql.SQLException {
	return resultSet.getInt(fieldIndex);
}

/**
 * Insert the method's description here.
 * Creation date: (31.07.2002 18:29:00)
 * @return long
 * @param fieldName java.lang.String
 * @exception java.sql.SQLException The exception description.
 */
public long getLong(int fieldIndex) throws java.sql.SQLException {
	return resultSet.getLong(fieldIndex);
}

/**
 * Insert the method's description here.
 * Creation date: (31.07.2002 18:31:27)
 * @return java.sql.Date
 * @param fieldName java.lang.String
 * @exception java.sql.SQLException The exception description.
 */
public java.sql.Date getSqlDate(int fieldIndex) throws java.sql.SQLException {
	return resultSet.getDate(fieldIndex);
}

/**
 * Insert the method's description here.
 * Creation date: (31.07.2002 18:29:37)
 * @return java.lang.String
 * @param fieldName java.lang.String
 * @exception java.sql.SQLException The exception description.
 */
public String getString(int fieldIndex) throws java.sql.SQLException {
	return resultSet.getString(fieldIndex);
}

/**
 * Insert the method's description here.
 * Creation date: (31.07.2002 18:30:56)
 * @return java.sql.Date
 * @param fieldName java.lang.String
 * @exception java.lang.Exception The exception description.
 */
public java.util.Date getUtilDate(int fieldIndex) throws java.lang.Exception {
	java.sql.Date d = resultSet.getDate(fieldIndex);
	if (d==null) {
		return null;
	} else  {
		return new java.util.Date(d.getTime());
	}
}

/**
 * Insert the method's description here.
 * Creation date: (09.08.2002 14:40:21)
 * @return boolean
 */
public boolean wasNull() throws SQLException{
	return resultSet.wasNull();
}
}
