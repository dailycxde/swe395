package tai.foundation.model;

/**
 * Insert the type's description here.
 * Creation date: (21.11.2001 09:45:27)
 * @author: Yusuf Donmez
 */

import java.util.Calendar;

public final class Utilities {
/**
 * DateConverter constructor comment.
 */
private Utilities() {
	super();
}
/**
 * Insert the method's description here.
 * Creation date: (21.11.2001 09:47:14)
 * @return java.util.Date
 * @param sqlDate java.sql.Date
 */
public final static java.util.Date convertDate(java.sql.Date sqlDate) {
	if (sqlDate != null) {
		java.util.Calendar cal = java.util.Calendar.getInstance();
		cal.setTime(sqlDate);
		cal.set(Calendar.HOUR, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		return new java.util.Date(cal.getTime().getTime());
	} else {
		return null;
	}
}
/**
 * Insert the method's description here.
 * Creation date: (21.11.2001 09:46:49)
 * @return java.sql.Date
 * @param utilDate java.util.Date
 */
public final static java.sql.Date convertDate(java.util.Date utilDate) {
	if (utilDate != null) {
		java.util.Calendar cal = java.util.Calendar.getInstance();
		cal.setTime(utilDate);
		cal.set(Calendar.HOUR, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		return new java.sql.Date(cal.getTime().getTime());
	} else {
		return null;
	}
}
/**
 * Insert the method's description here.
 * Creation date: (11/26/01 11:37:43)
 * @return java.sql.Time
 */
public final static java.sql.Time convertDateToTime(java.sql.Date sqlDate) {
	if (sqlDate != null) {
		java.util.Calendar cal = java.util.Calendar.getInstance();
		cal.setTime(sqlDate);
		cal.set(Calendar.HOUR, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		return new java.sql.Time(cal.getTime().getTime());
	} else {
		return null;
	}
}
/**
 * Insert the method's description here.
 * Creation date: (11/26/01 11:37:43)
 * @return java.sql.Time
 */
public final static java.sql.Time convertDateToTime(java.util.Date utilDate) {
	if (utilDate != null) {
		java.util.Calendar cal = java.util.Calendar.getInstance();
		cal.setTime(utilDate);
		cal.set(Calendar.HOUR, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		return new java.sql.Time(cal.getTime().getTime());
	} else {
		return null;
	}
}
/**
 * Insert the method's description here.
 * Creation date: (11/26/01 11:38:01)
 */
public final static java.sql.Timestamp convertDateToTimestamp(java.sql.Date sqlDate) {
	if (sqlDate != null) {
		java.util.Calendar cal = java.util.Calendar.getInstance();
		cal.setTime(sqlDate);
		cal.set(Calendar.HOUR, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		return new java.sql.Timestamp(cal.getTime().getTime());
	} else {
		return null;
	}
}
/**
 * Insert the method's description here.
 * Creation date: (11/26/01 11:38:01)
 */
public final static java.sql.Timestamp convertDateToTimestamp(java.util.Date utilDate) {
	if (utilDate != null) {
		java.util.Calendar cal = java.util.Calendar.getInstance();
		cal.setTime(utilDate);
		cal.set(Calendar.HOUR, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		return new java.sql.Timestamp(cal.getTime().getTime());
	} else {
		return null;
	}
}
}
