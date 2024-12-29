package tai.foundation.model;

import java.text.*;
/**
 * Insert the type's description here.
 * Creation date: (20.04.2002 03:45:13)
 * @author: Yusuf Donmez
 */
public class Formatter {
	private java.util.Hashtable formatters;
/**
 * Util constructor comment.
 */
public Formatter() {
	super();
	formatters = new java.util.Hashtable();
}
/**
 * Insert the method's description here.
 * Creation date: (20.04.2002 03:50:21)
 * @param name java.lang.String
 * @param formatter java.lang.Object
 */
public void addFormatter(String name, Object formatter) {
	formatters.put(name,formatter);
}
/**
 * Insert the method's description here.
 * Creation date: (20.04.2002 03:52:48)
 * @return java.lang.String
 * @param value double
 */
public String format(double value) {
	return ((NumberFormat)formatters.get("defaultNumberFormatter")).format(value);
}
/**
 * Insert the method's description here.
 * Creation date: (20.04.2002 03:49:32)
 * @return java.lang.String
 * @param formatterName java.lang.String
 * @param value double
 */
public String format(String formatterName, double value) {
	return ((NumberFormat)formatters.get(formatterName)).format(value);
}
/**
 * Insert the method's description here.
 * Creation date: (20.04.2002 03:45:53)
 * @return java.lang.String
 * @param aDate java.util.Date
 */
public String format(String formatterName,java.util.Date aDate) {
	if (aDate == null) {
		return "";
	} else {
		return ((SimpleDateFormat)formatters.get(formatterName)).format(aDate);		
	}
}
/**
 * Insert the method's description here.
 * Creation date: (20.04.2002 03:52:25)
 * @return java.lang.String
 * @param aDate java.util.Date
 */
public String format(java.util.Date aDate) {
	if (aDate==null) {
		return "";
	} else {
		return ((SimpleDateFormat)formatters.get("defaultDateFormatter")).format(aDate);
	}
}
/**
 * Insert the method's description here.
 * Creation date: (26.04.2002 02:50:12)
 * @return java.text.NumberFormat
 * @param name java.lang.String
 */
public NumberFormat getNumberFormatter() {
	return (NumberFormat)formatters.get("defaultNumberFormatter");
}
/**
 * Insert the method's description here.
 * Creation date: (26.04.2002 02:50:12)
 * @return java.text.NumberFormat
 * @param name java.lang.String
 */
public NumberFormat getNumberFormatter(String name) {
	return (NumberFormat)formatters.get(name);
}
/**
 * Insert the method's description here.
 * Creation date: (17.06.2002 08:21:57)
 * @return java.util.Date
 * @param dateString java.lang.String
 * @exception java.lang.Exception The exception description.
 */
public java.util.Date parseDate(String dateString) throws java.lang.Exception {
	try {
		return ((SimpleDateFormat)formatters.get("defaultDateFormatter")).parse(dateString);
	} catch (Exception ex) {
		return null;
	}
}
/**
 * Insert the method's description here.
 * Creation date: (20.04.2002 04:00:47)
 * @param formatter java.text.SimpleDateFormat
 */
public void setDefaultDateFormatter(java.text.SimpleDateFormat formatter) {
	formatters.put("defaultDateFormatter",formatter);
}
/**
 * Insert the method's description here.
 * Creation date: (20.04.2002 04:01:10)
 * @param formatter java.text.NumberFormat
 */
public void setDefaultNumberFormatter(java.text.NumberFormat formatter) {
	formatters.put("defaultNumberFormatter",formatter);
}

/**
 * Insert the method's description here.
 * Creation date: (20.04.2002 03:52:48)
 * @return java.lang.String
 * @param value double
 */
public String format(int value) {
	return ((NumberFormat)formatters.get("defaultNumberFormatter")).format(value);
}

/**
 * Insert the method's description here.
 * Creation date: (20.04.2002 03:52:48)
 * @return java.lang.String
 * @param value double
 */
public String format(long value) {
	return ((NumberFormat)formatters.get("defaultNumberFormatter")).format(value);
}
}
