package tai.foundation.model;

/**
 * tai 2000 copyright
 * Creation date: (3/26/01 2:45:41 PM)
 * @author: Yusuf Donmez
 */
public class UserException extends Exception {
	private int type;
	public final static int ERROR = 1;
	public final static int MESSAGE = 2;
	public final static int WARNING = 3;
/**
 * Insert the method's description here.
 * Creation date: (08.11.2001 03:12:29)
 */
public UserException(int newType, String message) {
	super(message);
	if (newType<1 && newType>3) {
		this.type = UserException.ERROR;
	} else {
		this.type = newType;
	}
}
/**
 * Insert the method's description here.
 * Creation date: (22.11.2001 12:49:28)
 * @return boolean
 * @param exceptionType int
 */
public boolean equals(int exceptionType) {
	return type==exceptionType;
}
}
