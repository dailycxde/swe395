package tai.foundation.model;

/**
 * Insert the type's description here.
 * Creation date: (14.06.2002 09:04:40)
 * @author: Yusuf Donmez
 */
public class Nesne implements java.io.Serializable {
	private int kodu;
	private java.lang.String adi;
	/**
	 * UzmanlikDali constructor comment.
	 */
	public Nesne() {
		super();
	}
	/**
	 * UzmanlikDali constructor comment.
	 */
	public Nesne(int yeniKodu, String yeniAdi) {
		this();
		this.setKodu(yeniKodu);
		this.setAdi(yeniAdi);
	}
	/**
	 * UzmanlikDali constructor comment.
	 */
	public Nesne(Nesne n) {
		this();
		this.setKodu(n.getKodu());
		this.setAdi(new String(n.getAdi()));
	}
	/**
	 * Insert the method's description here.
	 * Creation date: (14.06.2002 02:58:30)
	 * @return boolean
	 * @param obj java.lang.Object
	 */
	public boolean equals(Object obj) {
		if ((obj != null) && (obj instanceof Nesne)) {
			return (this.getKodu() == ((Nesne) obj).getKodu());
		} else {
			return false;
		}
	}
	/**
	 * Insert the method's description here.
	 * Creation date: (14.06.2002 09:04:55)
	 * @return java.lang.String
	 */
	public java.lang.String getAdi() {
		return adi;
	}
	/**
	 * Insert the method's description here.
	 * Creation date: (14.06.2002 09:04:47)
	 * @return int
	 */
	public int getKodu() {
		return kodu;
	}
	/**
	 * Insert the method's description here.
	 * Creation date: (14.06.2002 09:04:55)
	 * @param newAdi java.lang.String
	 */
	public void setAdi(java.lang.String newAdi) {
		adi = newAdi;
	}
	/**
	 * Insert the method's description here.
	 * Creation date: (14.06.2002 09:04:47)
	 * @param newKodu int
	 */
	public void setKodu(int newKodu) {
		kodu = newKodu;
	}

	/**
	 * UzmanlikDali constructor comment.
	 */
	public Nesne(int yeniKodu) {
		this();
		this.setKodu(yeniKodu);
	}
}