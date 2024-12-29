package tai.foundation.model;
public class NesneString {
	private String kodu;
	private String adi;
	/**
	 * Gets the adi
	 * @return Returns a String
	 */
	public String getAdi() {
		return adi;
	}
	/**
	 * Sets the adi
	 * @param adi The adi to set
	 */
	public void setAdi(String adi) {
		this.adi = adi;
	}

	/**
	 * Gets the kodu
	 * @return Returns a String
	 */
	public String getKodu() {
		return kodu;
	}
	/**
	 * Sets the kodu
	 * @param kodu The kodu to set
	 */
	public void setKodu(String kodu) {
		this.kodu = kodu;
	}
	/**
	* Insert the method's description here.
	* Creation date: (14.06.2002 02:58:30)
	* @return boolean
	* @param obj java.lang.Object
	*/
	public boolean equals(Object obj) {
		if ((obj != null) && (obj instanceof NesneString)) {
			return this.getKodu().equals(((NesneString) obj).getKodu());
		} else {
			return false;
		}
	}
	/**
	 * UzmanlikDali constructor comment.
	 */
	public NesneString() {
		super();
		this.setAdi("");
		this.setKodu("");
	}
	/**
	 * UzmanlikDali constructor comment.
	 */
	public NesneString(String yeniKodu, String yeniAdi) {
		super();
		this.setAdi(yeniAdi);
		this.setKodu(yeniKodu);
	}
	/**
	 * UzmanlikDali constructor comment.
	 */
	public NesneString(String yeniKodu) {
		super();
		this.setAdi(yeniKodu);
		this.setKodu("");
	}
	/**
	 * UzmanlikDali constructor comment.
	 */
	public NesneString(NesneString n) {
		super();
		this.setKodu(new String(n.getKodu()));
		this.setAdi(new String(n.getAdi()));
	}
}