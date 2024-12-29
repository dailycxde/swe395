package ssk.faaliyet.model;

/**
 * Insert the type's description here.
 * Creation date: (30.05.2002 02:08:03)
 * @author: Yusuf Donmez
 */
public class Kullanici implements java.io.Serializable {
	private String kullaniciId;
	private String sifre;
	private String adi;
	private String soyadi;
	

	
	
	/**
	 * Kullanici constructor comment.
	 */
	public Kullanici() {
		super();
	}
	
	/**
	 * Kullanici constructor comment.
	 */
	public Kullanici(String yeniKodu) {
		super();
		setKullaniciId(yeniKodu);
	}
	
	
	/**
	 * Gets the kodu
	 * @return Returns a int
	 */

	public String getAdi() {
		return adi;
	}

	/**
	 * @return
	 */
	public String getKullaniciId() {
		return kullaniciId;
	}

	/**
	 * @return
	 */
	public String getSifre() {
		return sifre;
	}

	/**
	 * @return
	 */
	public String getSoyadi() {
		return soyadi;
	}

	public String getAdSoyad() {
		return adi + " " + soyadi;
	}

	/**
	 * @param string
	 */
	public void setAdi(String string) {
		adi = string;
	}

	/**
	 * @param string
	 */
	public void setKullaniciId(String string) {
		kullaniciId = string;
	}

	/**
	 * @param string
	 */
	public void setSifre(String string) {
		sifre = string;
	}

	/**
	 * @param string
	 */
	public void setSoyadi(String string) {
		soyadi = string;
	}

	
	public boolean equals(Object obj) {
		if ((obj != null) && (obj instanceof Kullanici)) {
			return (this.getKullaniciId() == ((Kullanici) obj).getKullaniciId());
		} else {
			return false;
		}
	}
	/**
	 * @return
	 */
}