/*
 * Created on 03.Eki.2003
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package ssk.faaliyet.model;

import java.util.Date;

/**
 * @author Administrator
 *
 * To change the template for this generated type comment go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
public class Faaliyet {
	private Date tarih;
	private Kullanici kullanici;
	private String projeKod;
	private String projeAdi;
	private String calismaSaati;
	private String calisma;
	private String calisilanYer;
	/**
	 * @return
	 */
	
	public Faaliyet()	{
		super();
		this.setKullanici(new Kullanici());
		this.setTarih(new Date());
	}
	
	public String getCalisma() {
		return calisma;
	}

	/**
	 * @return
	 */


	/**
	 * @return
	 */
	public Date getTarih() {
		return tarih;
	}

	/**
	 * @return
	 */

	/**
	 * @param string
	 */
	public void setCalisma(String string) {
		calisma = string;
	}

	/**
	 * @param i
	 */

	/**
	 * @param date
	 */
	public void setTarih(Date date) {
		tarih = date;
	}

	/**
	 * @return
	 */
	public String getCalisilanYer() {
		return calisilanYer;
	}

	/**
	 * @param string
	 */
	public void setCalisilanYer(String string) {
		calisilanYer = string;
	}
	public Kullanici getKullanici() {
		return kullanici;
	}

	public void setKullanici(Kullanici kullanici) {
		this.kullanici = kullanici;
	}

	public String getProjeAdi() {
		return projeAdi;
	}

	public void setProjeAdi(String projeAdi) {
		this.projeAdi = projeAdi;
	}

	public String getProjeKod() {
		return projeKod;
	}

	public void setProjeKod(String projeKod) {
		this.projeKod = projeKod;
	}

	public String getCalismaSaati() {
		return calismaSaati;
	}

	public void setCalismaSaati(String calismaSaati) {
		this.calismaSaati = calismaSaati;
	}
	
}
