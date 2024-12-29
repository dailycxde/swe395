/*
 * Created on 03.Eki.2003
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package ssk.faaliyet.model;

/**
 * @author Administrator
 *
 * To change the template for this generated type comment go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
public class Proje {
	private String projeKod;
	private String projeAdi;
	
	/**
	 * @return
	 */
	public String getProjeAdi() {
		return projeAdi;
	}

	/**
	 * @return
	 */
	public String getProjeKod() {
		return projeKod;
	}

	/**
	 * @param string
	 */
	public void setProjeAdi(String string) {
		projeAdi = string;
	}

	/**
	 * @param i
	 */
	public void setProjeKod(String i) {
		projeKod = i;
	}

}
