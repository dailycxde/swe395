/*
 * Created on 07.Eki.2003
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package ssk.faaliyet.veri;

import java.sql.Connection;

import javax.ejb.DuplicateKeyException;

import ssk.faaliyet.model.Kullanici;
import tai.foundation.db.DataSourceBroker;
import tai.foundation.db.EasyPreparedStatement;
import tai.foundation.db.SqlQuery;
import tai.foundation.model.Hata;

/**
 * @author Administrator
 * 
 * To change the template for this generated type comment go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
public class KullaniciDS {

	public void kullaniciEkle(Kullanici kullanici) throws Hata {

		Connection conn = null;
		EasyPreparedStatement st = null;

		try {
			conn = DataSourceBroker.getConnection("faaliyet");
			st = new EasyPreparedStatement(conn, SQL_KULLANICI_EKLE);
			st.addParameter(kullanici.getKullaniciId());
			st.addParameter(kullanici.getSifre());
			st.addParameter(kullanici.getAdi());
			st.addParameter(kullanici.getSoyadi());
			int result = st.getPreparedStatement().executeUpdate();
			System.out.println(result);
		} catch (DuplicateKeyException de) {
			throw new Hata("Bu kullanýcý Id Kullanýlmakta!");
		} catch (Exception e) {
			System.out.println(e.getMessage() + e.getClass());
		} finally {
			try {
				st.close();
			} catch (Exception e) {
			}
			st = null;

			try {
				conn.close();
			} catch (Exception ex) {
			}
			conn = null;
		}
	}

	public String kullaniciAdBul(String kullaniciId) throws Hata {

		Connection conn = null;
		EasyPreparedStatement st = null;
		SqlQuery rs = null;
		String adSoyad = "";

		try {
			conn = DataSourceBroker.getConnection("faaliyet");
			st = new EasyPreparedStatement(conn, SQL_KULLANICI_BUL);
			st.addParameter(kullaniciId);
			rs = new SqlQuery(st.getPreparedStatement());

			while (rs.next()) {

				adSoyad = (rs.getString("ad")) + " " + (rs.getString("soyad"));

			}

		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			try {
				rs.close();
			} catch (Exception ex) {
			}
			rs = null;
		}
		try {
			st.close();
		} catch (Exception ex) {
		}
		st = null;

		try {
			conn.close();
		} catch (Exception ex) {
		}
		conn = null;

		return adSoyad;
	}
	public Kullanici kullaniciBul(String kullaniciId) throws Hata {

		Connection conn = null;
		EasyPreparedStatement st = null;
		SqlQuery rs = null;
		Kullanici kullanici = new Kullanici();

		try {
			conn = DataSourceBroker.getConnection("faaliyet");
			st = new EasyPreparedStatement(conn, SQL_KULLANICI_BUL);
			st.addParameter(kullaniciId);
			rs = new SqlQuery(st.getPreparedStatement());

			if (rs.next()) {

				kullanici.setAdi(rs.getString("AD"));
				kullanici.setSoyadi(rs.getString("SOYAD"));
				kullanici.setKullaniciId(rs.getString("KULLANICIID"));

			}

		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			try {
				rs.close();
			} catch (Exception ex) {
			}
			rs = null;
		}
		try {
			st.close();
		} catch (Exception ex) {
		}
		st = null;

		try {
			conn.close();
		} catch (Exception ex) {
		}
		conn = null;

		return kullanici;
	}

	public void sifreDegistir(Kullanici kullanici, String eskiSifre)
			throws Hata {

		Connection conn = null;
		EasyPreparedStatement st = null;

		try {
			conn = DataSourceBroker.getConnection("faaliyet");
			st = new EasyPreparedStatement(conn, SQL_SIFRE_GUNCELLE);
			st.addParameter(kullanici.getSifre());
			st.addParameter(kullanici.getKullaniciId());
			st.addParameter(eskiSifre);
			int result = st.getPreparedStatement().executeUpdate();
			if (result == 0) {
				throw new Hata("Eski Þifrenizi Doðru giriniz!");
			}
			System.out.println(result);
		} catch (Exception e) {
			throw new Hata("Eski Þifrenizi Doðru giriniz!");
		} finally {
			try {
				st.close();
			} catch (Exception e) {
			}
			st = null;
			try {
				conn.close();
			} catch (Exception ex) {
			}
			conn = null;
		}
	}

	private static KullaniciDS kullaniciDS = null;

	public static KullaniciDS getInstance() {
		if (kullaniciDS == null) {
			kullaniciDS = new KullaniciDS();
		}
		return kullaniciDS;
	}

	private static final String SQL_KULLANICI_EKLE = "INSERT INTO FAALIYET.KULLANICI(KULLANICIID, SIFRE, AD, SOYAD) VALUES(?,?,?,?)";

	private static final String SQL_SIFRE_GUNCELLE = "UPDATE FAALIYET.KULLANICI SET SIFRE=? WHERE KULLANICIID=? AND SIFRE=?";

	private static final String SQL_KULLANICI_BUL = "SELECT AD,SOYAD,KULLANICIID FROM FAALIYET.KULLANICI WHERE KULLANICIID=?";
}
