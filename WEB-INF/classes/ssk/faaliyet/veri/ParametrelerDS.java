/*
 * Created on 04.Eki.2003
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package ssk.faaliyet.veri;

import java.sql.Connection;
import java.util.ArrayList;

import ssk.faaliyet.model.Kullanici;
import ssk.faaliyet.model.Proje;
import tai.foundation.db.DataSourceBroker;
import tai.foundation.db.EasyPreparedStatement;
import tai.foundation.db.SqlQuery;

/**
 * @author Administrator
 *
 * To change the template for this generated type comment go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
public class ParametrelerDS {
	private final static java.lang.String SQL_KULLANICI_OKU =
			"SELECT  KULLANICIID , AD , SOYAD  FROM FAALIYET.KULLANICI WHERE KULLANICIID=? AND SIFRE=?";
	
	public ArrayList projeListesi() {
		
		ArrayList result = new ArrayList();
		Connection conn = null;
		EasyPreparedStatement st = null;
		SqlQuery rs = null;

		try {
			conn = DataSourceBroker.getConnection("faaliyet");
			st = new EasyPreparedStatement(conn, "SELECT * FROM FAALIYET.PROJE");
			rs = new SqlQuery(st.getPreparedStatement());
			while (rs.next()) {
				Proje proje = new Proje();
				proje.setProjeKod(rs.getString("kod"));
				proje.setProjeAdi(rs.getString("ad"));
				result.add(proje);
			}


		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			try {
				rs.close();
			} catch (Exception ex) {}
			rs = null;
		}
		try {
			st.close();
		} catch (Exception ex) {}
		st = null;

		try {
			conn.close();
		} catch (Exception ex) {}
		conn = null;

	return result;
	}
	
	public ArrayList kullaniciListesi() {
		
		ArrayList result = new ArrayList();
		Connection conn = null;
		EasyPreparedStatement st = null;
		SqlQuery rs = null;

		try {
			conn = DataSourceBroker.getConnection("faaliyet");
			st = new EasyPreparedStatement(conn, "SELECT AD,SOYAD,KULLANICIID FROM FAALIYET.KULLANICI");
			rs = new SqlQuery(st.getPreparedStatement());


			while (rs.next()) {

				Kullanici kullanici = new Kullanici();
				kullanici.setAdi(rs.getString("ad"));
				kullanici.setSoyadi(rs.getString("soyad"));
				kullanici.setKullaniciId(rs.getString("kullaniciId"));
				result.add(kullanici);
			}


		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			try {
				rs.close();
			} catch (Exception ex) {}
			rs = null;
		}
		try {
			st.close();
		} catch (Exception ex) {}
		st = null;

		try {
			conn.close();
		} catch (Exception ex) {}
		conn = null;

	return result;
	}
	


	public Kullanici kullaniciBul(String kullaniciAdi, String sifre) throws java.lang.Exception {
			Connection conn = null;
			SqlQuery ds = null;
			Kullanici k = null;
			try {
				conn = DataSourceBroker.getConnection("faaliyet");
				EasyPreparedStatement st = new EasyPreparedStatement(conn, SQL_KULLANICI_OKU);
				st.addParameter(kullaniciAdi);
				st.addParameter(sifre);
				ds = new SqlQuery(st.getPreparedStatement());
				if (ds.next()) {
					k = new Kullanici();
					k.setKullaniciId(ds.getString("KULLANICIID"));
					k.setAdi(ds.getString("AD"));
					k.setSoyadi(ds.getString("SOYAD"));
					k.setSifre(sifre);
				} else {
					throw new Exception("LÜTFEN KULLANICI ADI VE ÞÝFRENÝZÝ KONTROL EDÝNÝZ!");
				}
			} catch (Exception ex) {
				System.err.println("KullaniciBul:"+new java.util.Date() + ex.getMessage());
				throw ex;
			} finally {
				try {
					ds.close();
					ds = null;
				} catch (Exception e) {
					ds = null;
				}
				try {
					conn.close();
				} catch (Exception ex) {
					conn = null;
				}
			}
			
			return k;
		}
	
	
	
	
	
	private static ParametrelerDS parametrelerDS = null;
	
	public static ParametrelerDS getInstance()	{
		if (parametrelerDS==null)	{
			parametrelerDS = new ParametrelerDS();
		}
		return parametrelerDS;
	}


}