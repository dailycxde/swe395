/*
 * Created on 03.Eki.2003
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package ssk.faaliyet.veri;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import ssk.faaliyet.model.Faaliyet;
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
public class FaaliyetDS {

	public void faaliyetEkle(Faaliyet faaliyet)	throws Hata {
		
		Connection conn = null;
		EasyPreparedStatement st = null;
		
		try {
			conn = DataSourceBroker.getConnection("faaliyet");
			st = new EasyPreparedStatement(conn, SQL_FAALIYET_EKLE);
			st.addParameter(faaliyet.getTarih());
			st.addParameter(faaliyet.getKullanici().getKullaniciId());
			st.addParameter(faaliyet.getCalisilanYer());
			st.addParameter(faaliyet.getCalismaSaati());
			st.addParameter(faaliyet.getProjeKod());
			st.addParameter(faaliyet.getCalisma());
			int result = st.getPreparedStatement().executeUpdate();
			System.out.println(result);
		}catch(SQLException sql){
			if (sql.getSQLState().equals("23000")){
				throw new Hata("Daha önceden ayný tarihe ayný projeden faaliyet girilmiþ");
			}
		}catch(Exception e) {
			System.out.println(e.getMessage());
		} finally {
			try {
				st.close();
			} catch (Exception e) { }
			st = null;	
			
			try	{
				conn.close();
			} catch (Exception ex) { }
			conn = null;
		}		
	}
	public void faaliyetGuncelle (Faaliyet faaliyet) throws Hata{
		
		Connection conn = null;
		EasyPreparedStatement st = null;
		
		try {
			conn = DataSourceBroker.getConnection("faaliyet");
			st = new EasyPreparedStatement(conn, SQL_FAALIYET_GUNCELLE);
			st.addParameter(faaliyet.getCalisilanYer());
			st.addParameter(faaliyet.getCalismaSaati());
			st.addParameter(faaliyet.getCalisma());
			st.addParameter(faaliyet.getTarih());
			st.addParameter(faaliyet.getKullanici().getKullaniciId());
			st.addParameter(faaliyet.getProjeKod());
			int result = st.getPreparedStatement().executeUpdate();
			if (result==0){throw new Hata("Güncelleme iþlemi gerçekleþmedi!");}
			System.out.println(result);
		}catch (Hata h) {
			throw new Hata("Güncelleme iþlemi gerçekleþmedi!");
			}  
		catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			try {
				st.close();
			} catch (Exception e) { }
			st = null;	
			
			try	{
				conn.close();
			} catch (Exception ex) { }
			conn = null;
		}		
	}

	public void faaliyetSil (Faaliyet faaliyet) throws Hata{
		
		Connection conn = null;
		EasyPreparedStatement st = null;
		
		try {
			conn = DataSourceBroker.getConnection("faaliyet");
			st = new EasyPreparedStatement(conn, SQL_FAALIYET_SIL);
			st.addParameter(faaliyet.getTarih());
			st.addParameter(faaliyet.getKullanici().getKullaniciId());
			st.addParameter(faaliyet.getProjeKod());
			int result = st.getPreparedStatement().executeUpdate();
			if (result==0){throw new Hata("Silme iþlemi gerçekleþmedi!");}
			System.out.println(result);
		}catch (Hata h) {
			throw new Hata("Silme iþlemi gerçekleþmedi!");
			}  
		catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			try {
				st.close();
			} catch (Exception e) { }
			st = null;	
			
			try	{
				conn.close();
			} catch (Exception ex) { }
			conn = null;
		}		
	}

	public ArrayList faaliyetGoster(java.util.Date basTarih , java.util.Date bitTarih , String projeKod)	{
		
		Connection conn = null;
		EasyPreparedStatement st = null;
		SqlQuery rs = null;
		ArrayList result = new ArrayList();
		
		try {
			conn = DataSourceBroker.getConnection("faaliyet");
			st = new EasyPreparedStatement(conn, SQL_FAALIYET_GETIR);
			st.addParameter(basTarih);
			st.addParameter(bitTarih);
			st.addParameter(projeKod);
			rs = new SqlQuery(st.getPreparedStatement());
			while (rs.next()) {
				Faaliyet faaliyet = new Faaliyet();
				faaliyet.setTarih(rs.getUtilDate("TARIH"));
				faaliyet.setCalisilanYer(rs.getString("CALISILANYER"));
				faaliyet.setCalismaSaati(rs.getString("TOPLAMSAAT"));
				faaliyet.setProjeKod(rs.getString("PROJE_KOD"));
				faaliyet.setProjeAdi(ProjeDS.getInstance().projeBul(faaliyet.getProjeKod()).getProjeAdi());
				faaliyet.setCalisma(replace (rs.getString("YAPILANIS"), (char)13 , "<br>" )); 
				faaliyet.setKullanici(KullaniciDS.getInstance().kullaniciBul(rs.getString("KULLANICIID")));
				result.add(faaliyet);
			}
		}
		 catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			try {
				st.close();
			} catch (Exception e) { }
			st = null;	
			
			try	{
				conn.close();
			} catch (Exception ex) { }
			conn = null;
		}		
		return result;
	}
	
	public ArrayList faaliyetGosterBrsiz(java.util.Date basTarih , java.util.Date bitTarih , String kullaniciId)	{
		
		Connection conn = null;
		EasyPreparedStatement st = null;
		SqlQuery rs = null;
		ArrayList result = new ArrayList();
		
		try {
			conn = DataSourceBroker.getConnection("faaliyet");
			st = new EasyPreparedStatement(conn, SQL_FAALIYET_GETIR);
			//st.addParameter(new SimpleDateFormat("dd.MM.yyyy").parse(basTarih));
			//st.addParameter(new SimpleDateFormat("dd.MM.yyyy").parse(bitTarih));
			st.addParameter(basTarih);
			st.addParameter(bitTarih);
			st.addParameter(kullaniciId);
			rs = new SqlQuery(st.getPreparedStatement());
			while (rs.next()) {
				Faaliyet faaliyet = new Faaliyet();
				faaliyet.setTarih(rs.getUtilDate("TARIH"));
				faaliyet.setCalisilanYer(rs.getString("CALISILANYER"));
				faaliyet.setCalismaSaati(rs.getString("TOPLAMSAAT"));
				faaliyet.setProjeKod(rs.getString("PROJE_KOD"));
				faaliyet.setProjeAdi(ProjeDS.getInstance().projeBul(faaliyet.getProjeKod()).getProjeAdi());
				faaliyet.setCalisma(rs.getString("YAPILANIS")); 
				faaliyet.getKullanici().setKullaniciId(rs.getString("KULLANICIID"));
				result.add(faaliyet);
			}
		}
		 catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			try {
				st.close();
			} catch (Exception e) { }
			st = null;	
			
			try	{
				conn.close();
			} catch (Exception ex) { }
			conn = null;
		}		
		return result;
	}
	
	public Faaliyet faaliyetBul(java.util.Date tarih, String kullaniciId, String projeKod )	{
		
		Connection conn = null;
		EasyPreparedStatement st = null;
		SqlQuery rs = null;
		Faaliyet faaliyet = new Faaliyet();
		
		try {
			conn = DataSourceBroker.getConnection("faaliyet");
			st = new EasyPreparedStatement(conn, SQL_FAALIYET_BUL);
			st.addParameter(tarih);
			st.addParameter(kullaniciId);
			st.addParameter(projeKod);
			rs = new SqlQuery(st.getPreparedStatement());
			while (rs.next()) {
				faaliyet.setTarih(rs.getUtilDate("TARIH"));
				faaliyet.setCalisilanYer(rs.getString("CALISILANYER"));
				faaliyet.setCalismaSaati(rs.getString("TOPLAMSAAT"));
				faaliyet.setProjeKod(rs.getString("PROJE_KOD"));
				faaliyet.setProjeAdi(ProjeDS.getInstance().projeBul(faaliyet.getProjeKod()).getProjeAdi());
				faaliyet.setCalisma(rs.getString("YAPILANIS"));
				faaliyet.getKullanici().setKullaniciId(rs.getString("KULLANICIID"));
						}
		}
		 catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			try {
				st.close();
			} catch (Exception e) { }
			st = null;	
			
			try	{
				conn.close();
			} catch (Exception ex) { }
			conn = null;
		}		
		return faaliyet;
	}
	
	public final static String replace(String str, char fromCh, String toStr)	{
	
		if (str == null)	{
			return str;
		} else {
			StringBuffer result = new StringBuffer();
			for (int i=0;i<str.length();i++)	{
				if (str.charAt(i) == fromCh)	{
					result.append(toStr);
				} else {
					result.append(str.charAt(i));
				}
			}
			return result.toString();
		}
	}





	private static FaaliyetDS faaliyetDS = null;
	
	public static FaaliyetDS getInstance()	{
		if (faaliyetDS==null)	{
			faaliyetDS = new FaaliyetDS();
		}
		return faaliyetDS;
	}

	private static final String SQL_FAALIYET_EKLE =
		"INSERT INTO FAALIYET.FAALIYET(TARIH, KULLANICIID, CALISILANYER, TOPLAMSAAT, PROJE_KOD , YAPILANIS) VALUES(?,?,?,?,?,?)";
	private static final String SQL_FAALIYET_GUNCELLE =
		"UPDATE FAALIYET.FAALIYET SET CALISILANYER=? ,TOPLAMSAAT=?  , YAPILANIS=? WHERE TARIH=? AND KULLANICIID=? AND PROJE_KOD=?";		
	private static final String SQL_FAALIYET_GETIR =
		"SELECT TARIH , CALISILANYER , TOPLAMSAAT , PROJE_KOD , YAPILANIS ,KULLANICIID FROM FAALIYET.FAALIYET WHERE (TARIH BETWEEN ? AND ?) AND PROJE_KOD =?";
	private static final String SQL_FAALIYET_SIL =
		"DELETE FROM FAALIYET.FAALIYET WHERE TARIH = ? AND KULLANICIID = ? AND PROJE_KOD=?";
	private static final String SQL_FAALIYET_BUL =
		"SELECT * FROM FAALIYET.FAALIYET WHERE TARIH = ? AND KULLANICIID = ? AND PROJE_KOD=?";	
}
