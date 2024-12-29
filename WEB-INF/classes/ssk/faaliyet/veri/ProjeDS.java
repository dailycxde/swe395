/*
 * Created on 03.Eki.2003
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package ssk.faaliyet.veri;
import java.sql.Connection;
import java.util.ArrayList;

import javax.ejb.DuplicateKeyException;

import ssk.faaliyet.model.Proje;
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
public class ProjeDS {
	
	private static final String SQL_PROJE_EKLE = "INSERT INTO FAALIYET.PROJE(KOD,AD)VALUES(?,?)";
	private static final String SQL_PROJE_GETIR = "SELECT * FROM FAALIYET.PROJE";
	private static final String SQL_PROJE_GUNCELLE = "UPDATE FAALIYET.PROJE SET AD=? WHERE KOD=?";
	private static final String SQL_PROJE_BUL = "SELECT AD, KOD FROM FAALIYET.PROJE WHERE KOD=?";
	private static ProjeDS projeDS = null;
	
	
	public void projeEkle (Proje proje) throws Hata {
		
		Connection conn = null;
		EasyPreparedStatement st = null;
	
		
		try {
			conn = DataSourceBroker.getConnection("faaliyet");
			st = new EasyPreparedStatement(conn , SQL_PROJE_EKLE);
			st.addParameter(proje.getProjeKod());
			st.addParameter(proje.getProjeAdi());
			int result = st.getPreparedStatement().executeUpdate();
			}
			catch(DuplicateKeyException de){
				throw new Hata("Bu proje daha önceden girilmiþ");
			}
			catch (Exception ex){
				System.out.println(ex.getMessage());
				}finally{
					try{
						st.close();
					}catch (Exception ex){}
					st = null;
					try {
						conn.close();
					} catch (Exception ex) {}
					conn = null;
					
					
					
				}
	}
	
	public ArrayList projeGoster()	{
		
		Connection conn = null;
		EasyPreparedStatement st = null;
		SqlQuery rs = null;
		ArrayList result = new ArrayList();
		
		try {
			conn = DataSourceBroker.getConnection("faaliyet");
			st = new EasyPreparedStatement(conn, SQL_PROJE_GETIR);
			rs = new SqlQuery(st.getPreparedStatement());
			while (rs.next()) {
				Proje proje = new Proje();
				proje.setProjeKod(rs.getString("KOD"));
				proje.setProjeAdi(rs.getString("AD"));
				result.add(proje);
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
	
	public Proje projeBul(String projeKod)	{
		
		Connection conn = null;
		EasyPreparedStatement st = null;
		SqlQuery rs = null;
		Proje proje = new Proje();		
		try {
			conn = DataSourceBroker.getConnection("faaliyet");
			st = new EasyPreparedStatement(conn, SQL_PROJE_BUL);
			st.addParameter(projeKod);
			rs = new SqlQuery(st.getPreparedStatement());
			while (rs.next()) {
				proje.setProjeKod(rs.getString("KOD"));
				proje.setProjeAdi(rs.getString("AD"));
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
		return proje;
	}
	
	public void projeGuncelle (Proje proje) throws Hata{
		
		Connection conn = null;
		EasyPreparedStatement st = null;
		
		try {
			conn = DataSourceBroker.getConnection("faaliyet");
			st = new EasyPreparedStatement(conn, SQL_PROJE_GUNCELLE);
			st.addParameter(proje.getProjeAdi());
			st.addParameter(proje.getProjeKod());
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

	
	
	public static ProjeDS getInstance(){
		
		if (projeDS == null){
			projeDS = new ProjeDS();
			}
			return projeDS;
	}

}
