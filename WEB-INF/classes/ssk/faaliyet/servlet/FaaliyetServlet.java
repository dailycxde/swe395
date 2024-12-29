package ssk.faaliyet.servlet;

import java.util.ArrayList;
import java.util.Iterator;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import ssk.faaliyet.model.Faaliyet;
import ssk.faaliyet.model.Kullanici;
import ssk.faaliyet.model.Proje;
import ssk.faaliyet.veri.FaaliyetDS;
import tai.foundation.model.FormField;
import tai.foundation.model.FormVerifier;
import tai.foundation.model.Hata;
/**
 * @version 	1.0
 * @author
 */
public class FaaliyetServlet extends AbstractServlet {

	public void faaliyet(
		HttpServletRequest req,
		HttpServletResponse resp) {
			HttpSession session = req.getSession(false);
			FormVerifier fv =  new FormVerifier();
			fv.addField("tarih", FormField.FT_DATE);
			fv.addField("toplamSaat", FormField.FT_STRING, "11","11");
			fv.addField("calisma" ,FormField.FT_STRING);
			fv.addField("calisilanYer" , FormField.FT_STRING);
			fv.addField("proje" , FormField.FT_STRING);
			fv.getFieldValues(req);
			Kullanici k = (Kullanici)req.getSession().getAttribute("kullanici");
			try {
			} catch (NumberFormatException e) {
				
			}
			try {
				
				Faaliyet faaliyet = new Faaliyet();

				try{
				fv.verify();
				faaliyet.setTarih(fv.getField("tarih").dateValue());
				faaliyet.setProjeKod(fv.getField("proje").strValue());
				faaliyet.setCalismaSaati((fv.getField("toplamSaat").strValue()));
				faaliyet.setCalisma(fv.getField("calisma").strValue());
				faaliyet.setCalisilanYer(fv.getField("calisilanYer").strValue());
				session.setAttribute("calisilanYer",faaliyet.getCalisilanYer());
				faaliyet.setKullanici(k);
					}catch(Hata h){
					req.setAttribute("hata", h.getMessage());
					req.setAttribute(nextUrl, "/jsp/faaliyet.jsp");	
					return;
					}
				catch(Exception ex){
					req.setAttribute("hata", ex.getMessage());
					req.setAttribute(nextUrl, "/jsp/faaliyet.jsp");	
					return;
					}
				Faaliyet sessionFaaliyet = (Faaliyet) req.getSession().getAttribute("faaliyet"); 
				if (sessionFaaliyet.getProjeKod()==null){
				FaaliyetDS.getInstance().faaliyetEkle(faaliyet);
				session.setAttribute("projeEski" , faaliyet.getProjeKod());
				session.setAttribute("projeAdiEski" , faaliyet.getProjeAdi());
				session.setAttribute("calisilanYerEski" , faaliyet.getCalisilanYer());
				req.setAttribute(mesaj,"Faaliyetiniz Baþarýyla Eklenmiþtir");
				req.setAttribute(nextUrl, "/jsp/faaliyet.jsp");	
				}else{
				FaaliyetDS.getInstance().faaliyetGuncelle(faaliyet);
				session.setAttribute("faaliyet" , FaaliyetDS.getInstance().faaliyetBul(faaliyet.getTarih(),faaliyet.getKullanici().getKullaniciId(),faaliyet.getProjeKod()));
				req.setAttribute(mesaj,"Faaliyetiniz Baþarýyla Güncellenmiþtir");
				req.setAttribute(nextUrl, "/jsp/faaliyet.jsp");
				}
				
			} catch (Hata h) {
				req.setAttribute("hata",h.getMessage());
				req.setAttribute(nextUrl, "/jsp/faaliyet.jsp");	
			}
	}

	public void degistir(
		HttpServletRequest req,
		HttpServletResponse resp){
			
			HttpSession session = req.getSession();
			
			int siraNo = Integer.parseInt(req.getParameter("siraNo"));
			
			ArrayList liste2 = (ArrayList)session.getAttribute("liste2");
			Faaliyet faaliyet = (Faaliyet) liste2.get(siraNo);
			session.setAttribute("faaliyet", faaliyet);
			req.setAttribute(nextUrl, "/jsp/faaliyet.jsp");			
		}		

	public void faaliyetGoster(
		HttpServletRequest req,
		HttpServletResponse resp){
		
		HttpSession session = req.getSession();
		
		try{
		FormVerifier fv =  new FormVerifier();
		fv.addField("basTarih", FormField.FT_DATE);
		fv.addField("bitTarih", FormField.FT_DATE);
		fv.addField("proje" ,FormField.FT_STRING);
		fv.getFieldValues(req);
		try{
			fv.verify();
			}catch(Hata h){
			req.setAttribute("hata", h.getMessage());
			req.setAttribute(nextUrl, "/jsp/tarihSor.jsp");	
			return;
		}
		String projeKod = req.getParameter("proje");
		String tarih = req.getParameter("basTarih") + " - " + req.getParameter("bitTarih");
		session.setAttribute("tarih", tarih );
		ArrayList liste = FaaliyetDS.getInstance().faaliyetGoster(fv.getField("basTarih").dateValue(),fv.getField("bitTarih").dateValue(), projeKod);
		session.setAttribute("liste",liste);
		ArrayList projeler = (ArrayList)session.getAttribute("projeler");
		for (Iterator iter = projeler.iterator(); iter.hasNext();) {
			Proje element = (Proje) iter.next();
			if (element.getProjeKod().equals(projeKod)){
				session.setAttribute("proje", element);
			}
			
		}
		req.setAttribute(nextUrl,"/jsp/faaliyetSonuc.jsp");
		}
		catch(Exception e){
			
		}
	}
	
	public void faaliyetRapor(
		HttpServletRequest req,
		HttpServletResponse resp){
			
			req.setAttribute(nextUrl,"/jsp/faaliyetRapor.jsp");	
		}
	public void faaliyetSil(
			HttpServletRequest req,
			HttpServletResponse resp){
				HttpSession session = req.getSession();
			
				int siraNo = Integer.parseInt(req.getParameter("siraNo"));
			
				ArrayList liste = (ArrayList)session.getAttribute("liste");
				Faaliyet faaliyet = (Faaliyet) liste.get(siraNo);
				try {
					FaaliyetDS.getInstance().faaliyetSil(faaliyet);
				} catch (Hata e) {
					req.setAttribute("hata", e.getMessage());
					req.setAttribute(nextUrl, "/jsp/faaliyetSonuc.jsp");	
					e.printStackTrace();
				}
				req.setAttribute(nextUrl,"/jsp/tarihSor.jsp");	
			}
	public void faaliyetGoruntule(
		HttpServletRequest req,
		HttpServletResponse resp)
	{
				req.setAttribute(nextUrl,"/jsp/tarihSor.jsp");
				}
}
