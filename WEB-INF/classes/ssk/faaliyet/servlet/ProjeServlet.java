package ssk.faaliyet.servlet;

import java.io.IOException;
import javax.servlet.Servlet;
import javax.servlet.ServletException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import ssk.faaliyet.model.*;
import ssk.faaliyet.veri.*;
import tai.foundation.model.*;
import java.util.ArrayList;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpServlet;

/**
 * @version 	1.0
 * @author
 */
public class ProjeServlet extends AbstractServlet {

	public void proje(
		HttpServletRequest req,
		HttpServletResponse resp) {
			
			FormVerifier fv =  new FormVerifier();
			fv.addField("projeKod" ,FormField.FT_STRING ,"1","10");
			fv.addField("projeAdi" ,FormField.FT_STRING,"1","40");
			fv.getFieldValues(req);
			//ring projeKod = req.getParameter("projeKod");
			//ring projeAdi = req.getParameter("projeAdi");
			req.setAttribute(nextUrl,"/jsp/gonder.jsp");
			Proje proje = new Proje();
			try{
						
			fv.verify();
			proje.setProjeKod(fv.getField("projeKod").strValue());
			proje.setProjeAdi(fv.getField("projeAdi").strValue());
			Proje sessionProje = (Proje) req.getSession().getAttribute("proje");
			if (sessionProje.getProjeKod()==null){
			ProjeDS.getInstance().projeEkle(proje);
			req.setAttribute(mesaj,"Yeni Proje Baþarýyla Eklenmiþtir");
			req.setAttribute(nextUrl, "/jsp/proje.jsp");	
			}else{
			ProjeDS.getInstance().projeGuncelle(proje);	
			req.setAttribute(mesaj,"Proje Baþarýyla Güncellenmiþtir");
			req.setAttribute(nextUrl, "/jsp/proje.jsp");
			}
			}
			catch(Hata h){
					req.setAttribute("hata", h.getMessage());
					req.setAttribute(nextUrl, "/jsp/proje.jsp");	
				}
			catch(Exception ex){}
					
	}
	
	public void degistir(
		HttpServletRequest req,
		HttpServletResponse resp){
			
			HttpSession session = req.getSession();
			
			int siraNo = Integer.parseInt(req.getParameter("siraNo"));
			
			ArrayList liste = (ArrayList)session.getAttribute("liste");
			Proje proje = (Proje) liste.get(siraNo);
			session.setAttribute("proje", proje);
			req.setAttribute(nextUrl, "/jsp/proje.jsp");			
		}	
	public void projeGoster(
			HttpServletRequest req,
			HttpServletResponse resp){
		    HttpSession session = req.getSession();
			
		    try{
			
		    ArrayList liste = ProjeDS.getInstance().projeGoster();
		    session.setAttribute("liste",liste);
			req.setAttribute(nextUrl,"/jsp/projeSonuc.jsp");
			}
			catch(Exception e){
			
		}
			}

}
