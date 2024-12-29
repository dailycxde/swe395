package ssk.faaliyet.servlet;

import java.io.IOException;
import javax.servlet.Servlet;
import javax.servlet.ServletException;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpServlet;
import ssk.faaliyet.veri.*;

import ssk.faaliyet.model.*;

/**
 * @version 	1.0
 * @author
 */
public class MenuServlet extends AbstractServlet {

	public void faaliyet(
		HttpServletRequest req,
		HttpServletResponse resp) {
			req.getSession().setAttribute("faaliyet", new Faaliyet());
			req.setAttribute(nextUrl, "/jsp/faaliyet.jsp");
	}
	public void proje(
			HttpServletRequest req,
			HttpServletResponse resp) {
				req.getSession().setAttribute("proje", new Proje());
				req.setAttribute(nextUrl, "/jsp/proje.jsp");
		}
	public void kullaniciYarat(
			HttpServletRequest req,
			HttpServletResponse resp) {
				req.getSession().setAttribute("kullanici", new Kullanici());
				req.setAttribute(nextUrl, "/jsp/kullaniciYarat.jsp");
		}
	public void sifreDegistir(
			HttpServletRequest req,
			HttpServletResponse resp) {
				//req.getSession().setAttribute("kullanici", new Kullanici());
				req.setAttribute(nextUrl, "/jsp/sifreDegistir.jsp");
			}
	
	public void faaliyetGoster(
		HttpServletRequest req,
		HttpServletResponse resp) {
			req.setAttribute(nextUrl, "/jsp/tarihSor.jsp");
	}
	public void yonlendir(
		HttpServletRequest req,
		HttpServletResponse resp) {
			req.setAttribute(nextUrl, "/jsp/menu.jsp");
		}
	public void cikis(
		HttpServletRequest req,
		HttpServletResponse resp) {
//			java.util.Enumeration enum = req.getSession().getAttributeNames();
//			while (enum.hasMoreElements()) {
//				req.getSession().removeAttribute((String) enum.nextElement());
//			}
			req.getSession().invalidate();
			req.setAttribute(nextUrl, "/jsp/giris.jsp");
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
