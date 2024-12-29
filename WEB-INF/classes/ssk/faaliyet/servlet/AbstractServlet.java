package ssk.faaliyet.servlet;

import java.util.Formatter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import ssk.faaliyet.model.Islem;
import ssk.faaliyet.model.Kullanici;
/**
 * Insert the type's description hee.
 * Creation date: (28.05.2002 01:21:01)
 * @author: Yusuf Donmez
 */
public abstract class AbstractServlet extends javax.servlet.http.HttpServlet {
	protected static java.lang.String nextUrl = "FORWARD_REQ_TO_URL";
	static java.lang.String errorMessage 	= "UNEXPECTED_ERROR";
	public static final String mesaj 	   	= "mesaj";
	public static final String hataMesaji 	= "hataMesaji";
	public static final String uyariMesaji 	= "uyariMesaji";

	public void doGet(HttpServletRequest request, HttpServletResponse response)
		throws javax.servlet.ServletException, java.io.IOException {

		performTask(request, response);

	}
	/**
	 * Process incoming HTTP POST requests 
	 * 
	 * @param request Object that encapsulates the request to the servlet 
	 * @param response Object that encapsulates the response from the servlet
	 */
	public void doPost(HttpServletRequest request, HttpServletResponse response)
		throws javax.servlet.ServletException, java.io.IOException {

		performTask(request, response);

	}
	/**
	 * Insert the method's description here.
	 * Creation date: (30.05.2002 10:01:39)
	 * @param req javax.servlet.http.HttpServletRequest
	 * @param resp javax.servlet.http.HttpServletResponse
	 * @param url java.lang.String
	 */
	protected void forward(HttpServletRequest req, HttpServletResponse resp)
		throws java.io.IOException, ServletException {
		getServletContext()
			.getRequestDispatcher((String) req.getAttribute(nextUrl))
			.forward(req, resp);
	}
	/**
	 * Insert the method's description here.
	 * Creation date: (29.05.2002 04:24:42)
	 * @param req javax.servlet.http.HttpServletRequest
	 * @param resp javax.servlet.http.HttpServletResponse
	 */
	public void performTask(
		HttpServletRequest request,
		HttpServletResponse response) {

		try {
			String islemStr = request.getParameter("islem");
			
			HttpSession session = request.getSession();
			Kullanici kullanici = (Kullanici) session.getAttribute("kullanici");
			if (kullanici == null){
				request.setAttribute(nextUrl, "indexTimeout.html");
			} else 
			
			if ( isEmpty(islemStr) ) {
				request.setAttribute(nextUrl, "/giris.jsp");
			} else {
				switch (Integer.parseInt(islemStr)) {
					case Islem.FAALIYET :
						faaliyet(request, response);
						break;
					case Islem.PROJE :
						proje(request, response);
						break;
					case Islem.DEGISTIR :
						degistir(request, response);
						break;									
					case Islem.FAALIYETGOSTER :
						faaliyetGoster(request, response);
						break;					
					case Islem.PROJEGOSTER :
						projeGoster(request, response);
						break;				
					case Islem.KULLANICIYARAT :
						kullaniciYarat(request, response);
						break;				
					case Islem.SIFREDEGISTIR :
						sifreDegistir(request, response);
						break;				
					case Islem.FAALIYETRAPOR :
						faaliyetRapor(request, response);
						break;						
					case Islem.FAALIYETSIL :
						faaliyetSil(request, response);
						break;		
					case Islem.ANAMENU :
						yonlendir(request, response);
						break;				
					case Islem.CIKIS :
						cikis(request, response);
						break;									
				}
			}

			forward(request, response);
		} catch (Throwable theException) {
			theException.printStackTrace();
		}
	}

	protected void faaliyet(
		HttpServletRequest req,
		HttpServletResponse resp) {
	}
	
	protected void proje(
		HttpServletRequest req,
		HttpServletResponse resp) {
	}
	
	protected void degistir(
		HttpServletRequest req,
		HttpServletResponse resp) {
	}
	
	protected void faaliyetGoster(
		HttpServletRequest req,
		HttpServletResponse resp) {
	}
	protected void projeGoster(
		HttpServletRequest req,
		HttpServletResponse resp) {
	}
	protected void kullaniciYarat(
			HttpServletRequest req,
			HttpServletResponse resp) {
	}
	protected void sifreDegistir(
			HttpServletRequest req,
			HttpServletResponse resp) {
	}
	protected void faaliyetRapor(
			HttpServletRequest req,
			HttpServletResponse resp) {
	}
	protected void faaliyetSil(
			HttpServletRequest req,
			HttpServletResponse resp) {
		}
	protected void yonlendir(
			HttpServletRequest req,
			HttpServletResponse resp) {
			}
	protected void cikis(
			HttpServletRequest req,
			HttpServletResponse resp) {
				}
	
	protected void removeSessionAttributes(HttpSession session) {
		/*		Eczane eczane = (Eczane) session.getAttribute("eczane");
				java.util.Enumeration enum = session.getAttributeNames();
				while (enum.hasMoreElements()) {
					session.removeAttribute((String) enum.nextElement());
				}
				session.setAttribute("eczane", eczane);
				*/
	}

	/**
	 * Insert the method's description here.
	 * Creation date: (12.08.2002 21:16:37)
	 * @return boolean
	 * @param param java.lang.String
	 */
	protected boolean isEmpty(String param) {
		return (param == null || param.equals(""));
	}


	protected Formatter getFormatter() {
		return ((Formatter) getServletContext().getAttribute("formatter"));
	}

}