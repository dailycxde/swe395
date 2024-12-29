package ssk.faaliyet.servlet;

import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import ssk.faaliyet.model.Faaliyet;
import ssk.faaliyet.model.Kullanici;
import ssk.faaliyet.veri.ParametrelerDS;
import ssk.faaliyet.veri.ProjeDS;
import tai.foundation.model.Formatter;

public class GirisServlet extends HttpServlet {

	public void doGet(javax.servlet.http.HttpServletRequest request,
			javax.servlet.http.HttpServletResponse response)
			throws javax.servlet.ServletException, java.io.IOException {

		performTask(request, response);

	}

	/**
	 * Process incoming HTTP POST requests
	 * 
	 * @param request
	 *            Object that encapsulates the request to the servlet
	 * @param response
	 *            Object that encapsulates the response from the servlet
	 */
	public void doPost(javax.servlet.http.HttpServletRequest request,
			javax.servlet.http.HttpServletResponse response)
			throws javax.servlet.ServletException, java.io.IOException {

		performTask(request, response);

	}

	public void performTask(HttpServletRequest req, HttpServletResponse resp) {
		String nextUrl = "";
		try {
			HttpSession session = req.getSession();
			String islem = req.getParameter("islem");
			Kullanici k = null;
			getServletContext().setAttribute("admin", "admin");
			if (session == null || islem == null || islem.equals("")) {
				nextUrl = "/jsp/giris.jsp";
				// session = req.getSession(true);
			} else {
				String adi = req.getParameter("kullaniciId");
				String sifre = req.getParameter("sifre");
				try {
					k = ParametrelerDS.getInstance().kullaniciBul(adi, sifre);
				} catch (Exception e) {
					// SQL30081N Protocol Specific Error code "73"
					// SQLSTATE=08001 hatasý ilk loginde alýnýyor, çözmek için
					// iki kere deneniyor
					k = ParametrelerDS.getInstance().kullaniciBul(adi, sifre);
				}
				session.setAttribute("tarihBugun", getFormatter().format(Calendar.getInstance().getTime()));

				session.setAttribute("kullanici", k);
				session.setAttribute("faaliyet", new Faaliyet());
				session.setAttribute("projeler", ProjeDS.getInstance().projeGoster());
				nextUrl = "/jsp/menu.jsp";
			}
		} catch (Exception ex) {
			nextUrl = "/jsp/giris.jsp";
			req.setAttribute("hata", ex.getMessage());
		}
		try {
			getServletContext().getRequestDispatcher(nextUrl)
					.forward(req, resp);
		} catch (Throwable theException) {
			System.err.println(theException);
			nextUrl = "/jsp/giris.jsp";
		}
	}

	public Formatter getFormatter() {
		return (Formatter) getServletContext().getAttribute("formatter");
	}

	public void init() throws javax.servlet.ServletException {
		Formatter formatter = (Formatter) getServletContext().getAttribute(
				"formatter");
		if (formatter == null) {
			formatter = new Formatter();
			formatter
					.setDefaultDateFormatter(new SimpleDateFormat("dd.MM.yyyy"));
			NumberFormat f = NumberFormat.getInstance();
			f.setGroupingUsed(true);
			f.setMaximumFractionDigits(0);
			formatter.setDefaultNumberFormatter(f);
			getServletContext().setAttribute("formatter", formatter);
		}
		SimpleDateFormat formatterTime = new SimpleDateFormat("hh:mm");
		getServletContext().setAttribute("formatterTime", formatterTime);
	}

}