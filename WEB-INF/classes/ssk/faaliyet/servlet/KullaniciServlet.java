package ssk.faaliyet.servlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import ssk.faaliyet.model.Kullanici;
import ssk.faaliyet.veri.KullaniciDS;
import tai.foundation.model.FormField;
import tai.foundation.model.FormVerifier;
import tai.foundation.model.Hata;

/**
 * @version 1.0
 * @author
 */
public class KullaniciServlet extends AbstractServlet {

	/**
	 * @see ssk.faaliyet.servlet.AbstractServlet#void (HttpServletRequest,
	 *      HttpServletResponse)
	 */
	public void kullaniciYarat(HttpServletRequest req, HttpServletResponse resp) {

		Kullanici kullanici = new Kullanici();
		FormVerifier fv = new FormVerifier();
		fv.addField("kullaniciId", FormField.FT_STRING, "1", "15");
		fv.addField("sifre", FormField.FT_STRING, "1", "6");
		fv.addField("sifreTekrar", FormField.FT_STRING, "1", "6");
		fv.addField("adi", FormField.FT_STRING, "1", "20");
		fv.addField("soyadi", FormField.FT_STRING, "1", "20");
		fv.getFieldValues(req);
		// kullanici.setKullaniciId(req.getParameter("kullaniciId"));
		// kullanici.setSifre(req.getParameter("sifre"));
		// kullanici.setAdi(req.getParameter("adi"));
		// kullanici.setSoyadi(req.getParameter("soyadi"));

		try {
			fv.verify();
			kullanici.setKullaniciId(fv.getField("kullaniciId").strValue());
			kullanici.setSifre(fv.getField("sifre").strValue());
			kullanici.setAdi(fv.getField("adi").strValue());
			kullanici.setSoyadi(fv.getField("soyadi").strValue());

			if (fv.getField("sifre").strValue().equals(
					fv.getField("sifreTekrar").strValue())) {
				KullaniciDS.getInstance().kullaniciEkle(kullanici);
				req.setAttribute(mesaj,
						"Yeni Kullanýcý Baþarýyla Kaydedilmiþtir");
				req.setAttribute(nextUrl, "/jsp/kullaniciYarat.jsp");
			} else {
				req
						.setAttribute(hataMesaji,
								"Þifre ve þifre tekrar kýsmýna farklý þifreler girdiniz!");
				req.setAttribute(nextUrl, "/jsp/kullaniciYarat.jsp");
			}
		} catch (Hata h) {
			req.setAttribute("hata", h.getMessage());
			req.setAttribute(nextUrl, "/jsp/kullaniciYarat.jsp");
		} catch (Exception ex) {
		}

	}

	public void sifreDegistir(HttpServletRequest req, HttpServletResponse resp) {
		HttpSession session = req.getSession();
		
		Kullanici kullanici = new Kullanici();
	    Kullanici k = (Kullanici) session.getAttribute("kullanici");
		kullanici.setKullaniciId(k.getKullaniciId());
		FormVerifier fv = new FormVerifier();
		fv.addField("yeniSifre", FormField.FT_STRING, "1", "10");
		fv.addField("yeniSifreTekrar", FormField.FT_STRING, "1", "10");
		fv.addField("eskiSifre", FormField.FT_STRING, "1", "10");
		fv.getFieldValues(req);
		try {
			fv.verify();
			if (!fv.getField("yeniSifre").strValue().equals(fv.getField("yeniSifreTekrar").strValue())) {
				req.setAttribute("hata", "Þifre ve þifre tekrar kýsmýna farklý þifreler girdiniz!");
				req.setAttribute(nextUrl, "/jsp/sifreDegistir.jsp");
			} else {
				kullanici.setSifre(fv.getField("yeniSifre").strValue());
				KullaniciDS.getInstance().sifreDegistir(kullanici, fv.getField("eskiSifre").strValue());
				req.setAttribute(mesaj, "Þifreniz Baþarýyla Deðiþtirilmiþtir");
				req.setAttribute(nextUrl, "/jsp/sifreDegistir.jsp");
			}
		} catch (Hata hata) {
			req.setAttribute("hata", hata.getMessage());
			req.setAttribute(nextUrl, "/jsp/sifreDegistir.jsp");
		}

	}

}
