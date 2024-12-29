<%@ page 
language="java" 
contentType="text/html; charset=iSO-8859-9"
import="ssk.faaliyet.model.Islem"
%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">

<%@page import="ssk.faaliyet.model.Kullanici"%>
<HTML>

<HEAD>
<%@ include file="baslik.jsp"%>
<META http-equiv="Content-Type" content="text/html; charset=iSO-8859-9">
<META name="GENERATOR" content="IBM WebSphere Studio">
<LINK href="style/main.css" rel="stylesheet" type="text/css" >
<jsp:useBean id="kullanici" class="ssk.faaliyet.model.Kullanici" scope="session"></jsp:useBean>
<TITLE>
FAALİYET
</TITLE>
<SCRIPT>
function faaliyet()	{
	var frm = document.forms['frmMenu'];
	frm.islem.value = <%= Islem.FAALIYET %>;
	frm.submit();
}
function proje() {
	var frm = document.forms['frmMenu'];
	frm.islem.value = <%= Islem.PROJE %>;
	frm.submit();
}
function faaliyetGoruntule() {
	var frm = document.forms['frmMenu'];
	frm.islem.value = <%= Islem.FAALIYETGOSTER %>;
	frm.submit();
}
function projeGoruntule() {
	var frm = document.forms['frmMenu'];
	frm.islem.value = <%= Islem.PROJEGOSTER %>;
	frm.submit();
}
function kullaniciYarat() {
	var frm = document.forms['frmMenu'];
	frm.islem.value = <%= Islem.KULLANICIYARAT %>;
	frm.submit();
}
function sifreDegistir() {
	var frm = document.forms['frmMenu'];
	frm.islem.value = <%= Islem.SIFREDEGISTIR %>;
	frm.submit();
}
function cikisYap() {
	var frm = document.forms['frmMenu'];
	frm.islem.value = <%= Islem.CIKIS %>;
	frm.submit();
}
</SCRIPT>
</HEAD>
<BODY bgcolor="#c0c0c0">
<CENTER>
<FORM action="menu" method="post" name="frmMenu">
<INPUT type="hidden" name="islem">
<% if (application.getAttribute("admin").equals(kullanici.getKullaniciId())) { %>
<TABLE border="1">
	<TBODY>
		<TR>
			<TD class="tableHeading"><CENTER><INPUT type="button" width="100" name="Proje" class="button" value="İL SAĞ.MÜD. GİRİŞİ" 
				onclick="proje()"> </CENTER></TD>
		</TR>
		<TR>
			<TD class="tableHeading"><CENTER><INPUT type="button" width="100" name="projeListesi" class="button" value="İL SAĞ.MÜD. LİSTESİ" 
			onclick="projeGoruntule()"></CENTER></TD>
		</TR>
		<TR>
			<TD class="tableHeading"><CENTER><INPUT type="button" width="100" name="FaaliyetGoruntule" class="button" value="FAALİYET GÖRÜNTÜLEME" 
				onclick="faaliyetGoruntule()"></CENTER> </TD>
		</TR>
		<TR>
			<TD class="tableHeading"><CENTER><INPUT type="button" name="kullanici" class="button" value="YENİ KULLANICI"
				onclick="kullaniciYarat()"></CENTER></TD>
		</TR>
		<TR>
			<TD class="tableHeading"><CENTER><INPUT type="button" name="sifre" class="button" value="ŞİFRE DEĞİŞTİRME"
				onclick="sifreDegistir()"></CENTER></TD>
		</TR>
		<TR>
			<TD class="tableHeading"><CENTER><INPUT type="button" class="button" name="cikis" value="ÇIKIŞ" onclick="cikisYap()"></CENTER></TD>
		</TR>
	</TBODY>
</TABLE>
<% } else { %>
<TABLE border="1">
	<TBODY>
	<TR>
		<TD class="tableHeading"><CENTER><INPUT type="button" width="2000" class="button" name="Faaliyet" value="FAALİYET GİRİŞİ"
			onclick="faaliyet()"></CENTER></TD>
	</TR>
		<TR>
			<TD class="tableHeading"><CENTER><INPUT type="button" name="sifre" class="button" value="ŞİFRE DEĞİŞTİRME"
				onclick="sifreDegistir()"></CENTER></TD>
		</TR>
		<TR>
			<TD class="tableHeading"><CENTER><INPUT type="button" class="button" name="cikis" value="ÇIKIŞ" onclick="cikisYap()"></CENTER></TD>
		</TR>
	</TBODY>
</TABLE>
<% } %>
</FORM>

</CENTER>
</BODY>
</HTML>
