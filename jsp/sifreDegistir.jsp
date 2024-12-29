<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<HTML>

<HEAD>
<%@ include file="baslik.jsp"%>
<%@ page language="java" contentType="text/html; charset=iSO-8859-9"
	import="ssk.faaliyet.model.*,java.util.*,ssk.faaliyet.veri.*"%>
<META http-equiv="Content-Type" content="text/html; charset=iSO-8859-9">
<META name="GENERATOR" content="IBM WebSphere Studio">
<LINK href="style/main.css" rel="stylesheet" type="text/css">
<TITLE>sifreDegistir.jsp</TITLE>
</HEAD>
<BODY>
<FORM action="kullanici" method="post" name="sifreFrm">
<%
if (request.getAttribute("hata") != null) {
%> <SPAN id="hata" class="hataMesaji"><%=request.getAttribute("hata")%></SPAN>
<%
}
%> <INPUT type="hidden" name="islem" value="<%= Islem.SIFREDEGISTIR %>">
<TABLE border="1">
	<TBODY>
		<TR class="tableHeader">
			<TD colspan="2">ŞİFRE DEĞİŞTİRME EKRANI</TD>
		</TR>
		<TR>
			<TD class="tableHeading">Eski Şifre</TD>
			<TD class="tableData"><INPUT type="password" class="text"
				name="eskiSifre" size="10" maxlength="10"></TD>
		</TR>
		<TR>
			<TD class="tableHeading">Yeni Şifre</TD>
			<TD class="tableData"><INPUT type="password" class="text"
				name="yeniSifre" size="10" maxlength="10"></TD>
		</TR>
		<TR>
			<TD class="tableHeading">Yeni Şifre (Tekrar)</TD>
			<TD class="tableData"><INPUT type="password" class="text"
				name="yeniSifreTekrar" size="10" maxlength="10"></TD>
		</TR>
		<TR>
			<TD colspan="2" class="tableHeading"><INPUT type="submit"
				class="buttonSubmit" name="kaydet" value="KAYDET"></TD>
		</TR>
	</TBODY>
</TABLE>
<%
if (request.getAttribute("mesaj") != null) {
%> <SPAN id="hata" class="mesaj"><%=request.getAttribute("mesaj")%></SPAN>
<%
}
%>
</FORM>
</BODY>
</HTML>
