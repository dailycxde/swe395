<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<HTML>

<HEAD>
<%@ page 
language="java"  
contentType="text/html; charset=iSO-8859-9"
import="ssk.faaliyet.model.*, java.util.*, ssk.faaliyet.veri.*" 
%>
<%@ include file="baslik.jsp"%>
<META http-equiv="Content-Type" content="text/html; charset=iSO-8859-9">
<META name="GENERATOR" content="IBM WebSphere Studio">
<LINK href="style/main.css" rel="stylesheet" type="text/css" >
<TITLE>
kullaniciYarat.jsp
</TITLE>
</HEAD>
<BODY>
<FORM action="kullanici" method="post" name="kullaniciFrm">
<%
	if (request.getAttribute("hata") != null)	{
%>
<SPAN id="hata" class="hataMesaji"><%= request.getAttribute("hata") %></SPAN>
<%		
	}
%>


<INPUT type="hidden" name="islem" value="<%= Islem.KULLANICIYARAT %>">
<TABLE border="1">
	<TBODY>
		<TR>
			<TD colspan="2" class="tableHeader">KULLANICI TANIMLAMA EKRANI</TD></TR>
		<TR>
			<TD class="tableHeading">Kullanıcı ID</TD>
			<TD class="tableData"><INPUT type="text" class="text" name="kullaniciId" size="10" maxlength="15"></TD>
		</TR>
		<TR>
			<TD class="tableHeading">Şifre</TD>
			<TD class="tableData"><INPUT type="password" class="text" name="sifre" size="10" maxlength="6"></TD>
		</TR>
		<TR>
			<TD class="tableHeading">Şifre (Tekrar)</TD>
			<TD class="tableData"><INPUT type="password" class="text" name="sifreTekrar" size="10" maxlength="6"></TD>
		</TR>
		<TR>
			<TD class="tableHeading">Adı</TD>
			<TD class="tableData"><INPUT type="text" class="text" name="adi" size="20" maxlength="20"></TD>
		</TR>
		<TR>
			<TD class="tableHeading">Soyadı</TD>
			<TD class="tableData"><INPUT type="text" name="soyadi" class="text" size="20" maxlength="20"></TD>
		</TR>
		<TR>
			<TD colspan="2" class="tableHeading"><INPUT type="submit" name="kaydet" class="buttonSubmit" value="KAYDET"></TD></TR>
	</TBODY>
</TABLE>
<%
	if (request.getAttribute("mesaj") != null)	{
%>
<SPAN id="hata" class="mesaj"><%= request.getAttribute("mesaj") %></SPAN>
<%		
	}
%>
</FORM>
</BODY>
</HTML>
