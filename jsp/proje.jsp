<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<HTML>

<HEAD>

<%@ page 
language="java"
contentType="text/html; charset=iSO-8859-9"
import="ssk.faaliyet.model.Islem"
%>
<%@ include file="baslik.jsp"%>
<jsp:useBean id="hata" class="java.lang.String" scope="request"></jsp:useBean>
<jsp:useBean id="proje" class="ssk.faaliyet.model.Proje" scope="session"></jsp:useBean>
<META http-equiv="Content-Type" content="text/html; charset=iSO-8859-9">
<META name="GENERATOR" content="IBM WebSphere Studio">
<LINK href="style/main.css" rel="stylesheet" type="text/css" >
<TITLE>
</TITLE>
</HEAD>
<BODY bgcolor="#c0c0c0">
<CENTER>
<FORM action="proje" method="post" name="frmProje">
<%
	if (request.getAttribute("hata") != null)	{
%>
<SPAN id="hata" class="hataMesaji"><%= request.getAttribute("hata") %></SPAN>
<%		
	}
%>

<INPUT type="hidden" name="islem" value="<%= Islem.PROJE %>">
<TABLE border="1" width="546" height="119">
	<TBODY>
		<TR class="tableHeader">
			<TD colspan="2" align="center">İL SAĞLIK MÜDÜRLÜKLERİ</TD></TR>
		<TR>
			<TD width="80" class="tableHeading">İl Kodu</TD>
			<TD width="92" class="tableData"><INPUT type="text" class="text" name="projeKod" size="20" value="<%= proje.getProjeKod()==null?"":proje.getProjeKod() %>" <%= proje.getProjeKod()==null? "":"READONLY" %> maxlength="10"></TD>
		</TR>
		<TR>
			<TD width="80" class="tableHeading">İl Adı</TD>
			<TD width="92" class="tableData"><INPUT type="text" class="text" name="projeAdi" size="40" value="<%= proje.getProjeKod()==null?"":proje.getProjeAdi()%>" maxlength="40"></TD>
		</TR>
		<TR>
			<TD  class="tableHeading" colspan="2" align="center"><INPUT type="submit" class="buttonSubmit"
				name="gonder" value="KAYDET"></TD>
		</TR>
	</TBODY>
</TABLE></FORM>
</CENTER>
<%
	if (request.getAttribute("mesaj") != null)	{
%>
<SPAN id="hata" class="mesaj"><%= request.getAttribute("mesaj") %></SPAN>
<%		
	}
%>
</BODY>
</HTML>
