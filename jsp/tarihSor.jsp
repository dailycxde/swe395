<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<HTML>

<HEAD>
<%@ include file="baslik.jsp"%>
<%@ page 
language="java"
contentType="text/html; charset=iSO-8859-9"
import="ssk.faaliyet.model.Islem , ssk.faaliyet.model.*, java.util.*, ssk.faaliyet.veri.*"
%>
<jsp:useBean id="kullanici" class="ssk.faaliyet.model.Kullanici" scope="session"></jsp:useBean>
<jsp:useBean id="tarihBugun" class="java.lang.String" scope="session"></jsp:useBean>
<META http-equiv="Content-Type" content="text/html; charset=iSO-8859-9">
<META name="GENERATOR" content="IBM WebSphere Studio">
<LINK href="style/main.css" rel="stylesheet" type="text/css" >
<TITLE>
</TITLE>
</HEAD>
<BODY bgcolor="#c0c0c0">
<CENTER>
<FORM action="faaliyet" method="post" name="frmTarihSor">
<%
	if (request.getAttribute("hata") != null)	{
%>
<SPAN id="hata" class="hataMesaji"><%= request.getAttribute("hata") %></SPAN>
<%		
	}
%>

<INPUT type="hidden" name="islem" value="<%= Islem.FAALIYETGOSTER %>">
<TABLE border="1">

	<TBODY>
		<TR class="tableHeader">
			<TD width="457" height="30" colspan="4" align="center">FAALİYET GÖRÜNTÜLEME</TD>
		</TR>
		<TR>
			<TD width="104" class="tableHeading">Ad Soyad</TD>
			<TD width="113" class="tableData" colspan="3"><SELECT name="proje" class="text">
				<% ArrayList projeler = (ArrayList)session.getAttribute("projeler");
	for (int i =0 ; i< projeler.size() ; i++) {
	Proje proje = (Proje) projeler.get(i);
%>
				<OPTION class="text" value="<%=proje.getProjeKod()%>"> <%=proje.getProjeAdi()%></OPTION>
				<%
	}
%>
			</SELECT></TD>
		</TR>
		<TR>
			<TD align=left width="100" class="tableHeading">Başlangıç Tarihi</TD>
			<TD class="tableData"><INPUT type="text" class="text" name="basTarih" size="10" value="<%= tarihBugun%>"> (gg.aa.yyyy)</TD>
			<TD align=left width="100" class="tableHeading">Bitiş Tarihi</TD>
			<TD class="tableData"><INPUT type="text" class="text" name="bitTarih" size="10" value="<%= tarihBugun%>"> (gg.aa.yyyy)</TD>
		</TR>
		<TR> 
			<TD colspan="4" class="tableHeading" align="center"><CENTER><INPUT type="submit" class="buttonSubmit" name="goruntule"
				value="GÖSTER"></CENTER></TD>
		
		</TR>
	</TBODY>
</TABLE>
</FORM>

</CENTER>
</BODY>
</HTML>
