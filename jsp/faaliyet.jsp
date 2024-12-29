<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<HTML>

<HEAD>
<%@page contentType="text/html; charset=iSO-8859-9" import="ssk.faaliyet.model.*, java.util.*, ssk.faaliyet.veri.*" %>
<jsp:useBean id="formatter" class="tai.foundation.model.Formatter" scope="application"></jsp:useBean>
<jsp:useBean id="faaliyet" class="ssk.faaliyet.model.Faaliyet" scope="session"></jsp:useBean>
<jsp:useBean id="formatterTime" class="java.text.SimpleDateFormat" scope="application"></jsp:useBean>
<jsp:useBean id="calisilanYerEski" class="java.lang.String" scope="session"></jsp:useBean>
<jsp:useBean id="projeEski" class="java.lang.String" scope="session"></jsp:useBean>
<jsp:useBean id="projeAdiEski" class="java.lang.String" scope="session"></jsp:useBean>
<META http-equiv="Content-Type" content="text/html; charset=iSO-8859-9">
<META name="GENERATOR" content="IBM WebSphere Studio">
<LINK href="style/main.css" rel="stylesheet" type="text/css" >
<TITLE>
FAALIYET
</TITLE>
<%@ include file="baslik.jsp"%>
</HEAD>
<BODY bgcolor="#c0c0c0">
<FORM action="faaliyet" method="post" name="frmIslem">
<%
	if (request.getAttribute("hata") != null)	{
%>
<SPAN id="hata" class="hataMesaji"><%= request.getAttribute("hata") %></SPAN>
<%		
	}
%>

<INPUT type="hidden" name="islem" value="<%= Islem.FAALIYET %>">

<TABLE border="1">
	<TBODY>
		<TR class="tableHeader">
			<TD colspan="2" width="498" align="center">FAALIYET GİRİŞİ</TD>
		</TR>
		<TR>
			<TD class="tableHeading">Tarih</TD>
			<TD class="tableData"><INPUT type="text" class="text" name="tarih"  size="10" maxlength="10" value= "<%= formatter.format(faaliyet.getTarih()) %>" <%= faaliyet.getProjeKod()==null? "":"READONLY" %> ></TD>
		</TR>
		<TR>
			<TD class="tableHeading">İl Sag.Müd.</TD>
<% if (faaliyet.getProjeKod()==null){
%> 
			<TD class="tableData"><SELECT name="proje" class="text" >
<%  ArrayList projeler = ParametrelerDS.getInstance().projeListesi();
	for (int i =0 ; i< projeler.size() ; i++) {
	Proje proje = (Proje) projeler.get(i);
%>

				<OPTION class="text"  value="<%=proje.getProjeKod()%>" <%= proje.getProjeKod().equals(projeEski)?"SELECTED":"" %> ><%=proje.getProjeKod()+ " (" + proje.getProjeAdi() + ")"%></OPTION>
<%
	}
%>
			</SELECT></TD>
<%}else{
%>

<TD class="tableData"> 
<INPUT type="text" class="text" name="proje"  size="10" maxlength="10" value= "<%= faaliyet.getProjeKod()%>" <%= faaliyet.getProjeKod()==null? "":"READONLY" %> > </TD>	
<%
   }
%>	
		</TR>
		<TR>
			<TD class="tableHeading">Çalışma Saati</TD>
			<TD class="tableData"><INPUT type="text" name="toplamSaat" size="12" maxlength="11" class="text" value="<%= faaliyet.getCalismaSaati() %>"></TD>
		</TR>
		<TR>
			<TD class="tableHeading">Çalışılan Yer</TD>
			<TD class="tableData"><INPUT type="text" name="calisilanYer" size="25" maxlength="25" class="text"
				value="<%= faaliyet.getProjeKod()==null?calisilanYerEski:faaliyet.getCalisilanYer()%>" ></TD>
		</TR>
		<TR>
			<TD class="tableHeading">Faaliyet</TD>
			<TD class="tableData"><TEXTAREA rows="8" cols="54"  name="calisma" class="text"><%= faaliyet.getProjeKod()==null?"":faaliyet.getCalisma()%></TEXTAREA></TD>
		</TR>
		<TR>
			<TD class="tableHeading" colspan="2" align="center"><INPUT type="submit" name="Gonder" class="buttonSubmit"	value="KAYDET"></TD>
		</TR>
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
