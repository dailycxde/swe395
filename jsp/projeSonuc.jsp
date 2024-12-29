<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<HTML>

<HEAD>
<%@ include file="baslik.jsp"%>
<%@ page 
contentType="text/html; charset=iSO-8859-9"
import="ssk.faaliyet.model.*, java.util.*"
%>
<jsp:useBean id="liste" class="java.util.ArrayList" scope="session"></jsp:useBean>
<META http-equiv="Content-Type" content="text/html; charset=iSO-8859-9">
<META name="GENERATOR" content="IBM WebSphere Studio">
<LINK href="style/main.css" rel="stylesheet" type="text/css" >
<TITLE>
</TITLE>
<SCRIPT>
	function degistir(siraNo)	{
		var frm = document.forms['frmProjeListe'];
		frm.islem.value = <%= Islem.DEGISTIR%>;
		frm.siraNo.value = siraNo;
		frm.submit(); 
	}
</SCRIPT>
</HEAD>
<BODY>
<FORM action="proje" method="post" name="frmProjeListe">
<INPUT type="hidden" name="islem">
<INPUT type="hidden" name="siraNo">
<TABLE border="1">
	<TBODY>
		<TR>
			<TD colspan="3" class="tableHeader">İL SAĞ.MÜD. LİSTESİ</TD>
		</TR>
		<TR>
			<TD class="tableHeading"></TD>
			<TD class="tableHeading">İL KODU</TD>
			<TD class="tableHeading">İL ADI</TD>
		</TR>
<%
for(ListIterator iterator=liste.listIterator();iterator.hasNext();){
Proje proje = (Proje) iterator.next();
%>		
		<TR>
			<TD class="tableHeading"><INPUT type="button" name="Degistir" class="button" value="Değiştir" onclick="degistir(<%= iterator.previousIndex() %>)"></TD>
			<TD class="tableData"><%= proje.getProjeKod()%></TD>
			<TD class="tableData"><%= proje.getProjeAdi()%></TD>
		</TR>
<% }%>
	</TBODY>
</TABLE>
</FORM>
</BODY>
</HTML>
