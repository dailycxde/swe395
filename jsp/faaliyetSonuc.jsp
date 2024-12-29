<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<HTML>
<HEAD>
<%@ page 
contentType="text/html; charset=iSO-8859-9"
import="ssk.faaliyet.model.*, java.util.* ,java.sql.Time, java.util.Date " 
%>
<jsp:useBean id="formatterTime" class="java.text.SimpleDateFormat" scope="application"></jsp:useBean>
<jsp:useBean id="formatter" class="tai.foundation.model.Formatter" scope="application"></jsp:useBean>
<jsp:useBean id="kullanici" class="ssk.faaliyet.model.Kullanici" scope="session"></jsp:useBean>
<jsp:useBean id="liste" class="java.util.ArrayList" scope="session"></jsp:useBean>
<jsp:useBean id="sicilNo" class="java.lang.String" scope="session"></jsp:useBean>
<%@ include file="baslik.jsp"%>
<META http-equiv="Content-Type" content="text/html; charset=iSO-8859-9">
<META name="GENERATOR" content="IBM WebSphere Studio">
<LINK href="style/main.css" rel="stylesheet" type="text/css" >
<TITLE>
faaliyetSonuc.jsp
</TITLE>
<SCRIPT>
	function degistir(siraNo)	{
		var frm = document.forms['frmListe'];
		frm.islem.value = <%= Islem.DEGISTIR%>;
		frm.siraNo.value = siraNo;
		frm.submit(); 
	}

	function fnRaporAl(){
		var frm = document.forms['frmListe'];
		frm.islem.value = <%= Islem.FAALIYETRAPOR %>;
		frm.submit(); 
	}
	function raporSil(siraNo){
		var frm = document.forms['frmListe'];
		frm.islem.value = <%= Islem.FAALIYETSIL %>;
		frm.siraNo.value = siraNo;
		frm.submit(); 
	}
</SCRIPT>

</HEAD>
<BODY>
<FORM name="frmListe" method="post" action="faaliyet">
<INPUT type="hidden" name="islem">
<INPUT type="hidden" name="siraNo">
<TABLE border="1" width="%100">
	<TBODY>
		<TR>
			<TD colspan="7" class="tableHeader">FAALİYET LİSTESİ</TD>
		</TR>
		<TR>
			<TD class="tableHeading"></TD>
			<TD class="tableHeading">TARİH</TD>
			<TD class="tableHeading">AD SOYAD</TD>
			<TD class="tableHeading">İL SAĞ.MÜD</TD>
			<TD class="tableHeading">SAAT</TD>
			<TD colspan="2" class="tableHeading">YAPILAN İŞ</TD>
	    </TR>
<%
for(ListIterator iterator=liste.listIterator();iterator.hasNext();){
Faaliyet faaliyet = (Faaliyet) iterator.next();
%>		
		<TR>
			
			<TD class="tableHeading"><INPUT type="button" name="Degistir" value="Değiştir" class="button" onclick="degistir(<%= iterator.previousIndex() %>)" <%= (kullanici.getKullaniciId().equals(sicilNo))?"":"ENABLED" %> ></TD>
			<TD class="tableData2"><%= formatter.format(faaliyet.getTarih())%></TD>
			<TD class="tableData2"><%= faaliyet.getKullanici().getAdSoyad()%></TD>
			<TD class="tableData2"><%= faaliyet.getProjeAdi()%></TD>
			<TD class="tableData2"><%= faaliyet.getCalismaSaati()%> </TD>
			<TD class="tableData2"><%= faaliyet.getCalisma()%></TD>
			<TD class="tableHeading"><INPUT type="button" name="sil" class="button" value="  Sil  " onclick="raporSil(<%= iterator.previousIndex() %>)" <%= (kullanici.getKullaniciId().equals(sicilNo))?"":"ENABLED" %>></TD>
		</TR>
<% 
}
%>
		<TR>
			<TD colspan="7" align="right" class="tableHeading"><INPUT type="button" class="buttonSubmit" name="raporAl"
				value="RAPOR AL" onclick="fnRaporAl()"></TD>
		</TR>
	</TBODY>
</TABLE>

</FORM>
</BODY>
</HTML>
