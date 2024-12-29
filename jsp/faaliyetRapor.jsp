<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<HTML>
<HEAD>
<%@ page 
contentType="text/html; charset=iSO-8859-9"
import="ssk.faaliyet.model.*, java.util.* , java.sql.Time, java.util.Date"
%>
<jsp:useBean id="formatterTime" class="java.text.SimpleDateFormat" scope="application"></jsp:useBean>
<jsp:useBean id="formatter" class="tai.foundation.model.Formatter" scope="application"></jsp:useBean>
<jsp:useBean id="liste" class="java.util.ArrayList" scope="session"></jsp:useBean>
<jsp:useBean id="kullanici" class="ssk.faaliyet.model.Kullanici" scope="session"></jsp:useBean>
<jsp:useBean id="tarih" class="java.lang.String" scope="session"></jsp:useBean>
<jsp:useBean id="sicilNo" class="java.lang.String" scope="session"></jsp:useBean>
<jsp:useBean id="proje" class="ssk.faaliyet.model.Proje" scope="session"></jsp:useBean>
<META http-equiv="Content-Type" content="text/html; charset=iSO-8859-9">
<META name="GENERATOR" content="IBM WebSphere Studio">
<META http-equiv="expires" CONTENT="0">
<META http-equiv="cache-control" CONTENT="no-cache">
<META http-equiv="pragma" CONTENT="no-cache">
<LINK href="style/main.css" rel="stylesheet" type="text/css" >
<TITLE>
faaliyetSonuc.jsp
</TITLE>
</HEAD>
<BODY style="font-family: Tahoma, Verdana, Arial; font-size: 8pt; background-color: white;
	text-align: center">
<FORM name="frmListe" method="post" action="faaliyet">
<INPUT type="hidden" name="islem">
<INPUT type="hidden" name="siraNo">
<CENTER><h2>FAALİYET RAPORU</h2></CENTER>



<TABLE width="%100">
<TBODY>
<TR>
<TD class="printHeading">PROJE:</TD><TD class="printData"><%= proje.getProjeAdi() %></TD>
					<TD colspan="3" class="printData"></TD>
				</TR>
<TR>
<TD class="printHeading">TARİH:</TD><TD class="printData"><%= tarih %></TD>
					<TD colspan="3" class="printData"></TD>
				</TR>
</TBODY>
</TABLE>
<TD>
<TABLE class="border" border="1" bordercolor="#000000" cellspacing="0">
	<TBODY>		
		<TR>
					
			<TD class="printHeading">TARIH</TD>
			<TD class="printHeading">ÇALIŞAN</TD>
			<TD class="printHeading">SAAT</TD>
			<TD class="printHeading">YER</TD>
			<TD class="printHeading">YAPILAN İŞ</TD>
		</TR>
<%
for(ListIterator iterator=liste.listIterator();iterator.hasNext();){
Faaliyet faaliyet = (Faaliyet) iterator.next();
%>		
		<TR>
			
			<TD class="printData"><%= formatter.format(faaliyet.getTarih())%></TD>
			<TD class="printData"><%= faaliyet.getKullanici().getAdSoyad() %></TD>
			<TD class="printData"><%= faaliyet.getCalismaSaati()%> </TD>
			<TD class="printData"><%= faaliyet.getCalisilanYer()%></TD>
			<TD class="printData"><%= faaliyet.getCalisma()%></TD>
		</TR>
<% 
}
%>
	</TBODY>
</TABLE>
</TD>
</TR>
<P></P>
</FORM>
</BODY>
</HTML>
