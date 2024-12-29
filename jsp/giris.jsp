<%@ page import="ssk.faaliyet.model.Islem" %>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
<%@page contentType="text/html; charset=iSO-8859-9" %>
<%@ include file="baslik.jsp"%>
<HEAD>
<META name="GENERATOR" content="IBM WebSphere Studio">
<META http-equiv="Content-Type" content="text/html; charset=iSO-8859-9">
<LINK href="style/main.css" rel="stylesheet" type="text/css">
</HEAD>
<body bgcolor="#c0c0c0">
<CENTER>


<form name="menu" method="post" action="giris">
<%
	if (request.getAttribute("hata") != null)	{
%>
<SPAN id="hata" class="hataMesaji"><%= request.getAttribute("hata") %></SPAN>
<%		
	}
%>
<INPUT type="hidden" name="islem" value="<%=Islem.GIRIS%>">
<center>
<table class="border" width="250" cellpadding="0" cellspacing="1" border="0">
<tr>
<td colspan="2" class="tableHeader" align="center">
KULLANICI GIRISI
</td>
</tr>
<tr>
<td class="tableHeading" align="left" width="50%">
&nbsp;KULLANICI ADI
</td>
<td class="tableData" align="left" width="50%">
&nbsp;<INPUT class="text" type="text" class="textfield" name="kullaniciId" size="12" maxlength="15">
</td>
</tr>
<tr>
<td class="tableHeading" align="left">
&nbsp;SIFRE
</td>
<td class="tableData" align="left">
&nbsp;<INPUT class="text" type="password" class="textfield" name="sifre" size="12" maxlength="6">
</td>
</tr>
<tr>
<td colspan="2" class="tableHeading" align="center">
<INPUT type="submit" class="buttonAlt" value="GIRIS">
</td>
</tr>
</table>
</center>
</form>
<SCRIPT>
	document.forms['menu'].kullaniciId.focus();
</SCRIPT>
<CENTER>
</body>
</html>