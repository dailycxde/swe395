<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<HTML>
<HEAD>
<%@ page 
language="java"
contentType="text/html; charset=iSO-8859-9"
%>
<META http-equiv="Content-Type" content="text/html; charset=iSO-8859-9">
<META name="GENERATOR" content="IBM WebSphere Studio">

</HEAD>
<BODY>
<FORM action="kullanici" method="post" name="sifreFrm">
<%
	if (request.getAttribute("mesaj") != null)	{
%>
<SPAN id="hata" class="errorMessage"><%= request.getAttribute("mesaj") %></SPAN>
<%		
	}
%>

</BODY>
</HTML>
