<%@ page import="org.apache.hadoop.conf.Configuration" %>
<%@ page contentType="text/html;charset=UTF-8"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/fmt" prefix="fmt" %>
<%@ taglib prefix="t" tagdir="/WEB-INF/tags" %>

<%
    Configuration conf = (Configuration)getServletContext().getAttribute("conf");

    int port = conf.getInt("hbase.master.info.port", 60010);
    String hBaseServer = conf.get("hbase.master.info.bindAddress", "0.0.0.0");

    String hbaseAddress = "http://" + hBaseServer + ":" + port +"/master-status";
    request.setAttribute("hbaseAddress", hbaseAddress);
%>

<t:genericpage>
    <jsp:attribute name="title"><fmt:message key="menu.hbase.name"/></jsp:attribute>
    <jsp:attribute name="isHbasePage">true</jsp:attribute>

    <jsp:body>
        <div class="container fill">
            <div class="alert alert-info">
                <strong>HBase: <a href="${hbaseAddress}">${hbaseAddress}</a></strong>
            </div>

            <div class="well fill">
                <iframe src="${hbaseAddress}" frameborder="0" marginheight="0" marginwidth="0" scrolling="no"/>
            </div>
        </div>

    </jsp:body>
</t:genericpage>
