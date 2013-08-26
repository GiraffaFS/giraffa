<%@ page import="org.apache.hadoop.conf.Configuration" isELIgnored="false" %>
<%@ page import="org.apache.hadoop.hdfs.DFSConfigKeys" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/fmt" prefix="fmt" %>
<%@ page contentType="text/html;charset=UTF-8"%>
<%@ taglib prefix="t" tagdir="/WEB-INF/tags" %>

<%
    Configuration conf = (Configuration)getServletContext().getAttribute("conf");

    String namenodeAddress = "http://" + conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);

    request.setAttribute("namenodeAddress", namenodeAddress);
%>

<t:genericpage>
    <jsp:attribute name="title"><fmt:message key="menu.namenode.name"/></jsp:attribute>
    <jsp:attribute name="isNamenodePage">true</jsp:attribute>

    <jsp:body>
        <div class="container fill">
            <div class="alert alert-info">
                <strong>Namenode: <a href="${namenodeAddress}">${namenodeAddress}</a></strong>
            </div>

            <div class="well fill">
                <iframe src="${namenodeAddress}" frameborder="0" marginheight="0" marginwidth="0" scrolling="no"/>
            </div>
        </div>
    </jsp:body>
</t:genericpage>
