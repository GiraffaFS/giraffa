<%@ page contentType="text/html;charset=UTF-8" isELIgnored="false"%>
<%@ taglib prefix="t" tagdir="/WEB-INF/tags" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/fmt" prefix="fmt" %>
<%@ page contentType="text/html;charset=UTF-8"%>

<t:genericpage>
    <jsp:attribute name="subtitle"><fmt:message key="menu.giraffa.stats.name"/></jsp:attribute>
    <jsp:attribute name="isGiraffaPage">true</jsp:attribute>

    <jsp:body>
        <div class='container'>
            <h1><fmt:message key="menu.giraffa.stats.name"/></h1>
        </div>
    </jsp:body>
</t:genericpage>
