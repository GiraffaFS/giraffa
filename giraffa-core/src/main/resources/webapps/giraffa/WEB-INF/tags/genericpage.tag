<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/fmt" prefix="fmt" %>
<%@ attribute name="title" required="false" type="java.lang.String"%>
<%@ attribute name="subtitle" required="false" type="java.lang.String"%>
<%@ attribute name="isGiraffaPage" required="false" type="java.lang.Boolean"%>
<%@ attribute name="isHbasePage" required="false" type="java.lang.Boolean"%>
<%@ attribute name="isNamenodePage" required="false" type="java.lang.Boolean"%>

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <c:choose>
        <c:when test="${empty subtitle}">
            <c:choose>
                <c:when test="${empty title}">
                    <title><fmt:message key="webapp.title"/></title>
                </c:when>
                <c:otherwise>
                    <title>${title}</title>
                </c:otherwise>
            </c:choose>
        </c:when>
        <c:otherwise>
            <title><fmt:message key="webapp.title.short"/> &mdash; ${subtitle}</title>
        </c:otherwise>
    </c:choose>

    <link rel="stylesheet" type="text/css"
          href="/webjars/bootstrap/2.3.1/css/bootstrap.min.css">
    <link rel="stylesheet" type="text/css"
          href="/webjars/bootstrap/2.3.1/css/bootstrap-responsive.min.css">
    <link rel="stylesheet" type="text/css" href="/css/giraffa.css">
    <!-- Le HTML5 shim, for IE6-8 support of HTML5 elements -->
    <!--[if lt IE 9]>
    <script src="http://html5shim.googlecode.com/svn/trunk/html5.js"></script>
    <![endif]-->

    <script type="text/javascript" src='/webjars/jquery/1.9.0/jquery.min.js'></script>
    <script type="text/javascript" src='/webjars/bootstrap/2.3.1/js/bootstrap.js'></script>
    <script type="text/javascript" src='/webjars/jasny-bootstrap/2.3.0-j5/js/jasny-bootstrap.min.js'></script>

</head>
<body>


<div class="navbar navbar-fixed-top">
    <div class="navbar-inner">
        <div class="container">
            <a class="brand" href="#"><fmt:message key="webapp.title"/></a>
            <div class="navbar-content">
                <ul class="nav pull-right">
                    <li class="${isGiraffaPage ? 'active dropdown' : 'dropdown'}" >
                        <a data-toggle="dropdown" class="dropdown-toggle" role="button" href="#" id="drop1"><fmt:message key="menu.giraffa.name"/> <b class="caret"></b></a>
                        <ul aria-labelledby="drop1" role="menu" class="dropdown-menu">
                            <li role="presentation"><a href="/index.jsp" tabindex="-1" role="menuitem"><fmt:message key="giraffa.htable.descriptor"/></a></li>
                            <li role="presentation" class='disabled'><a href="/stats.jsp" tabindex="-1" role="menuitem"><fmt:message key="menu.giraffa.stats.name"/></a></li>
                            <li role="presentation"><a href="/htable.jsp" tabindex="-1" role="menuitem"><fmt:message key="giraffa.htable.browser"/></a></li>
                            <li role="presentation"><a href="/filebrowser.jsp" tabindex="-1" role="menuitem"><fmt:message key="giraffa.hdfs.browser"/></a></li>
                        </ul>
                    </li>
                    <li class="${isHbasePage ? 'active' : 'none'}">
                        <a href="/hbase.jsp"><fmt:message key="menu.hbase.name"/></a>
                    </li>
                    <li class="${isNamenodePage ? 'active' : 'none'}">
                        <a href="/namenode.jsp"><fmt:message key="menu.namenode.name"/></a>
                    </li>
                </ul>
            </div>
        </div>
    </div>
</div>

<jsp:doBody />

</body>
</html>