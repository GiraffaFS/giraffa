<%@ page contentType="text/html;charset=UTF-8" isELIgnored="false"%>
<%@ taglib prefix="t" tagdir="/WEB-INF/tags" %>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@ page import="static org.apache.giraffa.GiraffaConfiguration.getGiraffaTableName" %>
<%@ page import="org.apache.hadoop.hbase.client.HBaseAdmin" %>
<%@ page import="org.apache.hadoop.conf.Configuration" %>
<%@ page import="org.apache.giraffa.GiraffaConfiguration" %>
<%@ page import="org.apache.hadoop.hbase.io.ImmutableBytesWritable" %>
<%@ page import="org.apache.giraffa.RowKeyBytes" %>
<%@ page import="org.apache.hadoop.hbase.HTableDescriptor" %>
<%@ page import="java.util.*" %>
<%@ page import="org.apache.hadoop.hbase.HColumnDescriptor" %>


<%

    HBaseAdmin hBaseAdmin = (HBaseAdmin) application.getAttribute("hBaseAdmin");
    Configuration conf = (Configuration) application.getAttribute("conf");

    String tableName = getGiraffaTableName(conf);

    boolean tableMissing = false;
    if (!hBaseAdmin.isTableAvailable(tableName)) {
        tableMissing = true;
        request.setAttribute("tableMissing", tableMissing);
        return;
    }
    HTableDescriptor giraffaHTableDescriptor = hBaseAdmin.getTableDescriptor(tableName.getBytes());

    SortedMap<String, Object> giraffaHTableDetails = new TreeMap<String, Object>();
    giraffaHTableDetails.put("Table Name", RowKeyBytes.toString(giraffaHTableDescriptor.getName()));
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
            giraffaHTableDescriptor.getValues().entrySet()) {
        String key = RowKeyBytes.toString(e.getKey().get());
        String value = RowKeyBytes.toString(e.getValue().get());
        if (key == null) {
            continue;
        }
        String upperCase = key.toUpperCase();
        if (upperCase.equals(HTableDescriptor.IS_ROOT) || upperCase.equals(HTableDescriptor.IS_META)) {
            // Skip. Don't bother printing out read-only values if false.
            if (value.toLowerCase().equals(Boolean.FALSE.toString())) {
                continue;
            }
        }
        giraffaHTableDetails.put(RowKeyBytes.toString(e.getKey().get()), RowKeyBytes.toString(e.getValue().get()));
    }

    HashMap<String, Map<String, String>> giraffaHTableFamilies = new HashMap<String, Map<String, String>>();
    for (HColumnDescriptor columnDescriptor : giraffaHTableDescriptor.getFamilies()) {
        SortedMap<String, String> columnDescriptorValues = new TreeMap<String, String>();
        for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
                columnDescriptor.getValues().entrySet()) {
            columnDescriptorValues.put(RowKeyBytes.toString(e.getKey().get()), RowKeyBytes.toString(e.getValue().get()));
        }
        giraffaHTableFamilies.put(RowKeyBytes.toString(columnDescriptor.getName()), columnDescriptorValues);
    }

    request.setAttribute("giraffaHTableFamilies", giraffaHTableFamilies);
    request.setAttribute("giraffaHTableDetails", giraffaHTableDetails);
    request.setAttribute("tableMissing", tableMissing);
    request.setAttribute("tableName", tableName);

%>

<t:genericpage>
    <jsp:attribute name="subtitle">Table Descriptor</jsp:attribute>
    <jsp:attribute name="isGiraffaPage">true</jsp:attribute>

    <jsp:body>
        <div class="container">
            <ul class="breadcrumb">
                <li><a href="#"><fmt:message key="menu.giraffa.name"/></a> <span class="divider">/</span></li>
                <li class="active"><fmt:message key="giraffa.htable.descriptor"/></li>
            </ul>

            <div class="row">
                <div class="span12">
                    <div class="well">
                        <h3>Giraffa HTable Details</h3>
                        <c:choose>
                            <c:when test="${tableMissing}">
                                <div class="alert alert-error">
                                    <fmt:message key="giraffa.error.missing.table">
                                        <fmt:param value="${tableName}"/>
                                    </fmt:message>
                                    <br>
                                </div>
                            </c:when>
                            <c:otherwise>
                                <table class="table table-hover table-bordered">
                                    <tbody>
                                    <c:forEach items="${giraffaHTableDetails}" var="item">
                                        <tr>
                                            <td>${item.key}</td>
                                            <td>${item.value}</td>
                                        </tr>
                                    </c:forEach>
                                    </tbody>
                                </table>

                                <h3>HTable Families</h3>
                                <table class="table">
                                    <thead>
                                    <tr>
                                        <th>Family Name</th>
                                        <th>Values</th>
                                    </tr>
                                    </thead>
                                    <tbody>
                                    <c:forEach items="${giraffaHTableFamilies}" var="item">
                                        <tr>
                                            <td>${item.key}</td>
                                            <td>
                                                <table class="table table-condensed table-bordered">
                                                    <thead>
                                                    <tr>
                                                        <th>Property</th>
                                                        <th>Value</th>
                                                    </tr>
                                                    </thead>
                                                    <tbody>
                                                    <c:forEach items="${item.value}" var="item">
                                                        <tr>
                                                            <td>${item.key}</td>
                                                            <td>${item.value}</td>
                                                        </tr>
                                                    </c:forEach>
                                                    </tbody>
                                                </table>
                                            </td>
                                        </tr>
                                    </c:forEach>
                                    </tbody>
                                </table>
                            </c:otherwise>
                        </c:choose>
                    </div>
                </div>
            </div>
        </div>
    </jsp:body>
</t:genericpage>
