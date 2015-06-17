<%@ page contentType="text/html;charset=UTF-8" isELIgnored="false"%>
<%@ taglib prefix="t" tagdir="/WEB-INF/tags" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/fmt" prefix="fmt" %>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@ page import="static org.apache.giraffa.GiraffaConfiguration.getGiraffaTableName" %>
<%@ page import="org.apache.hadoop.hbase.client.HBaseAdmin" %>
<%@ page import="org.apache.hadoop.conf.Configuration" %>
<%@ page import="org.apache.giraffa.GiraffaConfiguration" %>


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

    request.setAttribute("tableMissing", tableMissing);
    request.setAttribute("tableName", tableName);
%>

<t:genericpage>
    <jsp:attribute name="subtitle"><fmt:message key="giraffa.htable.browser"/></jsp:attribute>
    <jsp:attribute name="isGiraffaPage">true</jsp:attribute>

    <jsp:body>
        <div class="container-fluid">
            <ul class="breadcrumb">
                <li><a href="#"><fmt:message key="menu.giraffa.name"/></a> <span class="divider">/</span></li>
                <li class="active"><fmt:message key="giraffa.htable.browser"/></li>
            </ul>
            <div style="overflow-x: auto;">
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
                       <table cellpadding="0" cellspacing="0" border="0" class="table table-striped table-bordered" id="namespace">
                           <thead>
                               <tr>
                                   <th>Key</th>
                                   <th>Name</th>
                                   <th>directory</th>
                                   <th>length</th>
                                   <th>blockSize</th>
                                   <th>block</th>
                                   <th>mtime</th>
                                   <th>atime</th>
                                   <th>permissions</th>
                                   <th>userName</th>
                                   <th>groupName</th>
                                   <th>symlink</th>
                                   <th>dsQuota</th>
                                   <th>nsQuota</th>
                                   <th>action</th>
                                   <th>replication</th>
                                   <th>state</th>
                               </tr>
                           </thead>
                       </table>
                    </c:otherwise>
                </c:choose>
            </div>
        </div>
        <script>
            $(document).ready(function() {
                var endKeyPosition;
                var dataTable = $('#namespace').dataTable( {
                    "bProcessing": true,
                    "bServerSide": true,
                    "bSort": false,
                    //"sPaginationType": "full_numbers",
                    "fnInfoCallback": function( oSettings, iStart, iEnd, iMax, iTotal, sPre ) {
                        endKeyPosition = iEnd;
                        return sPre;
                    },
                    "oLanguage": {
                        "sInfo": "Showing _START_ to _END_ of at least _TOTAL_ entries",
                        "sInfoFiltered": "(filtered from at least _MAX_ entries)"
                    },
                    "sAjaxSource": "/hbase/data.json",
                    "fnServerParams": function ( aoData ) {
                        //aoData.push( { "name": "iTotalRecords", "value": this.fnSettings().fnRecordsTotal() } );
                        //save last key only if use clicked on next page
                        if (this.fnGetData().length > 0 && endKeyPosition <= this.fnSettings()._iDisplayStart) {
                            aoData.push( { "name": "endKeyPosition", "value": endKeyPosition } );
                            aoData.push( { "name": "endKey", "value": decodeURIComponent(this.fnGetData()[this.fnGetData().length-1][0]) } );
                        }
                    },
                    "aoColumnDefs": [
                        { "sName": "Key", "aTargets": [ 0 ] },
                        { "sName": "Name", "aTargets": [ 1 ] },
                        { "sName": "directory", "aTargets": [ 2 ] },
                        { "sName": "length", "aTargets": [ 3 ] },
                        { "sName": "blockSize", "aTargets": [ 4 ] },
                        {
                            "sName": "block", "aTargets": [ 5 ],
                            "sClass": "blockCol",
                            "bSearchable ": "false",
                            "fnCreatedCell": function (nTd, sData, oData, iRow, iCol) {
                                if ( oData[iCol] !== "missing_column"
                                        && oData[iCol].blockDescriptors
                                        !== undefined && oData[iCol].totalBlockNumber > 0) {
                                    var blockHtml = "<table class='table table-striped table-bordered'>" +
                                            "<thead>" +
                                                "<th>Block Name</th><th>Size</th><th>Offset</th><th>Corrupt</th><th>Locations</th>" +
                                            "</thead>" +
                                            "<tbody>";

                                    jQuery.each(oData[iCol].blockDescriptors, function(key, val) {
                                        blockHtml +=
                                                "<tr>"
                                                +    "<td>" + val.blockName + "</td>"
                                                +    "<td>" + val.blockSize + "</td>"
                                                +     "<td>" + val.startOffset +"</td>"
                                                +     "<td>" + val.corrupt + "</td>"
                                                +     "<td>" + val.locs + "</td>"
                                                + "</tr>";
                                    });

                                    blockHtml += "</table>"

                                    var title = "Data Blocks (" +
                                            oData[iCol].totalBlockNumber + ")";
                                    if (oData[iCol].totalBlockNumber > oData[iCol].blockDescriptors.length) {
                                        title = "Data Blocks (first " + oData[iCol].blockDescriptors.length + " of " + oData[iCol].totalBlockNumber + ")";
                                    }
                                    $(nTd.firstChild).popover({title: title,
                                        html: true, content: blockHtml, trigger: 'manual' });
                                }
                            },
                            "mRender": function ( data, type, full ) {
                                            if (data == 'missing_column') {
                                                return "<span class='label'>no value</span>";
                                            } else if (data.totalBlockNumber > 0) {
                                                return "<button class=\"btn btn-mini btn-info\" onclick=\"$(this).popover('toggle')\" type=\"button\">" + data.totalBlockNumber + "</button>";
                                            } else {
                                                return "<span class='badge'>0</span>";
                                            }
                                        }
                        },
                        { "sName": "mtime",       "aTargets": [ 6 ] },
                        { "sName": "atime",       "aTargets": [ 7 ] },
                        { "sName": "permissions", "aTargets": [ 8 ] },
                        { "sName": "userName",    "aTargets": [ 9 ] },
                        { "sName": "groupName",   "aTargets": [ 10 ] },
                        { "sName": "symlink",     "aTargets": [ 11 ] },
                        { "sName": "dsQuota",     "aTargets": [ 12 ] },
                        { "sName": "nsQuota",     "aTargets": [ 13 ] },
                        { "sName": "action",      "aTargets": [ 14 ] },
                        { "sName": "replication", "aTargets": [ 15 ] },
                        { "sName": "state",       "aTargets": [ 16 ] },
                        {
                          "aTargets": [ '_all' ],
                            "mRender": function ( data, type, full ) {
                            if (data == 'missing_column') {
                                return "<span class='label'>no value</span>";
                            } else {
                                return data;
                            }
                        }
                    }]
                });
                dataTable.fnSetFilteringDelay(500);
                $("#namespace_filter input").popover({ title: "<strong class=\"text-error\">Warning!</strong>", content: "<p>Search can be slow on large datasets. HBase will perform full scan!</p>", trigger: 'hover', placement: "bottom", html: true });
            } );
        </script>
        <script type="text/javascript" src='/static/webjars/datatables/1.9.4/media/js/jquery.dataTables.min.js'></script>
        <script type="text/javascript" src='/static/js/DT_bootstrap.js'></script>
    </jsp:body>
</t:genericpage>