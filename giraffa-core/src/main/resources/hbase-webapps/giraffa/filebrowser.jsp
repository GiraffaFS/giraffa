<%@ page contentType="text/html;charset=UTF-8" isELIgnored="false"%>
<%@ taglib prefix="t" tagdir="/WEB-INF/tags" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/fmt" prefix="fmt" %>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>

<t:genericpage>
    <jsp:attribute name="subtitle"><fmt:message key="giraffa.hdfs.browser"/></jsp:attribute>
    <jsp:attribute name="isGiraffaPage">true</jsp:attribute>

    <jsp:body>
        <link href="/static/webjars/jasny-bootstrap/2.3.0-j5/css/jasny-bootstrap.min.css"
              rel="stylesheet">

        <div class="container">
            <ul class="breadcrumb">
                <li><a href="#"><fmt:message key="menu.giraffa.name"/></a> <span class="divider">/</span></li>
                <li class="active"><fmt:message key="giraffa.hdfs.browser"/></li>
            </ul>

            <a class="btn btn-mini btn-info pull-right" href="#!root" class="btn"><strong>Go to /&nbsp;&nbsp;</strong></a>

            <div id="breadcrumb"></div>
            <div id="ls"></div>
            <div id="fileadmin">
                <div class="input-append pull-left" style="margin-right: 10px; padding-top:10px">
                    <input  id="newfoldername" class="span3" type="text">
                    <button id="mkdir" class="btn" disabled="true" type="button"><fmt:message key="giraffa.hdfs.browser.mkdir"/></button>
                </div>

                <form id="uploadForm" enctype="multipart/form-data" method="post" action="/grfa">
                    <div class="fileupload fileupload-new" data-provides="fileupload" style="margin-bottom: 0px">
                        <div class="input-append">
                            <div class="uneditable-input span3">
                                <i class="icon-file fileupload-exists"></i>
                                <span class="fileupload-preview"></span>
                            </div>
                            <span class="btn btn-file">
                                <span class="fileupload-new">Upload file</span>
                                <span class="fileupload-exists">Change</span>
                                <input type="file" multiple="" name="myfile[]">
                            </span>
                            <a href="#" class="btn fileupload-exists" data-dismiss="fileupload">Remove</a>
                            <button id="uploadBtn" type="submit" class="btn fileupload-exists">Upload</button>
                        </div>
                    </div>

                    <a href="#" id="clearLog" style="display: none; margin-bottom: 10px" class="btn btn-mini btn-info pull-right">Clear</a>

                    <div class="progress progress-success progress-striped">
                        <div class="bar"></div>
                        <div class="percent">0%</div>
                    </div>
                    <div id="status"></div>
                </form>
            </div>
            <div id="fileview"></div>
        </div>
        <script>
            (function() {
                var bar = $('.bar');
                var percent = $('.percent');
                var progress = $('.progress');
                var clearLog = $('#clearLog');
                var status = $('#status');

                $("#uploadBtn").click(function (e) {
                    $('#uploadForm').ajaxForm({
                        dataType:  'json',
                        url: href,
                        clearForm: true,
                        beforeSubmit:  disableButton,
                        beforeSend: function() {
                            status.empty();
                            progress.show();
                            var percentVal = '0%';
                            bar.width(percentVal);
                            percent.html("<strong>" + percentVal + "</strong>");
                        },
                        uploadProgress: function(event, position, total, percentComplete) {
                            var percentVal = percentComplete + '%';
                            bar.width(percentVal);
                            percent.html("<strong>" + percentVal + "</strong>");
                            console.log(percentVal, position, total);
                        },
                        complete: function(xhr) {
                            var percentVal = '100%';
                            clearLog.show();
                            bar.width(percentVal);
                            $('.fileupload').fileupload('reset');
                            percent.html("<strong>" + percentVal + "</strong>");
                        },
                        success: function(responseText, statusText, xhr, $form) {
                            var sb;
                            var uploadResult = responseText.UploadResult;
                            if (uploadResult) {
                                sb = "<strong>Result:</strong>" +
                                "<dl class=\"dl-horizontal\">";
                                    for (idx in uploadResult.error) {
                                        sb += "<dt style='width: 80px;'><span class='label label-important'>Error</span></dt><dd style='margin-left: 90px;'>" +  uploadResult.error[idx] + "</dd>"
                                    }
                                    for (idx in uploadResult.success) {
                                        sb += "<dt style='width: 80px;'><span class='label label-success'>Success</span></dt><dd style='margin-left: 90px;'>" +  uploadResult.success[idx] + "</dd>"
                                    }
                                sb += "</dl>";
                                if (uploadResult.success.length > 0) {
                                    refresh();
                                }
                            }
                            status.html(sb);
                        }

                    });
                });

                clearLog.click(function (e) {
                    status.empty();
                    var percentVal = '0%';
                    bar.width(percentVal);
                    percent.html("<strong>" + percentVal + "</strong>");
                    progress.hide();
                    clearLog.hide();
                });


                function disableButton(data, $form, opts) {
                    $('uploadProgress').find('input:submit').val("Please wait...").click(function(e){
                        e.preventDefault();
                        return false;
                    });
                }

            })();
        </script>
        <script type="text/javascript"
                src='/static/webjars/jasny-bootstrap/2.3.0-j5/js/jasny-bootstrap.min.js'></script>
        <script type="text/javascript"
                src='/static/webjars/jquery-form/3.28.0-2013.02.06/jquery.form.js'></script>
        <script type="text/javascript" src='/static/js/filebrowser.js'></script>
    </jsp:body>
</t:genericpage>