if (typeof console == "undefined") { var console = {}; console.log = function () { }; }

var ie = (function(){
    var undef,
        v = 3,
        div = document.createElement('div'),
        all = div.getElementsByTagName('i');
    while (
        div.innerHTML = '<!--[if gt IE ' + (++v) + ']><i></i><![endif]-->',
        all[0]
    );
    return v > 4 ? v : undef;
}());

if ((ie && ie < 9) || location.href.indexOf("fakeie") >= 0) {
    document.body.className = "lamebrowser";
}
$.ajaxSetup({ cache: false, dataType: 'json' });

function fromDtoDate(dateStr) {
    return new Date(parseFloat(dateStr));
}
function toTwitterTime(a) {
    var b = new Date();
    var c = typeof a == "date" ? a : new Date(a);
    var d = b - c;
    var e = 1000, minute = e * 60, hour = minute * 60, day = hour * 24, week = day * 7;
    if (isNaN(d) || d < 0) { return "" }
    if (d < e * 7) { return "right now" }
    if (d < minute) { return Math.floor(d / e) + " secs ago" }
    if (d < minute * 2) { return "about 1 min ago" }
    if (d < hour) { return Math.floor(d / minute) + " mins ago" }
    if (d < hour * 2) { return "about 1 hour ago" }
    if (d < day) { return Math.floor(d / hour) + " hours ago" }
    if (d > day && d < day * 2) { return "yesterday" }
    if (d < day * 365) { return Math.floor(d / day) + " days ago" } else { return "over a year ago" }
}
function enc(html) {
    if (typeof html != "string") return html;
    return html.replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
function dirPath(path) {
    if (typeof path != "string") return path;
    var strPos = path.lastIndexOf('/', path.length - 1);
    if (strPos == -1) return path;
    return path.substr(0, strPos);
}
function _mkAjax(url, data, callback, type, method) {
    if ($.isFunction(data)) {
        callback = data, data = {};
    }
    return $.ajax({ type: method, url: url, data: data, success: callback, contentType: type });
}
$.extend({
    put: function (url, data, callback, type) {
        return _mkAjax(url, data, callback, type, 'PUT');
    },
    del: function (url, data, callback, type) {
        return _mkAjax(url, data, callback, type, 'DELETE');
    }
});

var href = "grfa", lastHref = "";

$(".btn").mousedown(function () { $(this).toggleClass("mousedown"); });
$(".btn").mouseup(function () { $(this).toggleClass("mousedown"); });

var refresh = function (callback, skipPushState) {
    if (!skipPushState && window.history.pushState)
        window.history.pushState(href, href.replace('/', ' / '), '#!' + href);

    var dirs = href.replace(/\/$/, "").split('/');
    var sb = '<div id="breadcrumb">';
    var sbDirs = "";
    for (var i = 0; i < dirs.length; i++) {
        var dir = dirs[i];
        if (!dir) continue;
        sb += (i == dirs.length - 1)
            ? '<strong>' + decodeURI(dir).replace("+", ' ') + '</strong>'
            : '<a href="#!' + sbDirs + dir + '">' + dir + '</a><b>/</b>';
        sbDirs += dir + "/";
    }
    $("#breadcrumb").html(sb + "</div>");

    var jqLs = $("#ls");
    $.getJSON(href, function (r) {
        var navBack = lastHref.length > href.length && lastHref.substr(0, href.length) == href,
            nextCls = navBack ? "results-0" : "results-2",
            hasResults = $("#ls TABLE").length == 1,
            cls = !hasResults ? "results-1" : nextCls;
        var sb = "<div class='" + cls + "'><table><thead><tr><th>name</th><th>age</th><th>size</th><th>user:group</th><th>Permissions</th></thead><tbody>";

        var file = r.FileItem;
        if (file) {
            if (!file.textFile) {
                location.href = href + "?ForDownload=true";
                setTimeout(function () { window.history.back() }, 1000);
                return;
            }

            var jqFile = $("#fileview");
            var fileSizeText = enc(file.length) + " bytes";
            if (file.length > file.preview.length) {
                fileSizeText = "Showing first " + enc(file.preview.length) + " bytes out of " + enc(file.length)
            }

            var sb =
                "<div class=\"navbar\">" +
                    "<div class=\"navbar-inner\">" +
                        "<div class=\"container\">" +
                            "<span class='txt brand'></span>" +
                            "<p class=\"nav navbar-text\">" + fileSizeText + "</p>" +
                            "<span class=\"nav divider-vertical\"></span>" +
                            "<p class='nav navbar-text'>" + toTwitterTime(fromDtoDate(file.modificationTime)) + "</p>" +
                            "<span class=\"nav divider-vertical\"></span>" +
                            "<a class=\"btn btn-info btn-small\" style=\"margin-top:7px\" href=\"" + href + "?ForDownload=true\">download file</a>" +
                        "</div>" +
                    "</div>" +
                "</div>" +
                "<textarea class=\"field span12\">" + enc(file.preview) + "</textarea>";
            jqFile.html(sb).show();

            //var height = $("#footer").position().top - $("#ls").position().top;
            //$("#fileview TEXTAREA").height(height - 65);

            $("#fileadmin").hide();
            jqLs.html("").hide();
            return;
        }

        $("#fileview").html("").hide();
        var dirList = r.FilesResponse;
        if (dirList) {
            if (dirs.length > 1) {
                var upHref = href.substr(0, href.lastIndexOf('/', href.length - 2));
                sb += "<tr><td><a class='up-dir' href='#!" + upHref + "'>..<a></td><td></td><td></td><td></td><td></td></tr>";
            }
            $.each(dirList.folders, function (i, dir) {
                sb += "<tr>" +
                        "<td><a class='dir' href=\"#!" + href + "/" + encodeURIComponent(dir.name) + "\">" +
                        "<b class='del' href='#!deletefile'></b>" + dir.name + "</a></td>" +
                        "<td>" + toTwitterTime(fromDtoDate(dir.modificationTime)) + "</td>" +
                        "<td>" + (typeof dir.len == "undefined" ? 0 : dir.len) + " files</td>" +
                        "<td>" + dir.owner + ":" + dir.group + "</td>" +
                        "<td>" + dir.permissionString + "</td>";
            });
            $.each(dirList.files, function (i, file) {
                var fileHref = "#!" + href + "/" + encodeURIComponent(file.name);


                sb += "<tr>" +
                        "<td><a class='file' href=\"" + fileHref + "\"><b class='del' href='#!deletefile'></b>" + file.name + "</a></td>" +
                    "<td>" + toTwitterTime(fromDtoDate(file.modificationTime)) + "</td>" +
                    "<td>" + file.len  + " bytes</td>" +
                    "<td>" + file.owner + ":" + file.group + "</td>" +
                    "<td>" + file.permissionString + "</td>";
            });
        }

        sb += "</tbody></table></div>";

        $("#fileadmin").show();
        $("#ls").show().append(sb);

        var jq1 = $(".results-1"), jq2 = $("." + nextCls), el1 = jq1[0], el2 = jq2[0];
        if (el1 && el2) {
            if (jq1.height() < jq2.height()) {
                jqLs.css({ "min-height": Math.max(jq1.height(), jq2.height()) + "px" });
            }
            else {
                jqLs.css({ "min-height": Math.min(jq1.height(), jq2.height()) + "px" });
            }

            jq1.addClass(navBack ? "slide-right" : "slide-left");
            jq2.addClass(navBack ? "slide-right" : "slide-left");

            jqLs.children().first().remove();
            jqLs.children().first()[0].className = "results-1";

        }
        else {
            $("#ls").css({ "min-height": jq1.height() + "px" });
        }
    });
}

window.onpopstate = function (e) {
    e = e || event;
    if (!e.state) return;
    href = e.state;
    refresh(null, true);
};

var clickHandlers = {
    grfa: function (el, e, href) {
        if (e.ctrlKey || e.shiftKey) {
            window.open('#!' + href);
            return;
        }
        refresh();
    },
    deletefile: function (el) {
        var fileHref = $(el.parentNode).attr('href').substr(2);
        $.del(fileHref, refresh);
        href = dirPath(fileHref), location.hash = "#!" + href;
    },
    root: function () {
        href = "grfa", location.hash = "#!grfa";
        refresh();
    }
}

$(document).click(function (e) {
    var attrHref, el = e.target;
    do { attrHref = el.getAttribute("href"); } while (!attrHref && (el = el.parentElement));
    if (!attrHref) return;

    if (attrHref.substr(0, 2) === "#!") {
        lastHref = href, href = attrHref.substr(2);
        var cmd = href.split('/')[0];

        var clickHandler = clickHandlers[cmd];
        if (clickHandler) {
            if (e.preventDefault) e.preventDefault();
            clickHandler(el, e, href);
        }
    }
});

var mkdir = function () {
    var dir = $("#newfoldername");
    var $submit = $("#mkdir");
    if (!dir.val()) {
        alert("Enter the name of the folder first");
        dir.focus();
        return;
    }
    if (dir.val().substring(0,1) == "/") {
        alert("Please remove any preceding /'s");
        dir.focus();
        return;
    }
    $.post(href + "/" + dir.val(), null, function () {
        dir.val("");
        $submit.attr("disabled", true);
        refresh();
    }).error(function(e) {
        dir.val("");
        $submit.attr("disabled", true);
        if(e.status == 200) {
            refresh();
        } else {
            alert('Internal Server Error: ' + e.responseText);
        }
    });
};

$("#newfoldername").keypress(function (e) { if (e.which == '13') mkdir(); });
$("#mkdir").click(mkdir);

var hash = location.hash.indexOf('#!') === 0 && location.hash.substr(2);
if (hash) href = hash;

$(document).ready(function(){
    $("#newfoldername").keyup(function() {
        var $submit = $("#mkdir");
        if(this.value.length > 0) {
            $submit.attr("disabled", false);
        } else {
            $submit.attr("disabled", true);
        }
    });
});

refresh();
