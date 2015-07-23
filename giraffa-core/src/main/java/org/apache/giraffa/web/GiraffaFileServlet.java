package org.apache.giraffa.web;

import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraffa.GiraffaFileSystem;
import org.apache.giraffa.RowKey;
import org.apache.giraffa.RowKeyBytes;
import org.apache.giraffa.RowKeyFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.giraffa.web.GiraffaWebJsonWrappers.FileItem;
import org.apache.giraffa.web.GiraffaWebJsonWrappers.FilesResponse;

public class GiraffaFileServlet extends HttpServlet {
  private static final Log LOG = LogFactory.getLog(GiraffaFileServlet.class);
  private static final long serialVersionUID = 1L;

  static final String[] TEXT_FILE_EXT =
      {".txt", ".proj", ".java", ".css", ".htm", ".html", ".xml", ".js"};

  private static final int TEXT_PREVIEW_SIZE = 1024 * 5; //5Kb

  public final SimpleDateFormat dateForm =
      new SimpleDateFormat("yyyy-MM-dd HH:mm");

  private transient ObjectMapper mapper = new ObjectMapper();
  private transient GiraffaFileSystem grfs;

  @Override
  public void init() throws ServletException {
    super.init();
    mapper.configure(SerializationConfig.Feature.WRAP_ROOT_VALUE, true);
    try {
      grfs = GiraffaWebUtils.getGiraffaFileSystem(getServletContext());
    } catch (IOException e) {
      throw new ServletException(e);
    }
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException {

    Path srcFile = GiraffaWebUtils.extractPath(request);
    FileStatus fileStatus = grfs.getFileStatus(srcFile);

    if (fileStatus.isFile()) {
      if (Boolean.valueOf(request.getParameter("ForDownload"))) {
        FSDataInputStream is = null;
        try {
          is = grfs.open(srcFile);

          response.setContentType("application/octet-stream");
          response.setContentLength(
              GiraffaWebUtils.safeLongToInt(fileStatus.getLen()));
          response.setHeader("Content-Disposition",
              "attachment; filename=\"" + srcFile.getName() + "\"");

          IOUtils.copy(is, response.getOutputStream());
        } catch (IOException ex) {
          response.getWriter().print(String
              .format("Error opening file %s. Details: %s",
                  fileStatus.getPath(),
                  ExceptionUtils.getCause(ex).getMessage()));
        } finally {
          if (is != null) {
            is.close();
          }

        }
        return;
      }

      FileItem fItem = convertToFileItem(fileStatus);
      if (fItem.isTextFile()) {
        FSDataInputStream is = null;
        try {
          is = grfs.open(srcFile);
          if (fItem.getLen() > TEXT_PREVIEW_SIZE) {
            byte[] content = IOUtils.toByteArray(is, TEXT_PREVIEW_SIZE);
            fItem.setPreview(new String(content, Charset.defaultCharset()));
          } else {
            fItem.setPreview(IOUtils.toString(is));
          }
        } catch (IOException ex) {
          //noinspection ThrowableResultOfMethodCallIgnored
          response.getWriter().print(String
              .format("Error opening input stream for %s. Details: %s",
                  fileStatus.getPath(),
                  ExceptionUtils.getCause(ex).getMessage()));
        } finally {
          if (is != null) {
            is.close();
          }
        }

      }
      mapper.writeValue(response.getWriter(), fItem);
      return;
    }

    FileStatus[] status = grfs.listStatus(srcFile);
    FilesResponse filesResponse = new FilesResponse();

    for (FileStatus stat : status) {
      Path cur = stat.getPath();
      String mdate = dateForm.format(new Date(stat.getModificationTime()));

      FileItem fItem = convertToFileItem(stat);

      if (fItem.isDir()) {
        filesResponse.getFolders().add(fItem);
      } else {
        filesResponse.getFiles().add(fItem);
      }

      int maxReplication = 3, maxLen = 10, maxOwner = 10, maxGroup = 10;

      if (LOG.isDebugEnabled()) {
        LOG.debug((stat.isDirectory() ? "d" : "-") + stat.getPermission() + " ");
        LOG.debug(String.format("%" + maxReplication + "s ",
            (stat.isFile() ? stat.getReplication() : "-")));
        LOG.debug(String.format("%-" + maxOwner + "s ", stat.getOwner()));
        LOG.debug(String.format("%-" + maxGroup + "s ", stat.getGroup()));
        LOG.debug(String.format("%" + maxLen + "d ", stat.getLen()));
        LOG.debug(mdate + " ");
        LOG.debug(cur.toUri().getPath());
      }
    }

    mapper.writeValue(response.getWriter(), filesResponse);

  }

  private FileItem convertToFileItem(FileStatus stat) throws IOException {
    RowKey rowKey = RowKeyFactory.newInstance(stat.getPath().toUri().getPath());
    FileItem file = new FileItem(rowKey, stat.getModificationTime(),
        stat.getAccessTime(), stat.getOwner(), stat.getGroup(),
        stat.getPermission(), (stat.isSymlink() ? RowKeyBytes.toBytes(
        stat.getSymlink().toString()) : null), stat.getLen(),
        stat.getReplication(), stat.getBlockSize());
    file.setTextFile(GiraffaWebUtils.endsWithAny(stat.getPath().toString(), TEXT_FILE_EXT));
    return file;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {

    Path path = GiraffaWebUtils.extractPath(req);

    boolean isMultipart = ServletFileUpload.isMultipartContent(req);

    //File Upload handler
    if (isMultipart) {
      FileItemFactory factory = new DiskFileItemFactory();
      ServletFileUpload upload = new ServletFileUpload(factory);
      List<DiskFileItem> items;
      try {
        items = upload.parseRequest(req);
      } catch (FileUploadException e) {
        resp.getWriter()
            .print("Can't parse upload request. Details:" + e.getMessage());
        return;
      }

      if (items == null || items.size() == 0) {
        resp.getWriter().print("Error! List of uploaded items was empty.");
        return;
      }

      List<String> uploadErrors = new ArrayList<String>();
      List<String> uploadSuccess = new ArrayList<String>();

      for (DiskFileItem uploadItem : items) {
        String uploadItemName = uploadItem.getName().replace(':', '-');
        if (grfs.exists(new Path(path, uploadItemName))) {
          LOG.warn("File already exists: " + path);
          uploadErrors.add(String.format("File already exists: %s",
              new Path(path, uploadItemName)));
          continue;
        }

        FSDataOutputStream grfaFis;
        InputStream uploadedStream = uploadItem.getInputStream();

        try {
          grfaFis = grfs.create(new Path(path, uploadItemName));
          LOG.debug("Created new file " + path);
        } catch (Exception ex) {
          LOG.error(
              String.format("Error occurred while creating new file %s.", path),
              ex);
          uploadErrors.add(String
              .format("Error occurred while creating new file %s. Details: %s",
                  path, ex.getMessage()));
          continue;
        }

        try {
          IOUtils.copy(uploadedStream, grfaFis);
        } catch (Exception ex) {
          if (grfaFis != null) {
            grfs.delete(path, false);
          }
          LOG.error("Can't copy file " + path, ex);
          uploadErrors.add("Can't copy file. Details:" + ex.getMessage());
        } finally {
          if (uploadedStream != null) {
            uploadedStream.close();
          }
          if (grfaFis != null) {
            grfaFis.close();
          }
        }
        LOG.info(String
            .format("Uploaded %s (%d) ", uploadItemName, uploadItem.getSize()));
        uploadSuccess.add(
            String.format("%s (%d)", uploadItemName, uploadItem.getSize()));
      }

      mapper.writeValue(resp.getWriter(),
          new UploadResult(uploadSuccess, uploadErrors));

      return;
    }

    //MkDir handler
    if (grfs.exists(path)) {
      resp.getWriter()
          .print(String.format("Can't create %s. Folder already exists", path));
    } else {
      try {
        grfs.mkdirs(path);
      } catch (Exception ex) {
        resp.getWriter().print(String
            .format("Can't create %s. Details: %s", path, ex.getMessage()));
      }
    }
  }

  static class UploadResult {
    List<String> success;
    List<String> error;

    UploadResult(List<String> success, List<String> error) {
      this.success = success;
      this.error = error;
    }

    public List<String> getSuccess() {
      return success;
    }

    public List<String> getError() {
      return error;
    }
  }

  @Override
  protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    Path path = GiraffaWebUtils.extractPath(req);

    try {
      grfs.delete(path, true);
      LOG.info("Deleted " + path);
    } catch (Exception ex) {
      LOG.error(String.format("Can't delete %s.", path), ex);
      resp.getWriter().print(
          String.format("Can't delete %s. Details: %s", path, ex.getMessage()));
    }

  }

}
