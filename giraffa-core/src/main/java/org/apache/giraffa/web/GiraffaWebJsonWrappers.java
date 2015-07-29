package org.apache.giraffa.web;

import org.apache.commons.lang.StringUtils;
import org.apache.giraffa.INodeFile;
import org.apache.giraffa.RowKey;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonPropertyOrder;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class GiraffaWebJsonWrappers {

  // http://datatables.net/usage/server-side

  public static class DataTablesRequest {

    //last key used for currently displayed rows (during pagination)
    private String endKey;
    private final Integer endKeyPosition;

    // Display start point in the current data set.
    private int iDisplayStart;

    /*
     * Number of records that the table can display in the current draw.
     * It is expected that the number of records returned will be equal
     * to this number, unless the server has fewer records to return.
     */
    private int iDisplayLength;

    // Global search field
    private String sSearch;

    // Information for DataTables to use for rendering.
    private int sEcho;

    private long iTotalRecords;

    public DataTablesRequest(HttpServletRequest req) {
      this.sEcho = Integer.valueOf(req.getParameter("sEcho"));
      this.iDisplayStart = Integer.valueOf(req.getParameter("iDisplayStart"));
      this.iDisplayLength = Integer.valueOf(req.getParameter("iDisplayLength"));
      this.iTotalRecords =
          StringUtils.isEmpty(req.getParameter("iTotalRecords")) ? 0 :
              Integer.valueOf(req.getParameter("iTotalRecords"));
      this.sSearch = req.getParameter("sSearch");
      this.endKey = req.getParameter("endKey");
      this.endKeyPosition =
          StringUtils.isEmpty(req.getParameter("endKeyPosition")) ? 0 :
              Integer.parseInt(req.getParameter("endKeyPosition"));
    }

    public int getIDisplayStart() {
      return iDisplayStart;
    }

    public int getIDisplayLength() {
      return iDisplayLength;
    }

    public long getITotalRecords() {
      return iTotalRecords;
    }

    public String getSSearch() {
      return sSearch;
    }

    public String getEndKey() {
      return endKey;
    }

    public Integer getEndKeyPosition() {
      return endKeyPosition;
    }

    public int getSEcho() {
      return sEcho;
    }
  }

  public static class DataTablesReply {

    // Total records, before filtering (i.e. the total number of records in the
    // database)
    private long iTotalRecords;

    /*
     * Total records, after filtering (i.e. the total number of records after
     * filtering has been applied - not just the number of records being
     * returned in this result set)
     */
    private long iTotalDisplayRecords;

    /* An unaltered copy of sEcho sent from the client side. This parameter will
     *  change with each draw (it is basically a draw count).
     */
    private int sEcho;

    private List<Map<Integer, Object>> aaData = new ArrayList<Map<Integer, Object>>();

    public List<Map<Integer, Object>> getAaData() {
      return aaData;
    }

    public DataTablesReply(int sEcho) {
      this.sEcho = sEcho;
    }

    public void setITotalRecords(long iTotalRecords) {
      this.iTotalRecords = iTotalRecords;
    }

    public void setITotalDisplayRecords(long iTotalDisplayRecords) {
      this.iTotalDisplayRecords = iTotalDisplayRecords;
    }

    public long getiTotalRecords() {
      return iTotalRecords;
    }

    public long getiTotalDisplayRecords() {
      return iTotalDisplayRecords;
    }

    public int getsEcho() {
      return sEcho;
    }
  }


  @JsonIgnoreProperties({"block", "blockToken", "locations"})
  @JsonPropertyOrder({"blockName","blockSize","corrupt","startOffset","locs"})
  public static class LocatedBlockDescriptor extends LocatedBlock {

    private String locs;

    public LocatedBlockDescriptor(LocatedBlock locatedBlock) {
      super(locatedBlock.getBlock(), locatedBlock.getLocations(),
          locatedBlock.getStartOffset(), locatedBlock.isCorrupt());
      this.locs = Arrays.asList(locatedBlock.getLocations()).toString();
    }

    public String getLocs() {
      return locs;
    }

    public String getBlockName() {
      return getBlock().toString();
    }

  }

  public static class LocatedBlocksWrapper {

    private List<LocatedBlockDescriptor> blockDescriptors;
    private long totalBlockNumber;

    public LocatedBlocksWrapper(List<LocatedBlockDescriptor> blockDescriptors,
                                long totalBlockNumber) {
      this.blockDescriptors = blockDescriptors;
      this.totalBlockNumber = totalBlockNumber;
    }

    public List<LocatedBlockDescriptor> getBlockDescriptors() {
      return blockDescriptors;
    }

    public long getTotalBlockNumber() {
      return totalBlockNumber;
    }
  }

  public static class FilesResponse {

    List<FileItem> files = new ArrayList<FileItem>();
    List<FileItem> folders = new ArrayList<FileItem>();

    public List<FileItem> getFiles() {
      return files;
    }

    public List<FileItem> getFolders() {
      return folders;
    }
  }

  @JsonIgnoreProperties({"key", "blocks", "fileState", "fileStatus",
      "locatedFileStatus", "getRowKey", "blocksBytes", "permission"})
  public static class FileItem extends INodeFile {

    private String preview;
    private boolean isTextFile;

    /**
     * Construct an INode from the RowKey and file attributes.
     */
    public FileItem(RowKey rowKey, long mtime, long atime, String owner,
                    String group, FsPermission perms, byte[] symlink,
                    long length, short replication, long blockSize) {
      super(rowKey, mtime, atime, owner, group, perms, symlink, null, length,
          replication, blockSize, null, null, null, null);
    }

    public String getPreview() {
      return preview;
    }

    public boolean isTextFile() {
      return isTextFile;
    }

    public void setPreview(String preview) {
      this.preview = preview;
    }

    public void setTextFile(boolean textFile) {
      isTextFile = textFile;
    }

    public String getName() {
      return new Path(getPath()).getName();
    }

    public String getPermissionString() {
      return getPermission().toString();
    }



  }

}
