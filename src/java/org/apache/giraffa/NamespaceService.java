/**
 * 
 */
package org.apache.giraffa;

import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.ClientProtocol;

/**
 * {@link NamespaceService} is a common interface that provides access
 * to a reliable storage system, like HBase, which maintains 
 * the Giraffa file system metadata.
 * <p>
 * It plays a role of a proxy used by DFSClient to communicate with
 * the underlying storage system as if it is a NameNode.
 * It implements {@link ClientProtocol} and is a replacement of the
 * NameNode RPC proxy.
 * <p>
 * Implement this interface to make Giraffa client connect to different
 * highly available storage systems.
 * <p>
 * {@link NamespaceAgent} is the default implementation of
 * {@link NamespaceService} for HBase.
 */
public interface NamespaceService extends ClientProtocol {
  
  public void initialize(GiraffaConfiguration conf) throws IOException;

  public void format(GiraffaConfiguration conf) throws IOException;

  public void close() throws IOException;
}
