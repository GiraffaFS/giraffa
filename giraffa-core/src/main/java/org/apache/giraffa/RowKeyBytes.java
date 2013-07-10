package org.apache.giraffa;

import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraffa.hbase.NamespaceProcessor;

public class RowKeyBytes {
  private static final String DEFAULT_ENCODING = GiraffaConstants.UTF8;
  
  private static final Log LOG =
    LogFactory.getLog(NamespaceProcessor.class.getName());

  public static byte[] toBytes(String toConv) {
    try {
      return toConv.getBytes(DEFAULT_ENCODING);
    } catch (UnsupportedEncodingException e) {
      LOG.error(DEFAULT_ENCODING + " not supported?", e);
    }
    return null;
  }
  
  public static String toString(byte[] b){
    if(b == null) {
      return null;
    }
    try {
      return new String(b, DEFAULT_ENCODING);
    }catch(UnsupportedEncodingException e) {
      LOG.error(DEFAULT_ENCODING + " not supported?", e);
      return null;
    }
  }

}
