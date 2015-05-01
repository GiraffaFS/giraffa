package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.NotServingRegionException;
import java.io.IOException;

/**
 * This interceptor throws any filesystem related exceptions that were generated
 * during the rpc call to prevent the call from being retried.
 */
class FileSystemExceptionInterceptor extends NoOpRetryableCallerInterceptor {

  @Override
  public void handleFailure(RetryingCallerInterceptorContext context,
                            Throwable t)
      throws IOException {
    // don't retry an IOExceptions unless its a NotServingRegionExceptions
    if(t instanceof IOException && !(t instanceof NotServingRegionException)) {
      throw (IOException) t;
    }
  }

  @Override
  public String toString() {
    return "FileSystemExceptionInterceptor";
  }
}
