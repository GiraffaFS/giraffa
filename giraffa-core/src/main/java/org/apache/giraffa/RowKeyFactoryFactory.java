package org.apache.giraffa;

import static org.apache.giraffa.GiraffaConfiguration.GRFA_ROW_KEY_DEFAULT;
import static org.apache.giraffa.GiraffaConfiguration.GRFA_ROW_KEY_KEY;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

public class RowKeyFactoryFactory {

  private static Class<? extends RowKeyFactory> RowKeyFactoryClass;

  public static RowKeyFactory createFactory(Configuration conf,
                                            GiraffaProtocol service) {
    registerRowKey(conf);
    RowKeyFactory fact = ReflectionUtils.newInstance(RowKeyFactoryClass, conf);
    fact.setService(service);
    return fact;
  }

  private static synchronized void registerRowKey(Configuration conf) {
    if (RowKeyFactoryClass == null) {
      RowKeyFactoryClass = conf.getClass(GRFA_ROW_KEY_KEY,
          GRFA_ROW_KEY_DEFAULT, RowKeyFactory.class);
    }
  }
}
