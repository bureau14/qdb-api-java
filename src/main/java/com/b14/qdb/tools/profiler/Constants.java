package com.b14.qdb.tools.profiler;

import java.lang.reflect.Field;

/**
 * Some useful constants.
 **/
final class Constants {
  private Constants() {}                          // can't construct

  /** True iff running on a 64bit JVM */
  public static final boolean JRE_IS_64BIT;

  /** The value of <tt>System.getProperty("java.version")<tt>. **/
  public static final String JAVA_VERSION = System.getProperty("java.version");
  public static final String JAVA_VENDOR = System.getProperty("java.vendor");
  public static final String JVM_VENDOR = System.getProperty("java.vm.vendor");
  public static final String JVM_VERSION = System.getProperty("java.vm.version");
  public static final String JVM_NAME = System.getProperty("java.vm.name");
  public static final String OS_ARCH = System.getProperty("os.arch");
  public static final String OS_VERSION = System.getProperty("os.version");

  static {
    final String OS_ARCH = System.getProperty("os.arch");
    boolean is64Bit = false;
    try {
      final Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
      final Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      final Object unsafe = unsafeField.get(null);
      final int addressSize = ((Number) unsafeClass.getMethod("addressSize").invoke(unsafe)).intValue();
      is64Bit = addressSize >= 8;
    } catch (Exception e) {
      final String x = System.getProperty("sun.arch.data.model");
      if (x != null) {
        is64Bit = x.indexOf("64") != -1;
      } else {
        if (OS_ARCH != null && OS_ARCH.indexOf("64") != -1) {
          is64Bit = true;
        } else {
          is64Bit = false;
        }
      }
    }
    JRE_IS_64BIT = is64Bit;
  }
}
