/* ----------------------------------------------------------------------------
 * This file was automatically generated by SWIG (http://www.swig.org).
 * Version 3.0.5
 *
 * Do not make changes to this file unless you know what you are doing--modify
 * the SWIG interface file instead.
 * ----------------------------------------------------------------------------- */

package com.b14.qdb.jni;

public final class qdb_operation_type_t {
  public final static qdb_operation_type_t optionp_uninitialized = new qdb_operation_type_t("optionp_uninitialized", qdbJNI.optionp_uninitialized_get());
  public final static qdb_operation_type_t optionp_get_alloc = new qdb_operation_type_t("optionp_get_alloc", qdbJNI.optionp_get_alloc_get());
  public final static qdb_operation_type_t optionp_put = new qdb_operation_type_t("optionp_put", qdbJNI.optionp_put_get());
  public final static qdb_operation_type_t optionp_update = new qdb_operation_type_t("optionp_update", qdbJNI.optionp_update_get());
  public final static qdb_operation_type_t optionp_remove = new qdb_operation_type_t("optionp_remove", qdbJNI.optionp_remove_get());
  public final static qdb_operation_type_t optionp_cas = new qdb_operation_type_t("optionp_cas", qdbJNI.optionp_cas_get());
  public final static qdb_operation_type_t optionp_get_and_update = new qdb_operation_type_t("optionp_get_and_update", qdbJNI.optionp_get_and_update_get());
  public final static qdb_operation_type_t optionp_get_and_remove = new qdb_operation_type_t("optionp_get_and_remove", qdbJNI.optionp_get_and_remove_get());
  public final static qdb_operation_type_t optionp_remove_if = new qdb_operation_type_t("optionp_remove_if", qdbJNI.optionp_remove_if_get());

  public final int swigValue() {
    return swigValue;
  }

  public String toString() {
    return swigName;
  }

  public static qdb_operation_type_t swigToEnum(int swigValue) {
    if (swigValue < swigValues.length && swigValue >= 0 && swigValues[swigValue].swigValue == swigValue)
      return swigValues[swigValue];
    for (int i = 0; i < swigValues.length; i++)
      if (swigValues[i].swigValue == swigValue)
        return swigValues[i];
    throw new IllegalArgumentException("No enum " + qdb_operation_type_t.class + " with value " + swigValue);
  }

  private qdb_operation_type_t(String swigName) {
    this.swigName = swigName;
    this.swigValue = swigNext++;
  }

  private qdb_operation_type_t(String swigName, int swigValue) {
    this.swigName = swigName;
    this.swigValue = swigValue;
    swigNext = swigValue+1;
  }

  private qdb_operation_type_t(String swigName, qdb_operation_type_t swigEnum) {
    this.swigName = swigName;
    this.swigValue = swigEnum.swigValue;
    swigNext = this.swigValue+1;
  }

  private static qdb_operation_type_t[] swigValues = { optionp_uninitialized, optionp_get_alloc, optionp_put, optionp_update, optionp_remove, optionp_cas, optionp_get_and_update, optionp_get_and_remove, optionp_remove_if };
  private static int swigNext = 0;
  private final int swigValue;
  private final String swigName;
}

