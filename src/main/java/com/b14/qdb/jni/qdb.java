/* ----------------------------------------------------------------------------
 * This file was automatically generated by SWIG (http://www.swig.org).
 * Version 3.0.5
 *
 * Do not make changes to this file unless you know what you are doing--modify
 * the SWIG interface file instead.
 * ----------------------------------------------------------------------------- */

package com.b14.qdb.jni;

public class qdb {
  public static String version() {
    return qdbJNI.version();
  }

  public static String build() {
    return qdbJNI.build();
  }

  public static SWIGTYPE_p_qdb_session open() {
    long cPtr = qdbJNI.open();
    return (cPtr == 0) ? null : new SWIGTYPE_p_qdb_session(cPtr, false);
  }

  public static qdb_error_t close(SWIGTYPE_p_qdb_session handle) {
    return qdb_error_t.swigToEnum(qdbJNI.close(SWIGTYPE_p_qdb_session.getCPtr(handle)));
  }

  public static qdb_error_t connect(SWIGTYPE_p_qdb_session handle, String host, int port) {
    return qdb_error_t.swigToEnum(qdbJNI.connect(SWIGTYPE_p_qdb_session.getCPtr(handle), host, port));
  }

  public static remoteNodeArray multi_connect(SWIGTYPE_p_qdb_session h, remoteNodeArray nodes) {
    return new remoteNodeArray(qdbJNI.multi_connect(SWIGTYPE_p_qdb_session.getCPtr(h), remoteNodeArray.getCPtr(nodes), nodes), true);
  }

  public static qdb_error_t stop_node(SWIGTYPE_p_qdb_session handle, qdb_remote_node_t node, String reason) {
    return qdb_error_t.swigToEnum(qdbJNI.stop_node(SWIGTYPE_p_qdb_session.getCPtr(handle), qdb_remote_node_t.getCPtr(node), node, reason));
  }

  public static qdb_error_t put(SWIGTYPE_p_qdb_session handle, String alias, java.nio.ByteBuffer content, long content_length, long expiry_time) {
    return qdb_error_t.swigToEnum(qdbJNI.put(SWIGTYPE_p_qdb_session.getCPtr(handle), alias, content, content_length, expiry_time));
  }

  public static java.nio.ByteBuffer get_buffer(SWIGTYPE_p_qdb_session handle, String alias, error_carrier err) { return qdbJNI.get_buffer(SWIGTYPE_p_qdb_session.getCPtr(handle), alias, error_carrier.getCPtr(err), err); }

  public static StringVec prefix_get(SWIGTYPE_p_qdb_session handle, String prefix, error_carrier err) {
    return new StringVec(qdbJNI.prefix_get(SWIGTYPE_p_qdb_session.getCPtr(handle), prefix, error_carrier.getCPtr(err), err), true);
  }

  public static RemoteNode get_location(SWIGTYPE_p_qdb_session handle, String alias, error_carrier err) {
    return new RemoteNode(qdbJNI.get_location(SWIGTYPE_p_qdb_session.getCPtr(handle), alias, error_carrier.getCPtr(err), err), true);
  }

  public static java.nio.ByteBuffer get_remove(SWIGTYPE_p_qdb_session handle, String alias, error_carrier err) { return qdbJNI.get_remove(SWIGTYPE_p_qdb_session.getCPtr(handle), alias, error_carrier.getCPtr(err), err); }

  public static java.nio.ByteBuffer get_buffer_update(SWIGTYPE_p_qdb_session handle, String alias, java.nio.ByteBuffer content, long content_length, long expiry_time, error_carrier err) { return qdbJNI.get_buffer_update(SWIGTYPE_p_qdb_session.getCPtr(handle), alias, content, content_length, expiry_time, error_carrier.getCPtr(err), err); }

  public static java.nio.ByteBuffer node_status(SWIGTYPE_p_qdb_session handle, qdb_remote_node_t node, error_carrier err) { return qdbJNI.node_status(SWIGTYPE_p_qdb_session.getCPtr(handle), qdb_remote_node_t.getCPtr(node), node, error_carrier.getCPtr(err), err); }

  public static java.nio.ByteBuffer node_config(SWIGTYPE_p_qdb_session handle, qdb_remote_node_t node, error_carrier err) { return qdbJNI.node_config(SWIGTYPE_p_qdb_session.getCPtr(handle), qdb_remote_node_t.getCPtr(node), node, error_carrier.getCPtr(err), err); }

  public static java.nio.ByteBuffer node_topology(SWIGTYPE_p_qdb_session handle, qdb_remote_node_t node, error_carrier err) { return qdbJNI.node_topology(SWIGTYPE_p_qdb_session.getCPtr(handle), qdb_remote_node_t.getCPtr(node), node, error_carrier.getCPtr(err), err); }

  public static java.nio.ByteBuffer compare_and_swap(SWIGTYPE_p_qdb_session handle, String alias, java.nio.ByteBuffer content, long content_length, java.nio.ByteBuffer comparand, long comparand_length, long expiry_time, error_carrier err) { return qdbJNI.compare_and_swap(SWIGTYPE_p_qdb_session.getCPtr(handle), alias, content, content_length, comparand, comparand_length, expiry_time, error_carrier.getCPtr(err), err); }

  public static void free_buffer(SWIGTYPE_p_qdb_session handle, java.nio.ByteBuffer content) {
    qdbJNI.free_buffer(SWIGTYPE_p_qdb_session.getCPtr(handle), content);
  }

  public static String make_error_string(qdb_error_t error) {
    return qdbJNI.make_error_string(error.swigValue());
  }

  public static qdb_error_t update(SWIGTYPE_p_qdb_session handle, String alias, java.nio.ByteBuffer content, long content_length, long expiry_time) {
    return qdb_error_t.swigToEnum(qdbJNI.update(SWIGTYPE_p_qdb_session.getCPtr(handle), alias, content, content_length, expiry_time));
  }

  public static qdb_error_t remove(SWIGTYPE_p_qdb_session handle, String alias) {
    return qdb_error_t.swigToEnum(qdbJNI.remove(SWIGTYPE_p_qdb_session.getCPtr(handle), alias));
  }

  public static qdb_error_t remove_if(SWIGTYPE_p_qdb_session handle, String alias, java.nio.ByteBuffer comparand, long comparand_length) {
    return qdb_error_t.swigToEnum(qdbJNI.remove_if(SWIGTYPE_p_qdb_session.getCPtr(handle), alias, comparand, comparand_length));
  }

  public static qdb_error_t purge_all(SWIGTYPE_p_qdb_session handle) {
    return qdb_error_t.swigToEnum(qdbJNI.purge_all(SWIGTYPE_p_qdb_session.getCPtr(handle)));
  }

  public static qdb_error_t iterator_begin(SWIGTYPE_p_qdb_session handle, qdb_const_iterator_t iterator) {
    return qdb_error_t.swigToEnum(qdbJNI.iterator_begin(SWIGTYPE_p_qdb_session.getCPtr(handle), qdb_const_iterator_t.getCPtr(iterator), iterator));
  }

  public static qdb_error_t iterator_rbegin(SWIGTYPE_p_qdb_session handle, qdb_const_iterator_t iterator) {
    return qdb_error_t.swigToEnum(qdbJNI.iterator_rbegin(SWIGTYPE_p_qdb_session.getCPtr(handle), qdb_const_iterator_t.getCPtr(iterator), iterator));
  }

  public static qdb_error_t iterator_next(qdb_const_iterator_t iterator) {
    return qdb_error_t.swigToEnum(qdbJNI.iterator_next(qdb_const_iterator_t.getCPtr(iterator), iterator));
  }

  public static qdb_error_t iterator_previous(qdb_const_iterator_t iterator) {
    return qdb_error_t.swigToEnum(qdbJNI.iterator_previous(qdb_const_iterator_t.getCPtr(iterator), iterator));
  }

  public static qdb_error_t iterator_close(qdb_const_iterator_t iterator) {
    return qdb_error_t.swigToEnum(qdbJNI.iterator_close(qdb_const_iterator_t.getCPtr(iterator), iterator));
  }

  public static qdb_error_t iterator_copy(qdb_const_iterator_t original, qdb_const_iterator_t copy) {
    return qdb_error_t.swigToEnum(qdbJNI.iterator_copy(qdb_const_iterator_t.getCPtr(original), original, qdb_const_iterator_t.getCPtr(copy), copy));
  }

  public static java.nio.ByteBuffer iterator_content(qdb_const_iterator_t iterator) { return qdbJNI.iterator_content(qdb_const_iterator_t.getCPtr(iterator), iterator); }

  public static qdb_error_t expires_at(SWIGTYPE_p_qdb_session handle, String alias, long expiry_time) {
    return qdb_error_t.swigToEnum(qdbJNI.expires_at(SWIGTYPE_p_qdb_session.getCPtr(handle), alias, expiry_time));
  }

  public static qdb_error_t expires_from_now(SWIGTYPE_p_qdb_session handle, String alias, long expiry_delta) {
    return qdb_error_t.swigToEnum(qdbJNI.expires_from_now(SWIGTYPE_p_qdb_session.getCPtr(handle), alias, expiry_delta));
  }

  public static long get_expiry(SWIGTYPE_p_qdb_session handle, String alias, error_carrier err) {
    return qdbJNI.get_expiry(SWIGTYPE_p_qdb_session.getCPtr(handle), alias, error_carrier.getCPtr(err), err);
  }

  public static run_batch_result run_batch(SWIGTYPE_p_qdb_session h, BatchOpsVec requests) {
    return new run_batch_result(qdbJNI.run_batch(SWIGTYPE_p_qdb_session.getCPtr(h), BatchOpsVec.getCPtr(requests), requests), true);
  }

  public static void release_batch_result(SWIGTYPE_p_qdb_session h, run_batch_result br) {
    qdbJNI.release_batch_result(SWIGTYPE_p_qdb_session.getCPtr(h), run_batch_result.getCPtr(br), br);
  }

}