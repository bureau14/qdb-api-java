package net.quasardb.qdb;

import java.nio.ByteBuffer;

import net.quasardb.qdb.batch.QdbBatchResult;
import net.quasardb.qdb.batch.Result;
import net.quasardb.qdb.batch.TypeOperation;
import net.quasardb.qdb.jni.BatchOpsVec;
import net.quasardb.qdb.jni.SWIGTYPE_p_qdb_session;
import net.quasardb.qdb.jni.qdb;
import net.quasardb.qdb.jni.qdb_error_t;
import net.quasardb.qdb.jni.qdb_operation_t;
import net.quasardb.qdb.jni.qdb_operation_type_t;
import net.quasardb.qdb.jni.run_batch_result;

public class QdbBatch {
    private final transient SWIGTYPE_p_qdb_session session;
    private final BatchOpsVec operations = new BatchOpsVec();

    /**
     * Creates an empty batch, i.e. an empty collection of operation.<br>
     * Batch operations can greatly increase performance when it is necessary to run many small operations. <br>
     * Operations in a QdbBatch are not executed until run() is called.
     *
     * @param session TODO
     */
    public QdbBatch(SWIGTYPE_p_qdb_session session) {
        this.session = session;
    }

    /**
     * Adds a "get and remove" operation to the batch. When executed, the "get and remove" operation atomically gets an entry and removes it.
     *
     * @param alias TODO
     */
    public void getAndRemove(String alias) {
        qdb_operation_t operation = new qdb_operation_t();
        operation.setType(qdb_operation_type_t.qdb_op_blob_get_and_remove);
        operation.setAlias(alias);
        operations.push_back(operation);
    }

    /**
     * Adds a "get" operation to the batch. When executed, the "get" operation retrieves an entry's content.
     *
     * @param alias TODO
     */
    public void get(String alias) {
        qdb_operation_t operation = new qdb_operation_t();
        operation.setType(qdb_operation_type_t.qdb_op_blob_get);
        operation.setAlias(alias);
        operations.push_back(operation);
    }

    /**
     * Adds a "remove" operation to the batch. When executed, the "remove" operation removes an entry.
     *
     * @param alias TODO
     */
    public void remove(String alias) {
        qdb_operation_t operation = new qdb_operation_t();
        operation.setType(qdb_operation_type_t.qdb_op_blob_remove);
        operation.setAlias(alias);
        operations.push_back(operation);
    }

    /**
     * Adds a "put" operation to the batch. When executed, the "put" operation adds an entry.<br>
     * Alias beginning with "qdb" are reserved and cannot be used.
     *
     * @param alias TODO
     * @param content TODO
     */
    public void put(String alias, ByteBuffer content) {
        this.put(alias, content, 0L);
    }

    /**
     * Adds a "put" operation to the batch. When executed, the "put" operation adds an entry.<br>
     * Alias beginning with "qdb" are reserved and cannot be used.
     *
     * @param alias TODO
     * @param content TODO
     * @param expiryTime TODO
     */
    public void put(String alias, ByteBuffer content, long expiryTime) {
        qdb_operation_t operation = new qdb_operation_t();
        operation.setType(qdb_operation_type_t.qdb_op_blob_put);
        operation.setAlias(alias);
        operation.setContent(content);
        operation.setContent_size(content.limit());
        operation.setExpiry_time((expiryTime == 0) ? 0 : (System.currentTimeMillis() / 1000) + expiryTime);
        operations.push_back(operation);
    }

    /**
     * Adds an "update" operation to the batch. When executed, the "update" operation updates an entry. <br>
     * If the entry already exists, the content will be updated. If the entry does not exist, it will be created.<br>
     * Alias beginning with "qdb" are reserved and cannot be used.
     *
     * @param alias TODO
     * @param content TODO
     */
    public void update(String alias, ByteBuffer content) {
        this.update(alias, content, 0L);
    }

    /**
     * Adds an "update" operation to the batch. When executed, the "update" operation updates an entry. <br>
     * If the entry already exists, the content will be updated. If the entry does not exist, it will be created.<br>
     * Alias beginning with "qdb" are reserved and cannot be used.
     *
     * @param alias TODO
     * @param content TODO
     * @param expiryTime TODO
     */
    public void update(String alias, ByteBuffer content, long expiryTime) {
        qdb_operation_t operation = new qdb_operation_t();
        operation.setType(qdb_operation_type_t.qdb_op_blob_update);
        operation.setAlias(alias);
        operation.setContent(content);
        operation.setContent_size(content.limit());
        operation.setExpiry_time((expiryTime == 0) ? 0 : (System.currentTimeMillis() / 1000) + expiryTime);
        operations.push_back(operation);
    }

    /**
     * Adds a "get and update" operation to the batch. When executed, the "get and update" operation atomically gets and updates (in this order) the entry.
     * @param alias TODO
     * @param content TODO
     */
    public void getAndUpdate(String alias, ByteBuffer content) {
        this.getAndUpdate(alias, content, 0L);
    }

    /**
     * Adds a "get and update" operation to the batch. When executed, the "get and update" operation atomically gets and updates (in this order) the entry.
     *
     * @param alias TODO
     * @param content TODO
     * @param expiryTime TODO
     */
    public void getAndUpdate(String alias, ByteBuffer content, long expiryTime) {
        qdb_operation_t operation = new qdb_operation_t();
        operation.setType(qdb_operation_type_t.qdb_op_blob_get_and_update);
        operation.setAlias(alias);
        operation.setContent(content);
        operation.setContent_size(content.limit());
        operation.setExpiry_time((expiryTime == 0) ? 0 : (System.currentTimeMillis() / 1000) + expiryTime);
        operations.push_back(operation);
    }

    /**
     * Adds a "remove if" operation to the batch. When executed, the "remove if" operation removes an entry if it matches $comparand. The operation is atomic.
     *
     * @param alias TODO
     * @param comparand TODO
     */
    public void removeIf(String alias, ByteBuffer comparand) {
        qdb_operation_t operation = new qdb_operation_t();
        operation.setType(qdb_operation_type_t.qdb_op_blob_remove_if);
        operation.setAlias(alias);
        operation.setComparand(comparand);
        operation.setComparand_size(comparand.limit());
        operations.push_back(operation);
    }

    /**
     * Adds a "compare and swap" operation to the batch. When executed, the "compare and swap" operation atomically compares the entry with $comparand and updates it to $new_content if, and only if, they match.
     *
     * @param alias TODO
     * @param content TODO
     * @param comparand TODO
     */
    public void compareAndSwap(String alias, ByteBuffer content, ByteBuffer comparand) {
        this.compareAndSwap(alias, content, comparand, 0L);
    }

    /**
     * Adds a "compare and swap" operation to the batch. When executed, the "compare and swap" operation atomically compares the entry with $comparand and updates it to $new_content if, and only if, they match.
     *
     * @param alias TODO
     * @param content TODO
     * @param comparand TODO
     * @param expiryTime TODO
     */
    public void compareAndSwap(String alias, ByteBuffer content, ByteBuffer comparand, long expiryTime) {
        qdb_operation_t operation = new qdb_operation_t();
        operation.setType(qdb_operation_type_t.qdb_op_blob_cas);
        operation.setAlias(alias);
        operation.setContent(content);
        operation.setContent_size(content.limit());
        operation.setComparand(comparand);
        operation.setComparand_size(comparand.limit());
        operations.push_back(operation);
    }

    /**
     *
     * @return QdbBatchResult
     */
    public QdbBatchResult run() {
        return new QdbBatchResult(session, operations, qdb.run_batch(session, operations));
    }
}
