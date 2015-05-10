package com.b14.qdb;

import java.nio.ByteBuffer;

import com.b14.qdb.batch.QdbBatchResult;
import com.b14.qdb.batch.Result;
import com.b14.qdb.batch.TypeOperation;
import com.b14.qdb.jni.BatchOpsVec;
import com.b14.qdb.jni.SWIGTYPE_p_qdb_session;
import com.b14.qdb.jni.qdb;
import com.b14.qdb.jni.qdb_error_t;
import com.b14.qdb.jni.qdb_operation_t;
import com.b14.qdb.jni.qdb_operation_type_t;
import com.b14.qdb.jni.run_batch_result;

public class QdbBatch {
    private final transient SWIGTYPE_p_qdb_session session;
    private final BatchOpsVec operations = new BatchOpsVec();

    /**
     * Creates an empty batch, i.e. an empty collection of operation.<br>
     * Batch operations can greatly increase performance when it is necessary to run many small operations. <br>
     * Operations in a QdbBatch are not executed until run() is called.
     * 
     * @param session
     */
    public QdbBatch(SWIGTYPE_p_qdb_session session) {
        this.session = session;
    }

    /**
     * Adds a "get and remove" operation to the batch. When executed, the "get and remove" operation atomically gets an entry and removes it.
     * 
     * @param alias
     */
    public void getAndRemove(String alias) {
        qdb_operation_t operation = new qdb_operation_t();
        operation.setType(qdb_operation_type_t.optionp_get_and_remove);
        operation.setAlias(alias);
        operations.push_back(operation);
    }
    
    /**
     * Adds a "get" operation to the batch. When executed, the "get" operation retrieves an entry's content.
     * 
     * @param alias
     */
    public void get(String alias) {
        qdb_operation_t operation = new qdb_operation_t();
        operation.setType(qdb_operation_type_t.optionp_get_alloc);
        operation.setAlias(alias);
        operations.push_back(operation);
    }
    
    /**
     * Adds a "remove" operation to the batch. When executed, the "remove" operation removes an entry.
     * 
     * @param alias
     */
    public void remove(String alias) {
        qdb_operation_t operation = new qdb_operation_t();
        operation.setType(qdb_operation_type_t.optionp_remove);
        operation.setAlias(alias);
        operations.push_back(operation);
    }
    
    /**
     * Adds a "put" operation to the batch. When executed, the "put" operation adds an entry.<br>
     * Alias beginning with "qdb" are reserved and cannot be used.
     * 
     * @param alias
     * @param content
     */
    public void put(String alias, ByteBuffer content) {
        this.put(alias, content, 0L);
    }
    
    /**
     * Adds a "put" operation to the batch. When executed, the "put" operation adds an entry.<br>
     * Alias beginning with "qdb" are reserved and cannot be used.
     * 
     * @param alias
     * @param content
     * @param expiryTime
     */
    public void put(String alias, ByteBuffer content, long expiryTime) {
        qdb_operation_t operation = new qdb_operation_t();
        operation.setType(qdb_operation_type_t.optionp_put);
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
     * @param alias
     * @param content
     */
    public void update(String alias, ByteBuffer content) {
        this.update(alias, content, 0L);
    }
    
    /**
     * Adds an "update" operation to the batch. When executed, the "update" operation updates an entry. <br>
     * If the entry already exists, the content will be updated. If the entry does not exist, it will be created.<br>
     * Alias beginning with "qdb" are reserved and cannot be used.
     * 
     * @param alias
     * @param content
     * @param expiryTime
     */
    public void update(String alias, ByteBuffer content, long expiryTime) {
        qdb_operation_t operation = new qdb_operation_t();
        operation.setType(qdb_operation_type_t.optionp_update);
        operation.setAlias(alias);
        operation.setContent(content);
        operation.setContent_size(content.limit());
        operation.setExpiry_time((expiryTime == 0) ? 0 : (System.currentTimeMillis() / 1000) + expiryTime);
        operations.push_back(operation);
    }
    
    /**
     * Adds a "get and update" operation to the batch. When executed, the "get and update" operation atomically gets and updates (in this order) the entry.
     * @param alias
     * @param content
     */
    public void getAndUpdate(String alias, ByteBuffer content) {
        this.getAndUpdate(alias, content, 0L);
    }
    
    /**
     * Adds a "get and update" operation to the batch. When executed, the "get and update" operation atomically gets and updates (in this order) the entry.
     * 
     * @param alias
     * @param content
     * @param expiryTime
     */
    public void getAndUpdate(String alias, ByteBuffer content, long expiryTime) {
        qdb_operation_t operation = new qdb_operation_t();
        operation.setType(qdb_operation_type_t.optionp_get_and_update);
        operation.setAlias(alias);
        operation.setContent(content);
        operation.setContent_size(content.limit());
        operation.setExpiry_time((expiryTime == 0) ? 0 : (System.currentTimeMillis() / 1000) + expiryTime);
        operations.push_back(operation);
    }
    
    /**
     * Adds a "remove if" operation to the batch. When executed, the "remove if" operation removes an entry if it matches $comparand. The operation is atomic.
     * 
     * @param alias
     * @param comparand
     */
    public void removeIf(String alias, ByteBuffer comparand) {
        qdb_operation_t operation = new qdb_operation_t();
        operation.setType(qdb_operation_type_t.optionp_get_and_update);
        operation.setAlias(alias);
        operation.setComparand(comparand);
        operation.setComparand_size(comparand.limit());
        operations.push_back(operation);
    }
    
    /**
     * Adds a "compare and swap" operation to the batch. When executed, the "compare and swap" operation atomically compares the entry with $comparand and updates it to $new_content if, and only if, they match.
     * 
     * @param alias
     * @param content
     * @param comparand
     */
    public void compareAndSwap(String alias, ByteBuffer content, ByteBuffer comparand) {
        this.compareAndSwap(alias, content, comparand, 0L);
    }
    
    /**
     * Adds a "compare and swap" operation to the batch. When executed, the "compare and swap" operation atomically compares the entry with $comparand and updates it to $new_content if, and only if, they match.
     * 
     * @param alias
     * @param content
     * @param comparand
     * @param expiryTime
     */
    public void compareAndSwap(String alias, ByteBuffer content, ByteBuffer comparand, long expiryTime) {
        qdb_operation_t operation = new qdb_operation_t();
        operation.setType(qdb_operation_type_t.optionp_get_and_update);
        operation.setAlias(alias);
        operation.setContent(content);
        operation.setContent_size(content.limit());
        operation.setComparand(comparand);
        operation.setComparand_size(comparand.limit());
        operations.push_back(operation);
    }
    
    /**
     * 
     * @return
     */
    public QdbBatchResult run() {
        final QdbBatchResult results = new QdbBatchResult();
        run_batch_result qdbResults = qdb.run_batch(session, operations);
        results.setNbOperations(operations.size());
        results.setNbSuccess(qdbResults.getSuccesses());
        results.setSuccess(operations.size() == qdbResults.getSuccesses());
        
        for (int i = 0; i < operations.size(); i++) {
            qdb_operation_t operation = qdbResults.getResults().get(i);
            Result result = new Result();
            
            // Set alias
            result.setAlias(operation.getAlias());

            // Set error
            result.setSuccess(operation.getError() == qdb_error_t.error_ok);
            if (!result.isSuccess()) {
                result.setError(operation.getError().toString());
            }

            // Set type
            boolean hasValue = false;
            if (operation.getType() == qdb_operation_type_t.optionp_get_alloc) { // GET operation
                result.setTypeOperation(TypeOperation.GET);
                hasValue = true && result.isSuccess();
            } else if (operation.getType() == qdb_operation_type_t.optionp_put) { // PUT operation
                result.setTypeOperation(TypeOperation.PUT);
            } else if (operation.getType() == qdb_operation_type_t.optionp_update) { // UPDATE operation
                result.setTypeOperation(TypeOperation.UPDATE);
            } else if (operation.getType() == qdb_operation_type_t.optionp_remove) { // REMOVE operation
                result.setTypeOperation(TypeOperation.REMOVE);
            } else if (operation.getType() == qdb_operation_type_t.optionp_cas) { // COMPARE AND SWAP operation
                result.setTypeOperation(TypeOperation.CAS);
                hasValue = true && result.isSuccess();
            } else if (operation.getType() == qdb_operation_type_t.optionp_get_and_update) { // GET AND UPDATE operation
                result.setTypeOperation(TypeOperation.GET_UPDATE);
                hasValue = true && result.isSuccess();
            } else if (operation.getType() == qdb_operation_type_t.optionp_get_and_remove) { // GET AND REMOVE operation
                result.setTypeOperation(TypeOperation.GET_REMOVE);
                hasValue = true && result.isSuccess();
            } else if (operation.getType() == qdb_operation_type_t.optionp_remove_if) { // REMOVE IF operation
                result.setTypeOperation(TypeOperation.REMOVE_IF);
            } else { // NO OPERATION
                result.setTypeOperation(TypeOperation.NO_OP);
            }

            // Set value
            if (hasValue) {
                final ByteBuffer buffer = operation.getResult();
                if (buffer != null) {
                    buffer.rewind();
                    result.setValue(buffer);
                } else {
                    result.setValue(null);
                    result.setError(qdb_error_t.error_unmatched_content.toString());
                    result.setSuccess(false);
                }
            }
            
            // Add result
            results.addResult(result);
        }
        
        // Clean-up
        qdb.release_batch_result(session, qdbResults);
        operations.clear();
        
        // Return
        return results;
    }
}
