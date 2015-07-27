package net.quasardb.qdb.batch;

import java.nio.ByteBuffer;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import net.quasardb.qdb.batch.Result;

import net.quasardb.qdb.jni.BatchOpsVec;
import net.quasardb.qdb.jni.SWIGTYPE_p_qdb_session;
import net.quasardb.qdb.jni.qdb;
import net.quasardb.qdb.jni.qdb_error_t;
import net.quasardb.qdb.jni.qdb_operation_t;
import net.quasardb.qdb.jni.qdb_operation_type_t;
import net.quasardb.qdb.jni.run_batch_result;

public class QdbBatchResult {
    private long nbOperations = 0;
    private long nbSuccess = 0;
    private Result [] results = null;
    private SWIGTYPE_p_qdb_session session = null;
    private run_batch_result batchResults = null;


    private void updateResults(BatchOpsVec operations)
    {
        results = new Result[(int)this.nbOperations];

        for (int i = 0; i < this.nbOperations; i++) {
            results[i] = new Result(batchResults.getResults().get(i));
        }
    }

    public QdbBatchResult() {}

    public QdbBatchResult(SWIGTYPE_p_qdb_session sess, BatchOpsVec operations, run_batch_result batch_res) {
        this.session = sess;
        this.nbOperations = operations.size();

        this.batchResults = batch_res;

        this.nbSuccess = batchResults.getSuccesses();

        this.updateResults(operations);
    }

    /**
     * If you forgot to call release() we have you covered.
     */
    @Override
    protected void finalize() throws Throwable {
        this.release();
        super.finalize();
    }

    /**
     * Explicit release of quasardb allocated memory.
     * Use this function when finalize() is called to late for your taste.
     *
     * Be sure to not call release when you are still working on the results object.
     *
     * If you are not sure, just let finalize() call release()
     *
     */
    public void release() {
        if (batchResults != null) {
            // reset values to avoid dangling access in case of resurection
            this.nbOperations = this.nbSuccess = 0;

            results = null;

            // release quasardb allocated memory for the result
            qdb.release_batch_result(session, batchResults);

            batchResults = null;
        }
    }

    /**
     * @return the success
     */
    public boolean isSuccess() {
        return nbOperations == nbSuccess;
    }

    /**
     * @return the nbOperations
     */
    public long getOperationsCount() {
        return nbOperations;
    }

    /**
     * @return the nbSuccess
     */
    public long getSuccessesCount() {
        return nbSuccess;
    }

    public Result get(int i) {
        return results[i];
    }

    public int getResultsLength() {
        return results.length;
    }

}
