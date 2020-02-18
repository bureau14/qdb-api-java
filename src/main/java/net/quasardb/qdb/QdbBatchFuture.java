package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.*;

public final class QdbBatchFuture<T> implements QdbFuture<T> {
    private final QdbBatch batch;
    private final QdbBatchOperation op;

    protected QdbBatchFuture(QdbBatch batch, QdbBatchOperation op) {
        this.batch = batch;
        this.op = op;
    }

    public final T get() {
        if (!batch.hasRun())
            throw new BatchNotRunException("Batch did not run");
        return (T)op.result;
    }

    public boolean success() {
        if (!batch.hasRun())
            throw new BatchNotRunException("Batch did not run");
        return qdb_error.severity(op.error) == qdb_err_severity.info;
    }
}
