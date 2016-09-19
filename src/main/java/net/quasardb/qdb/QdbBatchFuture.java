package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;

public final class QdbBatchFuture<T> implements QdbFuture<T> {
    private final QdbBatch batch;
    private final QdbBatchOperation op;

    protected QdbBatchFuture(QdbBatch batch, QdbBatchOperation op) {
        this.batch = batch;
        this.op = op;
    }

    public final T get() {
        if (!batch.hasRun())
            throw new QdbBatchNotRunException();
        QdbExceptionFactory.throwIfError(op.error);
        return (T)op.result;
    }

    public boolean success() {
        if (!batch.hasRun())
            throw new QdbBatchNotRunException();
        return qdb_error.severity(op.error) == qdb_err_severity.info;
    }
}
