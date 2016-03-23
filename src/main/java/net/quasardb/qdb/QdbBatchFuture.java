package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;

public abstract class QdbBatchFuture<T> implements QdbFuture<T> {
    private final QdbBatch batch;
    private final int index;

    protected QdbBatchFuture(QdbBatch batch, int index) {
        this.batch = batch;
        this.index = index;
    }

    protected abstract T getResult(qdb_operation_t operation);

    public final T get() {
        if (!batch.hasRun())
            throw new QdbBatchNotRunException();
        qdb_operation_t op = batch.getOperation(index);
        QdbErrorCode err = new QdbErrorCode(op.getError());
        QdbExceptionFactory.throwIfError(err);
        return getResult(op);
    }

    public boolean success() {
        if (!batch.hasRun())
            throw new QdbBatchNotRunException();
        qdb_operation_t op = batch.getOperation(index);
        QdbErrorCode err = new QdbErrorCode(op.getError());
        return err.severity() == qdb_error_severity_t.error_severity_info;
    }
}
