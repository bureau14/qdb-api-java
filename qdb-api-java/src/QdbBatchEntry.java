package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;

/**
 * Add operations in a batch.
 */
public class QdbBatchEntry {
    protected final QdbBatch batch;
    protected final String alias;

    protected QdbBatchEntry(QdbBatch batch, String alias) {
        this.batch = batch;
        this.alias = alias;
    }

    /**
     * Adds an "remove" operation to the batch: "Removes the entry from the database."
     *
     * @return A future that will contain the result of the op after the batch is executed.
     * @see QdbEntry#remove()
     */
    public QdbFuture<Void> remove() {
        assertNotAlreadyRun();

        qdb_operation_t op = new qdb_operation_t();
        op.setType(qdb_operation_type_t.qdb_op_remove);
        op.setAlias(alias);

        int index = batch.addOperation(op);

        return new QdbBatchFuture<Void>(batch, index) {
            @Override
            protected Void getResult(qdb_operation_t op) {
                return null;
            }
        };
    }

    protected void assertNotAlreadyRun() {
        if (batch.hasRun())
            throw new QdbBatchAlreadyRunException();
    }
}
