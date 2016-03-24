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

    protected void assertNotAlreadyRun() {
        if (batch.hasRun())
            throw new QdbBatchAlreadyRunException();
    }
}
