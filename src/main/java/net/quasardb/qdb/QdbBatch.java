package net.quasardb.qdb;

import java.lang.AutoCloseable;
import java.util.LinkedList;
import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.*;

/**
 * A batch containing a list of operation.
 *
 * Batches are used to reduce the number of requests by performing several operations in one request.
 */
public final class QdbBatch implements AutoCloseable {
    private final Session session;
    private LinkedList<QdbBatchOperation> operations; // <- to keep O(1) insertion time
    private boolean hasRun;
    private int successCount;
    private long batch;

    // Protected constructor. Call  QdbCluster.createBatch() to create a batch.
    protected QdbBatch(Session session) {
        this.session = session;
        operations = new LinkedList<QdbBatchOperation>();
    }

    /**
     * Add blob operations to the batch.
     *
     * A call to this method is usually followed by a call to an operation.
     * For example:
     * {@code
     * QdbFuture<ByteBuffer> result = myBatch.blob("myBlob").get();
     * }
     *
     * @param alias The alias of the blob you want to add operations for.
     * @return A handle to a virtual blob on with to perform the operation.
     * @throws BatchClosedException If close() has been called.
     * @throws BatchAlreadyRunException If the run() has been called.
     */
    public QdbBatchBlob blob(String alias) {
        throwIfClosed();
        throwIfHasRun();
        return new QdbBatchBlob(this, alias);
    }

    /**
     * Executes all operations in the batch.
     *
     * A batch can only be run once.
     * Once a batch is run, most method will throw a BatchAlreadyRunException.
     *
     * @throws BatchClosedException If close() has been called.
     * @throws BatchAlreadyRunException If the run() has been called.
     */
    public void run() {
        throwIfClosed();
        throwIfHasRun();

        if (operations.size() > 0) {
            batch = create_batch(session, operations.size());
            write_operations_to_batch(batch, operations);
            successCount = qdb.run_batch(session.handle(), batch, operations.size());
            read_operations_from_batch(batch, operations);

            // batch is deleted in close(), so as to keep buffers alive
        }

        hasRun = true;
    }

    /**
     * If you forgot to call close() we have you covered.
     */
    @Override
    protected void finalize() throws Throwable {
        this.close();
        super.finalize();
    }

    /**
     * Release the memory allocated by quasardb.
     *
     * Once this method has been called, most other methods will throw BatchClosedException.
     */
    public void close() {
        if (batch != 0) {
            delete_batch(session, batch);
            batch = 0;
        }
        operations = null;
    }

    /**
     * Check if all operations executed successfully.
     *
     * @return true if all operations succeeded, false if any operation failed.
     * @throws BatchClosedException If close() has been called.
     * @throws BatchNotRunException If run() has not been called.
     */
    public boolean success() {
        throwIfClosed();
        throwIfNotRun();
        return operations.size() == successCount;
    }

    /**
     * Gets the total number of operations in the batch
     *
     * @return The number of operations that the batch contains.
     * @throws BatchClosedException If close() has been called.
     */
    public int operationCount() {
        throwIfClosed();
        return hasRun ? (int)operations.size() : operations.size();
    }

    /**
     * Gets the number of successful operations
     *
     * @return The number of successful operations
     * @throws BatchClosedException If close() has been called.
     * @throws BatchNotRunException If run() has not been called.
     */
    public int successCount() {
        throwIfClosed();
        throwIfNotRun();
        return successCount;
    }

    /**
     * Checks if close() has been called.
     *
     * @return true if batch has been closed, false if not
     */
    public boolean isClosed() {
        return operations == null;
    }

    protected void addOperation(QdbBatchOperation op) {
        throwIfClosed();
        throwIfHasRun();
        operations.addLast(op);
    }

    protected boolean hasRun() {
        throwIfClosed();
        return hasRun;
    }

    private void throwIfClosed() {
        if (isClosed())
            throw new BatchClosedException();
    }

    private void throwIfHasRun() {
        if (hasRun)
            throw new BatchAlreadyRunException();
    }

    private void throwIfNotRun() {
        if (!hasRun)
            throw new BatchNotRunException();
    }

    private static long create_batch(Session session, int count) {
        Reference<Long> batch = new Reference<Long>();
        int err = qdb.init_operations(session.handle(), count, batch);
        ExceptionFactory.throwIfError(err);
        return batch.value;
    }

    private static void delete_batch(Session session, long batch) {
        int err = qdb.delete_batch(session.handle(), batch);
        ExceptionFactory.throwIfError(err);
    }

    private static void write_operations_to_batch(long batch, Iterable<QdbBatchOperation> operations) {
        int index = 0;
        for (QdbBatchOperation op : operations)
            op.write(batch, index++);
    }

    private static void read_operations_from_batch(long batch, Iterable<QdbBatchOperation> operations) {
        int index = 0;
        for (QdbBatchOperation op : operations)
            op.read(batch, index++);
    }
}
