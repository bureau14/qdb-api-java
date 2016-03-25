package net.quasardb.qdb;

import java.lang.AutoCloseable;
import java.util.LinkedList;
import net.quasardb.qdb.jni.*;

/**
 * A batch containing a list of operation.
 *
 * Batches are used to reduce the number of requests by performing several operations in one request. 
 */
public final class QdbBatch implements AutoCloseable {
    private final QdbSession session;
    private LinkedList<qdb_operation_t> queuedOperations; // <- to keep O(1) insertion time
    private BatchOpsVec executedOperations;               // <- to store the results
    private boolean hasRun;
    private int successCount;

    // Protected constructor. Call  QdbCluster.createBatch() to create a batch.
    protected QdbBatch(QdbSession session) {
        this.session = session;
        queuedOperations = new LinkedList<qdb_operation_t>();
        executedOperations = new BatchOpsVec();
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
     * @throws QdbBatchClosedException If close() has been called.
     * @throws QdbBatchAlreadyRunException If the run() has been called.
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
     * Once a batch is run, most method will throw a QdbBatchAlreadyRunException.
     *
     * @throws QdbBatchClosedException If close() has been called.
     * @throws QdbBatchAlreadyRunException If the run() has been called.
     */
    public void run() {
        throwIfClosed();
        throwIfHasRun();

        executedOperations.reserve(queuedOperations.size());
        while (!queuedOperations.isEmpty())
            executedOperations.push_back(queuedOperations.removeFirst());

        successCount = (int)qdb.run_batch2(session.handle(), executedOperations);

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
     * Once this method has been called, most other methods will throw QdbBatchClosedException.
     */
    public void close() {
        if (executedOperations != null) {
            qdb.free_operations(session.handle(), executedOperations);
            executedOperations = null;
        }
    }

    /**
     * Check if all operations executed successfully.
     *
     * @return true if all operations succeeded, false if any operation failed.
     * @throws QdbBatchClosedException If close() has been called.
     * @throws QdbBatchNotRunException If run() has not been called.
     */
    public boolean success() {
        throwIfClosed();
        throwIfNotRun();
        return executedOperations.size() == successCount;
    }

    /**
     * Gets the total number of operations in the batch
     *
     * @return The number of operations that the batch contains.
     * @throws QdbBatchClosedException If close() has been called.
     */
    public int operationCount() {
        throwIfClosed();
        return hasRun ? (int)executedOperations.size() : queuedOperations.size();
    }

    /**
     * Gets the number of successful operations
     *
     * @return The number of successful operations
     * @throws QdbBatchClosedException If close() has been called.
     * @throws QdbBatchNotRunException If run() has not been called.
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
        return executedOperations == null;
    }

    protected int addOperation(qdb_operation_t op) {
        throwIfClosed();
        throwIfHasRun();
        int index = queuedOperations.size();
        queuedOperations.addLast(op);
        return index;
    }

    protected qdb_operation_t getOperation(int index) {
        throwIfClosed();
        return hasRun ? executedOperations.get(index) : queuedOperations.get(index);
    }

    protected boolean hasRun() {
        throwIfClosed();
        return hasRun;
    }

    private void throwIfClosed() {
        if (isClosed())
            throw new QdbBatchClosedException();
    }

    private void throwIfHasRun() {
        if (hasRun)
            throw new QdbBatchAlreadyRunException();
    }

    private void throwIfNotRun() {
        if (!hasRun)
            throw new QdbBatchNotRunException();
    }
}
