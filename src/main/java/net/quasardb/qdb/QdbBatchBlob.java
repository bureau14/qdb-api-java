package net.quasardb.qdb;

import java.nio.ByteBuffer;
import net.quasardb.qdb.jni.*;

/**
 * Add blob operations in a batch.
 */
public final class QdbBatchBlob extends QdbBatchEntry {
    // Protected constructor. Call QdbCluster.blob() to get an instance.
    protected QdbBatchBlob(QdbBatch batch, String alias) {
        super(batch, alias);
    }

    /**
     * Adds a "compareAndSwap" operation to the batch: "Atomically compares the content of the blob and replaces it, if it matches."
     *
     * @param newContent The content to be updated to the server in case of match.
     * @param comparand The content to be compared to.
     * @return A future that will contain the result of the operation after the batch is run.
     * @see QdbBlob#compareAndSwap(ByteBuffer, ByteBuffer)
     */
    public QdbFuture<ByteBuffer> compareAndSwap(ByteBuffer newContent, ByteBuffer comparand) {
        return this.compareAndSwap(newContent, comparand, QdbExpiryTime.PRESERVE_EXPIRATION);
    }

    /**
     * Adds a "compareAndSwap" operation to the batch: "Atomically compares the content of the blob and replaces it, if it matches.""
     *
     * @param newContent The content to be updated to the server, in case of match.
     * @param comparand The content to be compared to.
     * @param expiryTime The new expiry time of the blob, in case of match
     * @return A future that will contain the result of the operation after the batch is run.
     * @see QdbBlob#compareAndSwap(ByteBuffer, ByteBuffer, QdbExpiryTime)
     */
    public QdbFuture<ByteBuffer> compareAndSwap(ByteBuffer newContent, ByteBuffer comparand, QdbExpiryTime expiryTime) {
        assertNotAlreadyRun();

        qdb_operation_t op = new qdb_operation_t();
        op.setType(qdb_operation_type_t.qdb_op_blob_cas);
        op.setAlias(alias);
        op.setContent(newContent);
        op.setContent_size(newContent.limit());
        op.setComparand(comparand);
        op.setComparand_size(comparand.limit());
        op.setExpiry_time(expiryTime.toSecondsSinceEpoch());

        int index = batch.addOperation(op);

        return new QdbBatchFuture<ByteBuffer>(batch, index) {
            @Override
            protected ByteBuffer getResult(qdb_operation_t op) {
                return op.getResult();
            }
        };
    }

    /**
     * Adds a "get" operation to the batch: "Read the content of the blob."
     *
     * @return A future that will contain the result of the operation after the batch is run.
     * @see QdbBlob#get()
     */
    public QdbFuture<ByteBuffer> get() {
        assertNotAlreadyRun();

        qdb_operation_t op = new qdb_operation_t();
        op.setType(qdb_operation_type_t.qdb_op_blob_get);
        op.setAlias(alias);

        int index = batch.addOperation(op);

        return new QdbBatchFuture<ByteBuffer>(batch, index) {
            @Override
            protected ByteBuffer getResult(qdb_operation_t op) {
                return op.getResult();
            }
        };
    }

    /**
     * Adds a "getAndRemove" operation to the batch: "Atomically reads the content of the blob and removes it."
     *
     * @return A future that will contain the result of the operation after the batch is run.
     * @see QdbBlob#getAndRemove()
     */
    public QdbFuture<ByteBuffer> getAndRemove() {
        assertNotAlreadyRun();

        qdb_operation_t op = new qdb_operation_t();
        op.setType(qdb_operation_type_t.qdb_op_blob_get_and_remove);
        op.setAlias(alias);

        int index = batch.addOperation(op);

        return new QdbBatchFuture<ByteBuffer>(batch, index) {
            @Override
            protected ByteBuffer getResult(qdb_operation_t op) {
                return op.getResult();
            }
        };
    }

    /**
     * Adds a "getAndUpdate" operation to the batch: "Atomically reads and replaces (in this order) the content of blob."
     *
     * @param content The content of the blob to be set, before being replaced.
     * @return A future that will contain the result of the operation after the batch is run.
     * @see QdbBlob#getAndUpdate(ByteBuffer)
     */
    public QdbFuture<ByteBuffer> getAndUpdate(ByteBuffer content) {
        return this.getAndUpdate(content, QdbExpiryTime.PRESERVE_EXPIRATION);
    }

    /**
     * Adds a "getAndUpdate" operation to the batch: "Atomically reads and replaces (in this order) the content of blob."
     *
     * @param content The content of the blob to be set, before being replaced.
     * @param expiryTime The new expiry time of the blob.
     * @return A future that will contain the result of the operation after the batch is run.
     * @see QdbBlob#getAndUpdate(ByteBuffer,QdbExpiryTime)
     */
    public QdbFuture<ByteBuffer> getAndUpdate(ByteBuffer content, QdbExpiryTime expiryTime) {
        assertNotAlreadyRun();

        qdb_operation_t op = new qdb_operation_t();
        op.setType(qdb_operation_type_t.qdb_op_blob_get_and_update);
        op.setAlias(alias);
        op.setContent(content);
        op.setContent_size(content.limit());
        op.setExpiry_time(expiryTime.toSecondsSinceEpoch());

        int index = batch.addOperation(op);

        return new QdbBatchFuture<ByteBuffer>(batch, index) {
            @Override
            protected ByteBuffer getResult(qdb_operation_t op) {
                return op.getResult();
            }
        };
    }

    /**
     * Adds a "put" operation to the batch: "Create a new blob with the specified content. Fails if the blob already exists."
     *
     * @param content The content of the blob to be created.
     * @return A future that will contain the result of the operation after the batch is run.
     * @see QdbBlob#put(ByteBuffer)
     */
    public QdbFuture<Void> put(ByteBuffer content) {
        return this.put(content, QdbExpiryTime.NEVER_EXPIRES);
    }

    /**
     * Adds a "put" operation to the batch: "Create a new blob with the specified content. Fails if the blob already exists."
     *
     * @param content The content of the blob to be created.
     * @param expiryTime The expiry time of the blob.
     * @return A future that will contain the result of the operation after the batch is run.
     * @see QdbBlob#put(ByteBuffer,QdbExpiryTime)
     */
    public QdbFuture<Void> put(ByteBuffer content, QdbExpiryTime expiryTime) {
        assertNotAlreadyRun();

        qdb_operation_t op = new qdb_operation_t();
        op.setType(qdb_operation_type_t.qdb_op_blob_put);
        op.setAlias(alias);
        op.setContent(content);
        op.setContent_size(content.limit());
        op.setExpiry_time(expiryTime.toSecondsSinceEpoch());

        int index = batch.addOperation(op);

        return new QdbBatchFuture<Void>(batch, index) {
            @Override
            protected Void getResult(qdb_operation_t op) {
                return null;
            }
        };
    }

    /**
     * Adds a "removeIf" operation to the batch: "Removes the blob if its content matches comparand."
     *
     * @param comparand The content to be compared to.
     * @return A future that will contain the result of the operation after the batch is run.
     * @see QdbBlob#removeIf(ByteBuffer)
     */
    public QdbFuture<Boolean> removeIf(ByteBuffer comparand) {
        assertNotAlreadyRun();

        qdb_operation_t op = new qdb_operation_t();
        op.setType(qdb_operation_type_t.qdb_op_blob_remove_if);
        op.setAlias(alias);
        op.setComparand(comparand);
        op.setComparand_size(comparand.limit());

        int index = batch.addOperation(op);

        return new QdbBatchFuture<Boolean>(batch, index) {
            @Override
            protected Boolean getResult(qdb_operation_t op) {
                return op.getError() != qdb_error_t.error_unmatched_content;
            }
        };
    }

    /**
     * Adds an "update" operation to the batch: "Replaces the content of the blob."
     *
     * @param content The content of the blob to be set.
     * @return A future that will contain the result of the operation after the batch is run.
     * @see QdbBlob#update(ByteBuffer)
     */
    public QdbFuture<Void> update(ByteBuffer content) {
        return this.update(content, QdbExpiryTime.PRESERVE_EXPIRATION);
    }

    /**
     * Adds an "update" operation to the batch: "Replaces the content of the blob."
     *
     * @param content The content of the blob to be set.
     * @param expiryTime The new expiry time of the blob.
     * @return A future that will contain the result of the operation after the batch is run.
     * @see QdbBlob#update(ByteBuffer,QdbExpiryTime)
     */
    public QdbFuture<Void> update(ByteBuffer content, QdbExpiryTime expiryTime) {
        assertNotAlreadyRun();

        qdb_operation_t op = new qdb_operation_t();
        op.setType(qdb_operation_type_t.qdb_op_blob_update);
        op.setAlias(alias);
        op.setContent(content);
        op.setContent_size(content.limit());
        op.setExpiry_time(expiryTime.toSecondsSinceEpoch());

        int index = batch.addOperation(op);

        return new QdbBatchFuture<Void>(batch, index) {
            @Override
            protected Void getResult(qdb_operation_t op) {
                return null;
            }
        };
    }
}
