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

        QdbBatchOperation op = new QdbBatchOperation() {
            @Override
            public void write(long batch, int index) {
                qdb.batch_write_blob_compare_and_swap(batch, index, alias, newContent, comparand, expiryTime.toSecondsSinceEpoch());
            }

            @Override
            public void read(long batch, int index) {
                Reference<ByteBuffer> originalContent = new Reference<ByteBuffer>();
                error = qdb.batch_read_blob_compare_and_swap(batch, index, alias, originalContent);
                result = originalContent.value;
            }
        };
        batch.addOperation(op);

        return new QdbBatchFuture<ByteBuffer>(batch, op);
    }

    /**
     * Adds a "get" operation to the batch: "Read the content of the blob."
     *
     * @return A future that will contain the result of the operation after the batch is run.
     * @see QdbBlob#get()
     */
    public QdbFuture<ByteBuffer> get() {
        assertNotAlreadyRun();

        QdbBatchOperation op = new QdbBatchOperation() {
            @Override
            public void write(long batch, int index) {
                qdb.batch_write_blob_get(batch, index, alias);
            }

            @Override
            public void read(long batch, int index) {
                Reference<ByteBuffer> content = new Reference<ByteBuffer>();
                error = qdb.batch_read_blob_get(batch, index, alias, content);
                result = content.value;
            }
        };
        batch.addOperation(op);

        return new QdbBatchFuture<ByteBuffer>(batch, op);
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

        QdbBatchOperation op = new QdbBatchOperation() {
            @Override
            public void write(long batch, int index) {
                qdb.batch_write_blob_get_and_update(batch, index, alias, content, expiryTime.toSecondsSinceEpoch());
            }

            @Override
            public void read(long batch, int index) {
                Reference<ByteBuffer> content = new Reference<ByteBuffer>();
                error = qdb.batch_read_blob_get_and_update(batch, index, alias, content);
                result = content.value;
            }
        };
        batch.addOperation(op);

        return new QdbBatchFuture<ByteBuffer>(batch, op);
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

        QdbBatchOperation op = new QdbBatchOperation() {
            @Override
            public void write(long batch, int index) {
                qdb.batch_write_blob_put(batch, index, alias, content, expiryTime.toSecondsSinceEpoch());
            }

            @Override
            public void read(long batch, int index) {
                Reference<ByteBuffer> content = new Reference<ByteBuffer>();
                error = qdb.batch_read_blob_put(batch, index, alias);
                result = null;
            }
        };
        batch.addOperation(op);

        return new QdbBatchFuture<Void>(batch, op);
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

        QdbBatchOperation op = new QdbBatchOperation() {
            @Override
            public void write(long batch, int index) {
                qdb.batch_write_blob_update(batch, index, alias, content, expiryTime.toSecondsSinceEpoch());
            }

            @Override
            public void read(long batch, int index) {
                Reference<ByteBuffer> content = new Reference<ByteBuffer>();
                error = qdb.batch_read_blob_update(batch, index, alias);
                result = null;
            }
        };
        batch.addOperation(op);

        return new QdbBatchFuture<Void>(batch, op);
    }
}
