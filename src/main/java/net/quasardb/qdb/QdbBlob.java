package net.quasardb.qdb;

import java.nio.ByteBuffer;
import net.quasardb.qdb.jni.*;

/**
 * A blob in the database.
 * Blob stands for "Binary Large Object", it's an entry which store binary data.
 */
public final class QdbBlob extends QdbExpirableEntry {
    // Protected constructor. Call QdbCluster.blob() to get an instance.
    protected QdbBlob(QdbSession session, String alias) {
        super(session, alias);
    }

    /**
     * Atomically compares the content of the blob and replaces it, if it matches.
     *
     * @param newContent The content to be updated to the server in case of match.
     * @param comparand The content to be compared to.
     * @return Returns The original content if comparand doesn't match. Returns null otherwise.
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public QdbBuffer compareAndSwap(ByteBuffer newContent, ByteBuffer comparand) {
        return this.compareAndSwap(newContent, comparand, QdbExpiryTime.PRESERVE_EXPIRATION);
    }

    /**
     * Atomically compares the content of the blob and replaces it, if it matches.
     *
     * @param newContent The content to be updated to the server, in case of match.
     * @param comparand The content to be compared to.
     * @param expiryTime The new expiry time of the blob, in case of match
     * @return Returns The original content if comparand doesn't match. Returns null otherwise.
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbInvalidArgumentException If the expiry time is in the past (with a certain tolerance)
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public QdbBuffer compareAndSwap(ByteBuffer newContent, ByteBuffer comparand, QdbExpiryTime expiryTime) {
        session.throwIfClosed();
        Reference<ByteBuffer> originalContent = new Reference<ByteBuffer>();
        int err = qdb.blob_compare_and_swap(session.handle(), alias, newContent, comparand, expiryTime.toSecondsSinceEpoch(), originalContent);
        QdbExceptionFactory.throwIfError(err);
        return session.wrapBuffer(originalContent);
    }

    /**
      * Read the content of the blob.
      *
      * @return The current content.
      * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
      * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
      * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public QdbBuffer get() {
        session.throwIfClosed();
        Reference<ByteBuffer> content = new Reference<ByteBuffer>();
        int err = qdb.blob_get(session.handle(), alias, content);
        QdbExceptionFactory.throwIfError(err);
        return session.wrapBuffer(content);
    }

    /**
     * Atomically reads the content of the blob and removes it.
     *
     * @return The content of the blob, before being removed.
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public QdbBuffer getAndRemove() {
        session.throwIfClosed();
        Reference<ByteBuffer> content = new Reference<ByteBuffer>();
        int err = qdb.blob_get_and_remove(session.handle(), alias, content);
        QdbExceptionFactory.throwIfError(err);
        return session.wrapBuffer(content);
    }

    /**
     * Atomically reads and replaces (in this order) the content of blob.
     *
     * @param content The content of the blob to be set, before being replaced.
     * @return A buffer representing the content of the blob, before the update.
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public QdbBuffer getAndUpdate(ByteBuffer content) {
        return this.getAndUpdate(content, QdbExpiryTime.PRESERVE_EXPIRATION);
    }

    /**
     * Atomically reads and replaces (in this order) the content of blob.
     *
     * @param content The content of the blob to be set, before being replaced.
     * @param expiryTime The new expiry time of the blob.
     * @return The content of the blob to be set, before being replaced.
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbInvalidArgumentException If the expiry time is in the past (with a certain tolerance)
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public QdbBuffer getAndUpdate(ByteBuffer content, QdbExpiryTime expiryTime) {
        session.throwIfClosed();
        Reference<ByteBuffer> originalContent = new Reference<ByteBuffer>();
        int err = qdb.blob_get_and_update(session.handle(), alias, content, expiryTime.toSecondsSinceEpoch(), originalContent);
        QdbExceptionFactory.throwIfError(err);
        return session.wrapBuffer(originalContent);
    }

    /**
     * Create a new blob with the specified content. Fails if the blob already exists.
     *
     * @param content The content of the blob to be created.
     * @throws QdbAliasAlreadyExistsException If an entry matching the provided alias already exists.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public void put(ByteBuffer content) {
        this.put(content, QdbExpiryTime.NEVER_EXPIRES);
    }

    /**
     * Create a new blob with the specified content. Fails if the blob already exists.
     *
     * @param content The content of the blob to be created.
     * @param expiryTime The expiry time of the blob.
     * @throws QdbAliasAlreadyExistsException If an entry matching the provided alias already exists.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     * @throws QdbInvalidArgumentException If the expiry time is in the past (with a certain tolerance)
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public void put(ByteBuffer content, QdbExpiryTime expiryTime) {
        session.throwIfClosed();
        int err = qdb.blob_put(session.handle(), alias, content, expiryTime.toSecondsSinceEpoch());
        QdbExceptionFactory.throwIfError(err);
    }

    /**
     * Removes the blob if its content matches comparand.
     *
     * @param comparand The content to be compared to.
     * @return true if the blob was actually removed, false if not.
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean removeIf(ByteBuffer comparand) {
        session.throwIfClosed();
        int err = qdb.blob_remove_if(session.handle(), alias, comparand);
        QdbExceptionFactory.throwIfError(err);
        return err != qdb_error.unmatched_content;
    }

    /**
      * Replaces the content of the blob.
      *
      * @param content The content of the blob to be set.
      * @return true if the blob was created, or false it it was updated.
      * @throws QdbClusterClosedException If QdbCluster.close() has been called.
      * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
      * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public boolean update(ByteBuffer content) {
        return this.update(content, QdbExpiryTime.PRESERVE_EXPIRATION);
    }

    /**
      * Replaces the content of the blob.
      *
      * @param content The content of the blob to be set.
      * @param expiryTime The new expiry time of the blob.
      * @return true if the blob was created, or false it it was updated.
      * @throws QdbClusterClosedException If QdbCluster.close() has been called.
      * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
      * @throws QdbInvalidArgumentException If the expiry time is in the past (with a certain tolerance)
      * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public boolean update(ByteBuffer content, QdbExpiryTime expiryTime) {
        session.throwIfClosed();
        int err = qdb.blob_update(session.handle(), alias, content, expiryTime.toSecondsSinceEpoch());
        QdbExceptionFactory.throwIfError(err);
        return err == qdb_error.ok_created;
    }
}
