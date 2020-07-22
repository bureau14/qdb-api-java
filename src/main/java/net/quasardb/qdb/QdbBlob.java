package net.quasardb.qdb;

import java.nio.ByteBuffer;
import net.quasardb.qdb.Buffer;
import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.*;

/**
 * A blob in the database.
 * Blob stands for "Binary Large Object", it's an entry which store binary data.
 */
public final class QdbBlob extends QdbExpirableEntry {
    // Protected constructor. Call QdbCluster.blob() to get an instance.
    protected QdbBlob(Session session, String alias) {
        super(session, alias);
    }

    /**
     * Atomically compares the content of the blob and replaces it, if it matches.
     *
     * @param newContent The content to be updated to the server in case of match.
     * @param comparand The content to be compared to.
     * @return Returns The original content if comparand doesn't match. Returns null otherwise.
     * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public Buffer compareAndSwap(ByteBuffer newContent, ByteBuffer comparand) {
        assert(newContent.isDirect());
        assert(comparand.isDirect());
        session.throwIfClosed();

        return this.compareAndSwap(newContent, comparand, QdbExpiryTime.PRESERVE_EXPIRATION);
    }

    /**
     * Atomically compares the content of the blob and replaces it, if it matches.
     *
     * @param newContent The content to be updated to the server, in case of match.
     * @param comparand The content to be compared to.
     * @param expiryTime The new expiry time of the blob, in case of match
     * @return Returns The original content if comparand doesn't match. Returns null otherwise.
     * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws InvalidArgumentException If the expiry time is in the past (with a certain tolerance)
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public Buffer compareAndSwap(ByteBuffer newContent, ByteBuffer comparand, QdbExpiryTime expiryTime) {
        assert(newContent.isDirect());
        assert(comparand.isDirect());
        session.throwIfClosed();

        Reference<ByteBuffer> originalContent = new Reference<ByteBuffer>();
        qdb.blob_compare_and_swap(session.handle(), alias, newContent, comparand, expiryTime.toMillisSinceEpoch(), originalContent);
        return Buffer.wrap(session, originalContent);
    }

    /**
      * Read the content of the blob.
      *
      * @return The current content.
      * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
      * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
      * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public Buffer get() {
        session.throwIfClosed();
        Reference<ByteBuffer> content = new Reference<ByteBuffer>();
        qdb.blob_get(session.handle(), alias, content);
        return Buffer.wrap(session, content);
    }

    /**
     * Atomically reads the content of the blob and removes it.
     *
     * @return The content of the blob, before being removed.
     * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public Buffer getAndRemove() {
        session.throwIfClosed();
        Reference<ByteBuffer> content = new Reference<ByteBuffer>();
        qdb.blob_get_and_remove(session.handle(), alias, content);
        return Buffer.wrap(session, content);
    }

    /**
     * Atomically reads and replaces (in this order) the content of blob.
     *
     * @param content The content of the blob to be set, before being replaced.
     * @return A buffer representing the content of the blob, before the update.
     * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public Buffer getAndUpdate(ByteBuffer content) {
        assert(content.isDirect());

        return this.getAndUpdate(content, QdbExpiryTime.PRESERVE_EXPIRATION);
    }

    /**
     * Atomically reads and replaces (in this order) the content of blob.
     *
     * @param content The content of the blob to be set, before being replaced.
     * @param expiryTime The new expiry time of the blob.
     * @return The content of the blob to be set, before being replaced.
     * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws InvalidArgumentException If the expiry time is in the past (with a certain tolerance)
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public Buffer getAndUpdate(ByteBuffer content, QdbExpiryTime expiryTime) {
        assert(content.isDirect());
        session.throwIfClosed();
        Reference<ByteBuffer> originalContent = new Reference<ByteBuffer>();
        qdb.blob_get_and_update(session.handle(), alias, content, expiryTime.toMillisSinceEpoch(), originalContent);
        return Buffer.wrap(session, originalContent);
    }

    /**
     * Create a new blob with the specified content. Fails if the blob already exists.
     *
     * @param content The content of the blob to be created.
     * @throws AliasAlreadyExistsException If an entry matching the provided alias already exists.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public void put(ByteBuffer content) {
        this.put(content, QdbExpiryTime.NEVER_EXPIRES);
    }

    /**
     * Create a new blob with the specified content. Fails if the blob already exists.
     *
     * @param content The content of the blob to be created.
     * @param expiryTime The expiry time of the blob.
     * @throws AliasAlreadyExistsException If an entry matching the provided alias already exists.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws InvalidArgumentException If the expiry time is in the past (with a certain tolerance)
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public void put(ByteBuffer content, QdbExpiryTime expiryTime) {
        assert(content.isDirect());
        session.throwIfClosed();
        qdb.blob_put(session.handle(), alias, content, expiryTime.toMillisSinceEpoch());
    }

    /**
     * Removes the blob if its content matches comparand.
     *
     * @param comparand The content to be compared to.
     * @return true if the blob was actually removed, false if not.
     * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean removeIf(ByteBuffer comparand) {
        assert(comparand.isDirect());
        session.throwIfClosed();
        int err = qdb.blob_remove_if(session.handle(), alias, comparand);
        return err != qdb_error.unmatched_content;
    }

    /**
      * Replaces the content of the blob.
      *
      * @param content The content of the blob to be set.
      * @return true if the blob was created, or false it it was updated.
      * @throws ClusterClosedException If QdbCluster.close() has been called.
      * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
      * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public boolean update(ByteBuffer content) {
        assert(content.isDirect());
        return this.update(content, QdbExpiryTime.PRESERVE_EXPIRATION);
    }

    /**
      * Replaces the content of the blob.
      *
      * @param content The content of the blob to be set.
      * @param expiryTime The new expiry time of the blob.
      * @return true if the blob was created, or false it it was updated.
      * @throws ClusterClosedException If QdbCluster.close() has been called.
      * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
      * @throws InvalidArgumentException If the expiry time is in the past (with a certain tolerance)
      * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public boolean update(ByteBuffer content, QdbExpiryTime expiryTime) {
        assert(content.isDirect());
        session.throwIfClosed();
        int err = qdb.blob_update(session.handle(), alias, content, expiryTime.toMillisSinceEpoch());
        return err == qdb_error.ok_created;
    }
}
