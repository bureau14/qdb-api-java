package net.quasardb.qdb;

import java.nio.ByteBuffer;
import net.quasardb.qdb.jni.*;

/**
 * A blob in the database.
 * Blob standands for "Binary Large Object", it's an entry which store binary data.
 */
public final class QdbBlob extends QdbExpirableEntry {
    // Protected constructor. Call QdbCluster.getBlob() to create a QdbBlob
    protected QdbBlob(SWIGTYPE_p_qdb_session session, String alias) {
        super(session, alias);
    }

    /**
     * Atomically compares the content of the blob and replaces it, if it matches.
     *
     * @param newContent The content to be updated to the server in case of match.
     * @param comparand The content to be compared to.
     * @return Returns The original content if comparand doesn't match. Returns null otherwise.
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public ByteBuffer compareAndSwap(ByteBuffer newContent, ByteBuffer comparand) {
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
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbInvalidArgumentException If the expiry time is in the past (with a certain tolerance)
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public ByteBuffer compareAndSwap(ByteBuffer newContent, ByteBuffer comparand, QdbExpiryTime expiryTime) {
        error_carrier err = new error_carrier();
        ByteBuffer value = qdb.blob_compare_and_swap(session, alias, newContent, newContent.limit(), comparand, comparand.limit(), expiryTime.toSecondsSinceEpoch(), err);
        if (err.getError() == qdb_error_t.error_unmatched_content)
            return value;
        QdbExceptionThrower.throwIfError(err);
        return null; // comparand matched
    }

    /**
     * Read the content of the blob.
     *
     * @return The current content.
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public ByteBuffer get() {
        error_carrier err = new error_carrier();
        ByteBuffer value = qdb.blob_get(session, alias, err);
        QdbExceptionThrower.throwIfError(err);
        return value;
    }

    /**
     * Atomically reads the content of the blob and removes it.
     *
     * @return The content of the blob, before being removed.
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public ByteBuffer getAndRemove() {
        error_carrier err = new error_carrier();
        ByteBuffer value = qdb.blob_get_and_remove(session, alias, err);
        QdbExceptionThrower.throwIfError(err);
        return value;
    }

    /**
     * Atomically reads and replaces (in this order) the content of blob.
     *
     * @param content The content of the blob to be set, before being replaced.
     * @return A buffer representing the content of the blob, before the update.
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public ByteBuffer getAndUpdate(ByteBuffer content) {
        return this.getAndUpdate(content, QdbExpiryTime.PRESERVE_EXPIRATION);
    }

    /**
     * Atomically reads and replaces (in this order) the content of blob.
     *
     * @param content The content of the blob to be set, before being replaced.
     * @param expiryTime The new expiry time of the blob.
     * @return The content of the blob to be set, before being replaced.
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbInvalidArgumentException If the expiry time is in the past (with a certain tolerance)
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public ByteBuffer getAndUpdate(ByteBuffer content, QdbExpiryTime expiryTime) {
        error_carrier err = new error_carrier();
        ByteBuffer value = qdb.blob_get_and_update(session, alias, content, content.limit(), expiryTime.toSecondsSinceEpoch(), err);
        QdbExceptionThrower.throwIfError(err);
        return value;
    }

    /**
     * Create a new blob with the specified content. Fails if the blob already exists.
     *
     * @param content The content of the blob to be created.
     * @throws QdbAliasAlreadyExistsException If an entry matching the provided alias already exists.
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
     * @throws QdbInvalidArgumentException If the expiry time is in the past (with a certain tolerance)
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public void put(ByteBuffer content, QdbExpiryTime expiryTime) {
        qdb_error_t err = qdb.blob_put(session, alias, content, content.limit(), expiryTime.toSecondsSinceEpoch());
        QdbExceptionThrower.throwIfError(err);
    }

    /**
     * Replaces the content of the blob.
     *
     * @param content The content of the blob to be set.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public void update(ByteBuffer content) {
        this.update(content, QdbExpiryTime.PRESERVE_EXPIRATION);
    }

    /**
     * Replaces the content of the blob.
     *
     * @param content The content of the blob to be set.
     * @param expiryTime The new expiry time of the blob.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbInvalidArgumentException If the expiry time is in the past (with a certain tolerance)
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public void update(ByteBuffer content, QdbExpiryTime expiryTime) {
        qdb_error_t err = qdb.blob_update(session, alias, content, content.limit(), expiryTime.toSecondsSinceEpoch());
        QdbExceptionThrower.throwIfError(err);
    }

    /**
     * Removes the blob if its content matches comparand.
     *
     * @param comparand The content to be compared to.
     * @return true if the blob was actually removed, false if not.
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean removeIf(ByteBuffer comparand) {
        qdb_error_t err = qdb.blob_remove_if(session, alias, comparand, comparand.limit());
        if (err == qdb_error_t.error_unmatched_content)
            return false;
        QdbExceptionThrower.throwIfError(err);
        return true;
    }
}
