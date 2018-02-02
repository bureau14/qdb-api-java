package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.*;

/**
 * A signed 64-bit integer in the database.
 */
public final class QdbInteger extends QdbExpirableEntry {
    // Protected constructor. Call QdbCluster.integer() to get an instance.
    protected QdbInteger(Session session, String alias) {
        super(session, alias);
    }

    /**
     * Atomically adds the given value to the current value.
     *
     * @param delta The increment to add to the current value.
     * @return The resulting value after the operation.
     * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public long add(long delta) {
        session.throwIfClosed();
        Reference<Long> result = new Reference<Long>();
        int err = qdb.int_add(session.handle(), alias, delta, result);
        ExceptionFactory.throwIfError(err);
        return result.value;
    }

    /**
     * Reads the current value of the integer.
     *
     * @return The current value
     * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public long get() {
        session.throwIfClosed();
        Reference<Long> value = new Reference<Long>();
        int err = qdb.int_get(session.handle(), alias, value);
        ExceptionFactory.throwIfError(err);
        return value.value;
    }

    /**
     * Creates a new integer. Errors if the integer already exists.
     *
     * @param initialValue The value of the new integer.
     * @throws AliasAlreadyExistsException If an entry matching the provided alias already exists.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public void put(long initialValue) {
        this.put(initialValue, QdbExpiryTime.NEVER_EXPIRES);
    }

    /**
     * Creates a new integer. Errors if the integer already exists.
     *
     * @param initialValue The value of the new integer.
     * @param expiryTime The expiry time of the entry.
     * @throws AliasAlreadyExistsException If an entry matching the provided alias already exists.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public void put(long initialValue, QdbExpiryTime expiryTime) {
        session.throwIfClosed();
        int err = qdb.int_put(session.handle(), alias, initialValue, expiryTime.toMillisSinceEpoch());
        ExceptionFactory.throwIfError(err);
    }

    /**
     * Updates an existing integer or creates one if it does not exist.
     *
     * @param newValue The new value of the integer.
     * @return true if the integer was created, or false it it was updated.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean update(long newValue) {
        return update(newValue, QdbExpiryTime.PRESERVE_EXPIRATION);
    }

    /**
     * Updates an existing integer or creates one if it does not exist.
     *
     * @param newValue The new value of the integer.
     * @param expiryTime The expiry time of the entry.
     * @return true if the integer was created, or false it it was updated.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean update(long newValue, QdbExpiryTime expiryTime) {
        session.throwIfClosed();
        int err = qdb.int_update(session.handle(), alias, newValue, expiryTime.toMillisSinceEpoch());
        ExceptionFactory.throwIfError(err);
        return err == qdb_error.ok_created;
    }
}
