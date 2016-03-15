package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;

/**
 * A signed 64-bit integer in the database.
 */
public final class QdbInteger extends QdbExpirableEntry {
    // Protected constructor. Call QdbCluster.integer() to get an instance.
    protected QdbInteger(QdbSession session, String alias) {
        super(session, alias);
    }

    /**
     * Atomically adds the given value to the current value.
     *
     * @param delta The increment to add to the current value.
     * @return The resulting value after the operation.
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public long add(long delta) {
        error_carrier err = new error_carrier();
        long res = qdb.int_add(session.handle(), alias, delta, err);
        QdbExceptionFactory.throwIfError(err);
        return res;
    }

    /**
     * Reads the current value of the integer.
     *
     * @return The current value
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public long get() {
        error_carrier err = new error_carrier();
        long value = qdb.int_get(session.handle(), alias, err);
        QdbExceptionFactory.throwIfError(err);
        return value;
    }

    /**
     * Creates a new integer. Errors if the integer already exists.
     *
     * @param initialValue The value of the new integer.
     * @throws QdbAliasAlreadyExistsException If an entry matching the provided alias already exists.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public void put(long initialValue) {
        this.put(initialValue, QdbExpiryTime.NEVER_EXPIRES);
    }

    /**
     * Creates a new integer. Errors if the integer already exists.
     *
     * @param initialValue The value of the new integer.
     * @param expiryTime The expiry time of the entry.
     * @throws QdbAliasAlreadyExistsException If an entry matching the provided alias already exists.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public void put(long initialValue, QdbExpiryTime expiryTime) {
        qdb_error_t err = qdb.int_put(session.handle(), alias, initialValue, expiryTime.toSecondsSinceEpoch());
        QdbExceptionFactory.throwIfError(err);
    }

    /**
     * Updates an existing integer or creates one if it does not exist.
     *
     * @param newValue The new value of the integer.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public void update(long newValue) {
        update(newValue, QdbExpiryTime.PRESERVE_EXPIRATION);
    }

    /**
     * Updates an existing integer or creates one if it does not exist.
     *
     * @param newValue The new value of the integer.
     * @param expiryTime The expiry time of the entry.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public void update(long newValue, QdbExpiryTime expiryTime) {
        qdb_error_t err = qdb.int_update(session.handle(), alias, newValue, expiryTime.toSecondsSinceEpoch());
        QdbExceptionFactory.throwIfError(err);
    }
}
