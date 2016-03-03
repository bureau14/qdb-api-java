package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;

/**
 * A signed 64-bit integer in the database.
 */
public final class QdbInteger extends QdbExpirableEntry {
    protected QdbInteger(SWIGTYPE_p_qdb_session session, String alias) {
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
        long res = qdb.int_add(session, alias, delta, err);
        QdbExceptionThrower.throwIfError(err);
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
        long value = qdb.int_get(session, alias, err);
        QdbExceptionThrower.throwIfError(err);
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
        this.put(initialValue, QdbExpiryTime.PRESERVE_EXPIRATION);
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
        qdb_error_t err = qdb.int_put(session, alias, initialValue, expiryTime.toSecondsSinceEpoch());
        QdbExceptionThrower.throwIfError(err);
    }

    /**
     * Updates an existing integer or creates one if it does not exist.
     *
     * @param newValue The new value of the integer.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public void set(long newValue) {
        qdb_error_t err = qdb.int_update(session, alias, newValue, 0);
        QdbExceptionThrower.throwIfError(err);
    }

    /**
     * Updates an existing integer or creates one if it does not exist.
     *
     * @param newValue The new value of the integer.
     * @param expiryTime The expiry time of the entry.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public void set(long newValue, QdbExpiryTime expiryTime) {
        qdb_error_t err = qdb.int_update(session, alias, newValue, 0);
        QdbExceptionThrower.throwIfError(err);
    }
}
