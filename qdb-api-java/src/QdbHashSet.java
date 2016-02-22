package net.quasardb.qdb;

import java.nio.ByteBuffer;
import net.quasardb.qdb.jni.*;

/**
 * A hash-set in the database
 */
public final class QdbHashSet extends QdbEntry {
    protected QdbHashSet(SWIGTYPE_p_qdb_session session, String alias) {
        super(session, alias);
    }

    /**
     * Determines if a set has a given value. The set must already exist.
     *
     * @param content The value to search for and compare against.
     * @return true if the value is in the hash-set, false if not.
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean contains(ByteBuffer content) {
        qdb_error_t err = qdb.hset_contains(session, alias, content, content.limit());

        if (err == qdb_error_t.error_element_not_found)
            return false;
        QdbExceptionThrower.throwIfError(err);
        return true;
    }

    /**
     * Removes a value from a set. The set must already exist.
     *
     * @param content The value to search for and remove.
     * @return true if the value was in the hash-set, false if not.
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean erase(ByteBuffer content) {
        qdb_error_t err = qdb.hset_erase(session, alias, content, content.limit());

        if (err == qdb_error_t.error_element_not_found)
            return false;
        QdbExceptionThrower.throwIfError(err);
        return true;
    }

    /**
     * Inserts a value into a hset. Creates the hset if it does not already exist.
     *
     * @param content The value to add in the hash-set.
     * @return true if the value has been added, false if it was already present.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean insert(ByteBuffer content) {
        qdb_error_t err = qdb.hset_insert(session, alias, content, content.limit());

        if (err == qdb_error_t.error_element_already_exists)
            return false;
        QdbExceptionThrower.throwIfError(err);
        return true;
    }
}
