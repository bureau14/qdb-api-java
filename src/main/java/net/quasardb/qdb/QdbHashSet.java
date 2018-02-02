package net.quasardb.qdb;

import java.nio.ByteBuffer;
import net.quasardb.qdb.exception.*;
import net.quasardb.qdb.jni.*;

/**
 * A hash-set in the database
 */
public final class QdbHashSet extends QdbEntry {
    protected QdbHashSet(Session session, String alias) {
        super(session, alias);
    }

    /**
     * Determines if a set has a given value. The set must already exist.
     *
     * @param content The value to search for and compare against.
     * @return true if the value is in the hash-set, false if not.
     * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean contains(ByteBuffer content) {
        session.throwIfClosed();
        int err = qdb.hset_contains(session.handle(), alias, content);
        ExceptionFactory.throwIfError(err);
        return err != qdb_error.element_not_found;
    }

    /**
     * Removes a value from a set. The set must already exist.
     *
     * @param content The value to search for and remove.
     * @return true if the value was in the hash-set, false if not.
     * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean erase(ByteBuffer content) {
        session.throwIfClosed();
        int err = qdb.hset_erase(session.handle(), alias, content);
        ExceptionFactory.throwIfError(err);
        return err != qdb_error.element_not_found;
    }

    /**
     * Inserts a value into a hset. Creates the hset if it does not already exist.
     *
     * @param content The value to add in the hash-set.
     * @return true if the value has been added, false if it was already present.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws IncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean insert(ByteBuffer content) {
        session.throwIfClosed();
        int err = qdb.hset_insert(session.handle(), alias, content);
        ExceptionFactory.throwIfError(err);
        return err != qdb_error.element_already_exists;
    }
}
