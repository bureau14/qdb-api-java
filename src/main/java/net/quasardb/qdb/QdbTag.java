package net.quasardb.qdb;

import java.util.*;
import net.quasardb.qdb.jni.*;

/**
 * Represents a tag in a quasardb database.
 */
public final class QdbTag extends QdbEntry {
    protected QdbTag(QdbSession session, String alias) {
        super(session, alias);
    }

    /**
     * Assigns the tag to an entry. The tag is created if it does not exist.
     *
     * @param entry The entry to tag.
     * @return true if the tag has been set, false if it was already set
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean addEntry(QdbEntry entry) {
        return addEntry(entry.alias());
    }

    /**
     * Assigns the tag to an entry. The tag is created if it does not exist.
     *
     * @param entry The alias of the entry to tag.
     * @return true if the tag has been set, false if it was already set
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean addEntry(String entry) {
        session.throwIfClosed();
        qdb_error_t err = qdb.add_tag(session.handle(), entry, alias);
        QdbExceptionFactory.throwIfError(err);
        return err != qdb_error_t.error_tag_already_set;
    }

    /**
     * Gets the entries tagged with this tag.
     *
     * @return A collection of subclasses of QdbEntry, whose types depends on the actual type of the entries in the database.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     */
    public Iterable<QdbEntry> entries() {
        return new QdbTagEntries(session, alias);
    }

    /**
     * Removes the tag assignment from an entry.
     *
     * @param entry The entry to untag.
     * @return true if the tag has been removed, false if the tag was not set
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean removeEntry(QdbEntry entry) {
        return removeEntry(entry.alias());
    }

    /**
     * Removes the tag assignment from an entry.
     *
     * @param entry The alias of the entry to untag.
     * @return true if the tag has been removed, false if the tag was not set
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean removeEntry(String entry) {
        session.throwIfClosed();
        qdb_error_t err = qdb.remove_tag(session.handle(), entry, alias);
        QdbExceptionFactory.throwIfError(err);
        return err != qdb_error_t.error_tag_not_set;
    }
}
