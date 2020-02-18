package net.quasardb.qdb;

import java.util.*;
import net.quasardb.qdb.exception.*;
import net.quasardb.qdb.jni.*;

/**
 * Represents a tag in a quasardb database.
 */
public final class QdbTag extends QdbEntry {
    protected QdbTag(Session session, String alias) {
        super(session, alias);
    }

    /**
     * Attaches the tag to an entry. The tag is created if it does not exist.
     *
     * @param entry The entry to attach.
     * @return true if the tag has been attached, false if it was already attached
     * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean attachEntry(QdbEntry entry) {
        return attachEntry(entry.alias());
    }

    /**
     * Attaches the tag to an entry. The tag is created if it does not exist.
     *
     * @param entry The alias of the entry to attach.
     * @return true if the tag has been attached, false if it was already attached
     * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean attachEntry(String entry) {
        session.throwIfClosed();
        int err = qdb.attach_tag(session.handle(), entry, alias);
        return err != qdb_error.tag_already_set;
    }

    /**
     * Gets the entries tagged with this tag.
     *
     * @return A collection of subclasses of QdbEntry, whose types depends on the actual type of the entries in the database.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     */
    public Iterable<QdbEntry> entries() {
        return new QdbTagEntries(session, alias);
    }

    /**
     * Detaches the tag from an entry.
     *
     * @param entry The entry to detach.
     * @return true if the tag has been detached, false if the tag was not attached
     * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean detachEntry(QdbEntry entry) {
        return detachEntry(entry.alias());
    }

    /**
     * Detaches the tag from an entry.
     *
     * @param entry The alias of the entry to detach.
     * @return true if the tag has been detached, false if the tag was not attached
     * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws ClusterClosedException If QdbCluster.close() has been called.
     * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean detachEntry(String entry) {
        session.throwIfClosed();
        int err = qdb.detach_tag(session.handle(), entry, alias);
        return err != qdb_error.tag_not_set;
    }
}
