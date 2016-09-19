package net.quasardb.qdb;

import java.util.*;
import net.quasardb.qdb.jni.*;

/**
 * An entry in the database.
 */
public class QdbEntry {
    protected final transient QdbSession session;
    protected final String alias;

    protected QdbEntry(QdbSession session, String alias) {
        this.session = session;
        this.alias = alias;
    }

    /**
     * Gets the alias (i.e. its "key") of the entry in the database.
     *
     * @return The alias.
     */
    public String alias() {
        return alias;
    }

    /**
      * Attaches a tag to the entry. The tag is created if it does not exist.
      *
      * @param tag The tag to attach.
      * @return true if the tag has been attached, false if it was already attached
      * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
      * @throws QdbClusterClosedException If QdbCluster.close() has been called.
      * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public boolean attachTag(QdbTag tag) {
        return attachTag(tag.alias());
    }

    /**
      * Attaches a tag to the entry. The tag is created if it does not exist.
      *
      * @param tag The alias of the tag to attach.
      * @return true if the tag has been attached, false if it was already attached
      * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
      * @throws QdbClusterClosedException If QdbCluster.close() has been called.
      * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public boolean attachTag(String tag) {
        session.throwIfClosed();
        int err = qdb.attach_tag(session.handle(), alias, tag);
        QdbExceptionFactory.throwIfError(err);
        return err != qdb_error.tag_already_set;
    }

    /**
       * Checks if a QdbEntry points to the same entry in the database.
       *
       * @return true if entry type and alias are equals, false otherwise
       */
    @Override
    public boolean equals(Object obj) {
        return obj instanceof QdbEntry && equals((QdbEntry)obj);
    }

    /**
       * Checks if a QdbEntry points to the same entry in the database.
       *
       * @param entry An entry to compare to.
       * @return true if entry type and alias are equals, false otherwise.
       */
    public boolean equals(QdbEntry entry) {
        return entry != null && entry.getClass().equals(getClass()) && entry.alias.equals(alias);
    }

    /**
     * Gets alias hash code.
     *
     * @return A hash-code based on the entry alias.
     */
    @Override
    public int hashCode() {
        return alias.hashCode();
    }

    /**
       * Checks if a tag is attached to the entry.
       *
       * @param tag The tag to check
       * @return true if the entry has the provided tag, false otherwise
       * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
       * @throws QdbClusterClosedException If QdbCluster.close() has been called.
       * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
       */
    public boolean hasTag(QdbTag tag) {
        return hasTag(tag.alias());
    }

    /**
      * Checks if a tag is attached to the entry.
      *
      * @param tag The alias to the tag to check.
      * @return true if the entry has the provided tag, false otherwise
      * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
      * @throws QdbClusterClosedException If QdbCluster.close() has been called.
      * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public boolean hasTag(String tag) {
        session.throwIfClosed();
        int err = qdb.has_tag(session.handle(), alias, tag);
        QdbExceptionFactory.throwIfError(err);
        return err != qdb_error.tag_not_set;
    }

    /**
      * Removes the entry from the database.
      *
      * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
      * @throws QdbClusterClosedException If QdbCluster.close() has been called.
      * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public void remove() {
        session.throwIfClosed();
        int err = qdb.remove(session.handle(), alias);
        QdbExceptionFactory.throwIfError(err);
    }

    /**
       * Detaches a tag from the entry.
       *
       * @param tag The tag to detach.
       * @return true if the tag has been detached, false if the tag was not attached
       * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
       * @throws QdbClusterClosedException If QdbCluster.close() has been called.
       * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
       */
    public boolean detachTag(QdbTag tag) {
        return detachTag(tag.alias());
    }

    /**
      * Detaches a tag from the entry.
      *
      * @param tag The alias of the tag to detach.
      * @return true if the tag has been detached, false if the tag was not attached
      * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
      * @throws QdbClusterClosedException If QdbCluster.close() has been called.
      * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public boolean detachTag(String tag) {
        session.throwIfClosed();
        int err = qdb.detach_tag(session.handle(), alias, tag);
        QdbExceptionFactory.throwIfError(err);
        return err != qdb_error.tag_not_set;
    }

    /**
   * Retrieves the tags attached to the entry.
   *
   * @return The tags attached to the entry.
   * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
   * @throws QdbClusterClosedException If QdbCluster.close() has been called.
   */
    public Iterable<QdbTag> tags() {
        return new QdbEntryTags(session, alias);
    }
}
