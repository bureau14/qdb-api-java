package net.quasardb.qdb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.*;
import net.quasardb.qdb.exception.*;
import net.quasardb.qdb.jni.*;

/**
 * An entry in the database.
 */
public class QdbEntry {
    protected final transient Session session;
    protected final String alias;

    protected QdbEntry(Session session, String alias) {
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
      * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
      * @throws ClusterClosedException If QdbCluster.close() has been called.
      * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public boolean attachTag(QdbTag tag) {
        return attachTag(tag.alias());
    }

    /**
      * Attaches a tag to the entry. The tag is created if it does not exist.
      *
      * @param tag The alias of the tag to attach.
      * @return true if the tag has been attached, false if it was already attached
      * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
      * @throws ClusterClosedException If QdbCluster.close() has been called.
      * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public boolean attachTag(String tag) {
        session.throwIfClosed();
        int err = qdb.attach_tag(session.handle(), alias, tag);
        ExceptionFactory.throwIfError(err);
        return err != qdb_error.tag_already_set;
    }

    /**
       * Checks if a QdbEntry points to the same entry in the database.
       *
       * @return true if entry type and alias are equals, false otherwise
       */
    @Override
    public boolean equals(Object obj) {
        return obj instanceof QdbEntry && equals((QdbEntry) obj);
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
       * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
       * @throws ClusterClosedException If QdbCluster.close() has been called.
       * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
       */
    public boolean hasTag(QdbTag tag) {
        return hasTag(tag.alias());
    }

    /**
      * Checks if a tag is attached to the entry.
      *
      * @param tag The alias to the tag to check.
      * @return true if the entry has the provided tag, false otherwise
      * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
      * @throws ClusterClosedException If QdbCluster.close() has been called.
      * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public boolean hasTag(String tag) {
        session.throwIfClosed();
        int err = qdb.has_tag(session.handle(), alias, tag);
        ExceptionFactory.throwIfError(err);
        return err != qdb_error.tag_not_set;
    }

    /**
      * Removes the entry from the database.
      *
      * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
      * @throws ClusterClosedException If QdbCluster.close() has been called.
      * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public void remove() {
        session.throwIfClosed();
        int err = qdb.remove(session.handle(), alias);
        ExceptionFactory.throwIfError(err);
    }

    /**
       * Detaches a tag from the entry.
       *
       * @param tag The tag to detach.
       * @return true if the tag has been detached, false if the tag was not attached
       * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
       * @throws ClusterClosedException If QdbCluster.close() has been called.
       * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
       */
    public boolean detachTag(QdbTag tag) {
        return detachTag(tag.alias());
    }

    /**
      * Detaches a tag from the entry.
      *
      * @param tag The alias of the tag to detach.
      * @return true if the tag has been detached, false if the tag was not attached
      * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
      * @throws ClusterClosedException If QdbCluster.close() has been called.
      * @throws ReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
      */
    public boolean detachTag(String tag) {
        session.throwIfClosed();
        int err = qdb.detach_tag(session.handle(), alias, tag);
        ExceptionFactory.throwIfError(err);
        return err != qdb_error.tag_not_set;
    }

    /**
    * Retrieves the tags attached to the entry.
    *
    * @return The tags attached to the entry.
    * @throws AliasNotFoundException If an entry matching the provided alias cannot be found.
    * @throws ClusterClosedException If QdbCluster.close() has been called.
    */
    public Iterable<QdbTag> tags() {
        return new QdbEntryTags(session, alias);
    }

    public QdbEntryMetadata metadata() {
        ByteBuffer meta = ByteBuffer.allocateDirect(80);

        int err = qdb.get_metadata(session.handle(), alias, meta);
        ExceptionFactory.throwIfError(err);

        meta.order(ByteOrder.LITTLE_ENDIAN);

        QdbId reference = new QdbId(meta.getLong(0), meta.getLong(8), meta.getLong(16), meta.getLong(24));
        long size = meta.getLong(40);
        Instant lastModification = Instant.ofEpochSecond(meta.getLong(48), meta.getLong(56));
        Instant expiry = Instant.ofEpochSecond(meta.getLong(64), meta.getLong(72));

        return new QdbEntryMetadata(reference, size, lastModification, expiry);
    }

    public QdbNode node() {
        session.throwIfClosed();
        Reference<String> address = new Reference<String>();
        Reference<Integer> port = new Reference<Integer>();
        int err = qdb.get_location(session.handle(), alias, address, port);
        ExceptionFactory.throwIfError(err);
        return new QdbNode(session, address.value, port.value);
    }
}
