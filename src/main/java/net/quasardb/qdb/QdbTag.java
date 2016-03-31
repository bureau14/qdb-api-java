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
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean addEntry(String entry) {
        qdb_error_t err = qdb.add_tag(session.handle(), entry, alias);
        QdbExceptionFactory.throwIfError(err);
        return err != qdb_error_t.error_tag_already_set;
    }

    /**
     * Retrieves the list of tags of the entry
     *
     * @return A collection of alias.
     */
    public Iterable<String> getEntriesAlias() {
        results_list res = qdb.get_tagged(session.handle(), alias);
        QdbExceptionFactory.throwIfError(res.getError());
        return QdbJniApi.resultsToList(res.getResults());
    }

    /**
     * Gets the entries tagged with this tag.
     *
     * @return A collection of subclasses of QdbEntry, whose types depends on the actual type of the entries in the database.
     */
    public Iterable<QdbEntry> getEntries() {
        ArrayList<QdbEntry> entries = new ArrayList<QdbEntry>();
        QdbEntryFactory factory = new QdbEntryFactory(session);
        for (String alias : getEntriesAlias()) {
            entries.add(factory.createEntry(alias));
        }
        return entries;
    }
}
