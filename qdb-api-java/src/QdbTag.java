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
     * Retrieves the list of tags of the entry
     *
     * @return A collection of alias.
     */
    public Iterable<String> getEntriesAlias() {
        results_list res = qdb.get_tagged(session.handle(), alias);
        QdbExceptionFactory.throwIfError(res.getError());
        return QdbNativeApi.resultsToList(res.getResults());
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
