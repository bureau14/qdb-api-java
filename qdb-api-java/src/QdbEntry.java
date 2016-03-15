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
    public String getAlias() {
        return alias;
    }

    /**
     * Removes the entry from the database.
     *
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public void remove() {
        qdb_error_t err = qdb.remove(session.handle(), alias);
        QdbExceptionFactory.throwIfError(err);
    }

    /**
     * Assigns a tag to an entry. The tag is created if it does not exist.
     *
     * @param tag The tag's name.
     * @return true If the tag has been set, false if it was already set
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean addTag(String tag) {
        qdb_error_t err = qdb.add_tag(session.handle(), alias, tag);

        if (err == qdb_error_t.error_tag_already_set)
            return false;
        QdbExceptionFactory.throwIfError(err);
        return true;
    }

    /**
     * Determines if a given tag has been assigned to an entry.
     *
     * @param tag The tag's name.
     * @return true if the entry has the provided tag, false otherwise
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean hasTag(String tag) {
        qdb_error_t err = qdb.has_tag(session.handle(), alias, tag);
        if (err == qdb_error_t.error_tag_not_set) {
            return false;
        }
        QdbExceptionFactory.throwIfError(err);
        return true;
    }

    /**
     * Removes a tag assignment from an entry.
     *
     * @param tag The tag's name.
     * @return true if the tag has been removed, false if the tag was not set
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean removeTag(String tag) {
        qdb_error_t err = qdb.remove_tag(session.handle(), alias, tag);
        if (err == qdb_error_t.error_tag_not_set) {
            return false;
        }
        QdbExceptionFactory.throwIfError(err);
        return true;
    }

    /**
     * Retrieves the tags assigned to the given alias.
     *
     * @return The list of tag.
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     */
    public List<String> getTags() {
        results_list res = qdb.get_tags(session.handle(), alias);
        QdbExceptionFactory.throwIfError(res.getError());

        return resultsToList(res.getResults());
    }

    static protected List<String> resultsToList(StringVec results) {
        int vecSize = (int)results.size();

        Vector<String> entries = new Vector<String>(vecSize, 2);

        for (int i = 0; i < vecSize; i++) {
            entries.add(results.get(i));
        }

        return entries;
    }
}
