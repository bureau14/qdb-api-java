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
     * Assigns a tag to the entry. The tag is created if it does not exist.
     *
     * @param tag The tag to add.
     * @return true If the tag has been set, false if it was already set
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean addTag(QdbTag tag) {
        return addTag(tag.alias());
    }

    /**
     * Assigns a tag to the entry. The tag is created if it does not exist.
     *
     * @param tag The alias of the tag to add.
     * @return true If the tag has been set, false if it was already set
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean addTag(String tag) {
        qdb_error_t err = qdb.add_tag(session.handle(), alias, tag);
        QdbExceptionFactory.throwIfError(err);
        return err != qdb_error_t.error_tag_already_set;
    }

    /**
     * Retrieves the tags assigned to the entry.
     *
     * @return The tags assigned to the entry.
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     */
    public Iterable<QdbTag> getTags() {
        List<QdbTag> tags = new ArrayList();
        for (String alias : getTagsAlias())
            tags.add(new QdbTag(session, alias));
        return tags;
    }

    /**
     * Retrieves the tags assigned to the entry.
     *
     * @return The aliases of the tags assigned to the entry.
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     */
    public Iterable<String> getTagsAlias() {
        results_list res = qdb.get_tags(session.handle(), alias);
        QdbExceptionFactory.throwIfError(res.getError());
        return QdbJniApi.resultsToList(res.getResults());
    }

    /**
     * Determines if a given tag has been assigned to the entry.
     *
     * @param tag The tag to check
     * @return true if the entry has the provided tag, false otherwise
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean hasTag(QdbTag tag) {
        return hasTag(tag.alias());
    }

    /**
     * Determines if a given tag has been assigned to the entry.
     *
     * @param tag The alias to the tag to check.
     * @return true if the entry has the provided tag, false otherwise
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean hasTag(String tag) {
        qdb_error_t err = qdb.has_tag(session.handle(), alias, tag);
        QdbExceptionFactory.throwIfError(err);
        return err != qdb_error_t.error_tag_not_set;
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
     * Removes a tag assignment from the entry.
     *
     * @param tag The tag to remove.
     * @return true if the tag has been removed, false if the tag was not set
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean removeTag(QdbTag tag) {
        return removeTag(tag.alias());
    }

    /**
     * Removes a tag assignment from the entry.
     *
     * @param tag The alias of the tag to removed.
     * @return true if the tag has been removed, false if the tag was not set
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public boolean removeTag(String tag) {
        qdb_error_t err = qdb.remove_tag(session.handle(), alias, tag);
        QdbExceptionFactory.throwIfError(err);
        return err != qdb_error_t.error_tag_not_set;
    }
}
