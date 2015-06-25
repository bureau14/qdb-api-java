package net.quasardb.qdb;

import java.util.List;
import java.util.Vector;

import net.quasardb.qdb.jni.SWIGTYPE_p_qdb_session;
import net.quasardb.qdb.jni.error_carrier;
import net.quasardb.qdb.jni.qdb;
import net.quasardb.qdb.jni.qdb_error_t;
import net.quasardb.qdb.jni.results_list;
import net.quasardb.qdb.jni.StringVec;

public class QdbEntry {

    protected final transient SWIGTYPE_p_qdb_session session;
    private final String alias;

    /**
     * Create an empty entry associated with given alias.
     *
     * @param session
     * @param alias
     */
    public QdbEntry(final SWIGTYPE_p_qdb_session session, final String alias) {
        this.session = session;
        this.alias = alias;
    }

    /**
     * Gets the alias (i.e. its "key") of the integer in the database.
     *
     * @return The alias.
     */
    public final String getAlias() {
        return this.alias;
    }

    /**
     * @return The underlying session object
     */

    public final SWIGTYPE_p_qdb_session session() {
        return this.session;
    }

    /**
     * Removes the entry from the database.
     *
     * @throws QdbException
     */
    public final void remove() throws QdbException {
        final qdb_error_t err = qdb.remove(session, alias);
        if (err != qdb_error_t.error_ok) {
            throw new QdbException(err);
        }
    }

    /**
     * Adds a tag to the entry
     *
     * @return true if the tag has been set, false if it was already set
     * @throws QdbException
     */

    public final boolean addTag(String tag) throws QdbException {
        final qdb_error_t err = qdb.add_tag(this.session, this.alias, tag);
        if (err == qdb_error_t.error_tag_already_set) {
            return false;
        }
        if (err != qdb_error_t.error_ok) {
            throw new QdbException(err);
        }
        return true;
    }

    /**
     *
     * Tests if an entry has the provided tag
     *
     * @return true if the entry has the provided tag, false otherwise
     * @throws QdbException
     *
     */
    public final boolean hasTag(String tag) throws QdbException {
        final qdb_error_t err = qdb.has_tag(this.session, this.alias, tag);
        if (err == qdb_error_t.error_tag_not_set)
        {
            return false;
        }
        if (err != qdb_error_t.error_ok) {
            throw new QdbException(err);
        }
        return true;
    }

    /**
     *
     * Removes the provided tag
     *
     * @return true if the tag has been removed, false if the tag was not set
     * @throws QdbException
     *
     */
    public final boolean removeTag(String tag) throws QdbException {
        final qdb_error_t err = qdb.remove_tag(this.session, this.alias, tag);
        if (err == qdb_error_t.error_tag_not_set)
        {
            return false;
        }
        if (err != qdb_error_t.error_ok) {
            throw new QdbException(err);
        }
        return true;
    }

    static protected List<String> resultsToList(StringVec results)
    {
        int vecSize = (int)results.size();

        Vector<String> entries = new Vector<String>(vecSize, 2);

        for(int i = 0; i < vecSize; i++)
        {
            entries.add(results.get(i));
        }

        return entries;
    }

    /**
     *
     * Return the list of tags associated with the entry
     *
     * @throws QdbException
     */
    public List<String> getTags() throws QdbException {
        final results_list res = qdb.get_tags(this.session, this.alias);
        if (res.getError() != qdb_error_t.error_ok) {
            throw new QdbException(res.getError());
        }

        return resultsToList(res.getResults());
    }

}