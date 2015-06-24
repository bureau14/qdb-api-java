package com.b14.qdb;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.List;
import java.util.Vector;

import com.b14.qdb.jni.SWIGTYPE_p_qdb_session;
import com.b14.qdb.jni.error_carrier;
import com.b14.qdb.jni.qdb;
import com.b14.qdb.jni.qdb_error_t;
import com.b14.qdb.jni.results_list;
import com.b14.qdb.jni.StringVec;

import com.b14.qdb.QdbEntry;

/**
 * Represents a tag in a quasardb database. 
 * 
 * @author &copy; <a href="https://www.quasardb.net">quasardb</a> - 2015
 * @version 2.0.0
 * @since 2.0.0
 */
public class QdbTag extends QdbEntry {
    /**
     * Create an empty Blob associated with given alias.
     * 
     * @param session
     * @param alias
     */
    protected QdbTag(final SWIGTYPE_p_qdb_session session, final String alias) {
        super(session, alias);
    }

    /**
     * Retrieves the list of tags of the entry
     *
     * @return The entry's list of tags
     */
    public List<String> getEntries() throws QdbException {
        results_list res = qdb.get_tagged(this.session, this.getAlias());
        if (res.getError() != qdb_error_t.error_ok) {
            throw new QdbException(res.getError());
        }

        return resultsToList(res.getResults());
    }
    

}
