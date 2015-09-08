package net.quasardb.qdb;

import java.util.List;
import java.util.Vector;

import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import net.quasardb.qdb.jni.SWIGTYPE_p_qdb_session;
import net.quasardb.qdb.jni.error_carrier;
import net.quasardb.qdb.jni.qdb;
import net.quasardb.qdb.jni.qdb_error_t;
import net.quasardb.qdb.jni.results_list;
import net.quasardb.qdb.jni.StringVec;

import net.quasardb.qdb.QdbEntry;

public class QdbExpirableEntry extends QdbEntry  {

    protected QdbExpirableEntry(final SWIGTYPE_p_qdb_session session, final String alias) {
        super(session, alias);
    }

    /**
     * Sets the expiry time of an existing entry.
     * 
     * @param expiryDate absolute time after which the entry expires
     * @throws QdbException if the entry does not exist or if the expiry time is in the past
     */
    public void expiresAt(final Date expiryDate) throws QdbException {
        final GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(expiryDate);
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        final qdb_error_t qdbError = qdb.expires_at(session, getAlias(), cal.getTimeInMillis() / 1000);
        if (qdbError != qdb_error_t.error_ok) {
            throw new QdbException(qdbError);
        }
    }
    
    /**
     * Sets the expiry time of an existing entry.<br>
     * An expiry time of zero means the entry expires as soon as possible.
     * 
     * @param expiryTimeInSeconds time in seconds, relative to the call time, after which the entry expires.
     * @throws QdbException if the entry does not exist or if the expiry time is in the past
     */
    public void expiresFromNow(final long expiryTimeInSeconds) throws QdbException {
        final qdb_error_t qdbError = qdb.expires_from_now(session, getAlias(), expiryTimeInSeconds);
        if (qdbError != qdb_error_t.error_ok) {
            throw new QdbException(qdbError);
        }
    }
    
    /**
     * Retrieves the expiry time of the blob. A value of zero means the blob never expires.
     * 
     * @return The absolute expiry time, in seconds since epoch.
     * @throws QdbException If the blob does not exist.
     */
    public long getExpiryTime() throws QdbException {
        final error_carrier error = new error_carrier();
        final long result = qdb.get_expiry(session, getAlias(), error);
        if (error.getError() != qdb_error_t.error_ok) {
            throw new QdbException(error.getError());
        }
        return result;
    }

}