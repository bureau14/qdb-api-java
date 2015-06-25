package net.quasardb.qdb;

import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import net.quasardb.qdb.jni.SWIGTYPE_p_qdb_session;
import net.quasardb.qdb.jni.error_carrier;
import net.quasardb.qdb.jni.qdb;
import net.quasardb.qdb.jni.qdb_error_t;

import net.quasardb.qdb.QdbExpirableEntry;

/**
 * Represents an signed 64-bit integer in a quasardb database.
 * 
 * @author &copy; <a href="https://www.quasardb.net">quasardb</a> - 2015
 * @version 2.0.0
 * @since 2.0.0
 */
public class QdbInteger extends QdbExpirableEntry {
    private static final long serialVersionUID = 7669517081331110295L;
        
    /**
     * Build a QdbInteger with an initial value and store it into quasardb cluster as provided alias.
     * 
     * @param session quasardb session handler.
     * @param alias alias (i.e. its "key") of the integer in the database.
     * @throws QdbException 
     */
    protected QdbInteger(final SWIGTYPE_p_qdb_session session, final String alias) throws QdbException {
    	super(session, alias);
    }

    /**
     * 
     * @throws QdbException
     */
    public final void put() throws QdbException {
    	this.put(0, 0L);
    }
    
    /**
     * 
     * @param initialValue
     * @throws QdbException
     */
    public final void put(final long initialValue) throws QdbException {
    	this.put(initialValue, 0L);
    }
    
    /**
     * 
     * @param initialValue
     * @param expiryTimeInSeconds
     * @throws QdbException
     */
    public final void put(final long initialValue, final long expiryTimeInSeconds) throws QdbException {
    	final qdb_error_t qdbError = qdb.int_put(session, getAlias(), initialValue, (expiryTimeInSeconds == 0L) ? expiryTimeInSeconds : (System.currentTimeMillis() / 1000) + expiryTimeInSeconds);
        if (qdbError != qdb_error_t.error_ok) {
            throw new QdbException(qdbError);
        } 
    }

    /**
     * Gets the current value.
     *
     * @return the current value
     * @throws QdbException 
     */
    public final long get() throws QdbException {
        final error_carrier error = new error_carrier();
        long value = qdb.int_get(session, getAlias(), error);
        if (error.getError() != qdb_error_t.error_ok) {
            throw new QdbException(error.getError());
        }
        return value;
    }

    /**
     * Sets to the given value.
     *
     * @param newValue the new value
     * @throws QdbException 
     */
    public final void set(long newValue) throws QdbException {
        final qdb_error_t qdbError = qdb.int_update(session, getAlias(), newValue, 0);
        if (qdbError != qdb_error_t.error_ok) {
            throw new QdbException(qdbError);
        }
    }
    
    /**
     * Atomically sets to the given value and returns the old value.
     *
     * @param newValue the new value
     * @return the previous value
     * @throws QdbException 
     */
    public final long getAndSet(long newValue) throws QdbException {
        for (;;) {
            long current = get();
            if (qdb.int_update(session, getAlias(), newValue, 0) == qdb_error_t.error_ok) {
                return current;
            }
        }
    }
        
    /**
     * Atomically adds the given value to the current value.
     *
     * @param delta the value to add
     * @return the resutling value
     * @throws QdbException 
     */
    public final long add(long delta) throws QdbException {
        final error_carrier error = new error_carrier();
        long res = qdb.int_add(session, getAlias(), delta, error);
        if (error.getError() != qdb_error_t.error_ok) {
                throw new QdbException(error.getError());
        }
        return res;
    }
    


}
