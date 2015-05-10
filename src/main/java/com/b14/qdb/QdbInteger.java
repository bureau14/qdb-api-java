package com.b14.qdb;

import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import com.b14.qdb.jni.SWIGTYPE_p_qdb_session;
import com.b14.qdb.jni.error_carrier;
import com.b14.qdb.jni.qdb;
import com.b14.qdb.jni.qdb_error_t;

/**
 * Represents an signed 64-bit integer in a quasardb database.
 * 
 * @author &copy; <a href="https://www.quasardb.net">quasardb</a> - 2015
 * @version master
 * @since 2.0.0
 */
public class QdbInteger extends Number {
    private static final long serialVersionUID = 7669517081331110295L;
    
    private final transient SWIGTYPE_p_qdb_session session;
    private final String alias;
    private volatile long value;
    
    /**
     * Build a QdbInteger with an initial value and store it into quasardb cluster as provided alias.
     * 
     * @param session quasardb session handler.
     * @param alias alias (i.e. its "key") of the integer in the database.
     * @throws QdbException 
     */
    protected QdbInteger(final SWIGTYPE_p_qdb_session session, final String alias) throws QdbException {
    	this.session = session;
        this.alias = alias;
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
    public final void put(final int initialValue) throws QdbException {
    	this.put(initialValue, 0L);
    }
    
    /**
     * 
     * @param initialValue
     * @param expiryTimeInSeconds
     * @throws QdbException
     */
    public final void put(final int initialValue, final long expiryTimeInSeconds) throws QdbException {
    	final qdb_error_t qdbError = qdb.int_put(session, alias, initialValue, (expiryTimeInSeconds == 0L) ? expiryTimeInSeconds : (System.currentTimeMillis() / 1000) + expiryTimeInSeconds);
        if (qdbError != qdb_error_t.error_ok) {
            throw new QdbException(qdbError);
        } else {
        	this.value = initialValue;
        }
    }
    
    /**
     * Gets the alias (i.e. its "key") of the integer in the database.
     * 
     * @return The alias.
     */
    public final String alias() {
        return this.alias;
    }

    /**
     * Gets the current value.
     *
     * @return the current value
     * @throws QdbException 
     */
    public final int get() throws QdbException {
        final error_carrier error = new error_carrier();
        this.value = qdb.int_get(session, alias, error);
        if (error.getError() != qdb_error_t.error_ok) {
            throw new QdbException(error.getError());
        }
        return (int) this.value;
    }

    /**
     * Sets to the given value.
     *
     * @param newValue the new value
     * @throws QdbException 
     */
    public final void set(int newValue) throws QdbException {
        final qdb_error_t qdbError = qdb.int_update(session, alias, newValue, 0);
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
    public final int getAndSet(int newValue) throws QdbException {
        for (;;) {
            int current = get();
            if (qdb.int_update(session, alias, newValue, 0) == qdb_error_t.error_ok) {
                return current;
            }
        }
    }
    
    /**
     * Atomically increments by one the current value.
     *
     * @return the previous value
     * @throws QdbException 
     */
    public final int getAndIncrement() throws QdbException {
        final error_carrier error = new error_carrier();
        for (;;) {
            int current = get();
            qdb.int_add(session, alias, 1, error);
            if (error.getError() != qdb_error_t.error_ok) {
                throw new QdbException(error.getError());
            }
            return current;
        }
    }
    
    /**
     * Atomically adds the given value to the current value.
     *
     * @param delta the value to add
     * @return the previous value
     * @throws QdbException 
     */
    public final int getAndAdd(int delta) throws QdbException {
        final error_carrier error = new error_carrier();
        for (;;) {
            int current = get();
            qdb.int_add(session, alias, delta, error);
            if (error.getError() != qdb_error_t.error_ok) {
                throw new QdbException(error.getError());
            }
            return current;
        }
    }
    
    /**
     * Atomically increments by one the current value.
     *
     * @return the updated value
     * @throws QdbException 
     */
    public final int incrementAndGet() throws QdbException {
        final error_carrier error = new error_carrier();
        for (;;) {
            int current = get();
            int next = current + 1;
            qdb.int_add(session, alias, 1, error);
            if (error.getError() != qdb_error_t.error_ok) {
                throw new QdbException(error.getError());
            }
            return next;
        }
    }
    
    /**
     * Atomically adds the given value to the current value.
     *
     * @param delta the value to add
     * @return the updated value
     * @throws QdbException 
     */
    public final int addAndGet(int delta) throws QdbException {
        final error_carrier error = new error_carrier();
        for (;;) {
            int current = get();
            int next = current + delta;
            qdb.int_add(session, alias, delta, error);
            if (error.getError() != qdb_error_t.error_ok) {
                throw new QdbException(error.getError());
            }
            return next;
        }
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
        final qdb_error_t qdbError = qdb.expires_at(session, alias, cal.getTimeInMillis() / 1000);
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
        final qdb_error_t qdbError = qdb.expires_from_now(session, alias, expiryTimeInSeconds);
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
        final long result = qdb.get_expiry(session, alias, error);
        if (error.getError() != qdb_error_t.error_ok) {
            throw new QdbException(error.getError());
        }
        return result;
    }
    
    /**
     * Removes the integer from the database.
     * 
     * @return true if integer was successfully removed
     */
    public final boolean remove() {
        final qdb_error_t qdbError = qdb.remove(session, alias);
        return (qdbError == qdb_error_t.error_ok);
    }
    
    @Override
    public int intValue() {
        try {
            return get();
        } catch (QdbException e) {
            return 0;
        }
    }

    @Override
    public long longValue() {
        final error_carrier error = new error_carrier();
        return qdb.int_get(session, alias, error);
    }

    @Override
    public float floatValue() {
        final error_carrier error = new error_carrier();
        return (float) qdb.int_get(session, alias, error);
    }

    @Override
    public double doubleValue() {
        final error_carrier error = new error_carrier();
        return (double) qdb.int_get(session, alias, error);
    }

    @Override
    public String toString() {
        try {
            return Integer.toString(get());
        } catch (QdbException e) {
            return "0";
        }
    }
}
