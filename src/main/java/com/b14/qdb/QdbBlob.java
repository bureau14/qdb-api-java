package com.b14.qdb;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import com.b14.qdb.jni.SWIGTYPE_p_qdb_session;
import com.b14.qdb.jni.error_carrier;
import com.b14.qdb.jni.qdb;
import com.b14.qdb.jni.qdb_error_t;

/**
 * Represents a blob in a quasardb database. Blob stands for Binary Large Object, meaning that you can store arbitrary data in this blob.
 * 
 * @author &copy; <a href="https://www.quasardb.net">quasardb</a> - 2015
 * @version master
 * @since 2.0.0
 */
public class QdbBlob {
	private static final String EMPTY_BUFFER = "ByteBuffer shouldn't be null or empty.";
	
    private final transient SWIGTYPE_p_qdb_session session;
    private final String alias;
    private long expiryTime;
    
    /**
     * Create an empty Blob associated with given alias.
     * 
     * @param session
     * @param alias
     */
    protected QdbBlob(final SWIGTYPE_p_qdb_session session, final String alias) {
        this.session = session;
        this.alias = alias;
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
    public final ByteBuffer get() throws QdbException {
    	final error_carrier error = new error_carrier();
        final ByteBuffer value = qdb.get(session, alias, error);
        if (error.getError() != qdb_error_t.error_ok) {
            throw new QdbException(error.getError());
        }
        return value;
    }
    
    /**
     * Atomically gets blob's content and removes it.
     * 
     * @return A {@link ByteBuffer} representing the blob’s content, before the remove.
     * @throws QdbException
     */
    public final ByteBuffer getAndRemove() throws QdbException {
    	final error_carrier error = new error_carrier();
        final ByteBuffer value = qdb.get_and_remove(session, alias, error);
        if (error.getError() != qdb_error_t.error_ok) {
            throw new QdbException(error.getError());
        }
        return value;
    }
    
    /**
     * Atomically gets and updates (in this order) the blob.
     * 
     * @param content A {@link ByteBuffer} representing the blob’s content to be set.
     * @return A {@link ByteBuffer} representing the blob’s content, before the update.
     * @throws QdbException
     */
    public final ByteBuffer getAndUpdate(final ByteBuffer content) throws QdbException {
    	return this.getAndUpdate(content, 0L);
    }
    
    /**
     * Atomically gets and updates (in this order) the blob.
     * 
     * @param content A {@link ByteBuffer} representing the blob’s content to be set.
     * @param expiryTime The absolute expiry time of the blob, in seconds, relative to epoch.
     * @return A {@link ByteBuffer} representing the blob’s content, before the update.
     * @throws QdbException
     */
    public final ByteBuffer getAndUpdate(final ByteBuffer content, final long expiryTime) throws QdbException {
    	final error_carrier error = new error_carrier();
    	final ByteBuffer value = qdb.get_and_update(session, alias, content, content.limit(), (expiryTime == 0) ? 0 : (System.currentTimeMillis() / 1000) + expiryTime, error);
    	if (error.getError() != qdb_error_t.error_ok) {
            throw new QdbException(error.getError());
        }
        return value;
    }
    
    /**
     * Sets blob's content but fails if the blob already exists.<br>
     * Alias beginning with "qdb" are reserved and cannot be used.
     * 
     * @param content A {@link ByteBuffer} representing the blob’s content to be set.
     * @throws QdbException
     * @see QdbBlob#update(ByteBuffer)
     */
    public final void put(ByteBuffer content) throws QdbException {
        this.put(content, this.expiryTime);
    }
    
    /**
     * Sets blob's content but fails if the blob already exists.<br>
     * Alias beginning with "qdb" are reserved and cannot be used.
     * 
     * @param content A {@link ByteBuffer} representing the blob’s content to be set.
     * @param expiryTime The absolute expiry time of the blob, in seconds, relative to epoch.
     * @throws QdbException If the blob already exists.
     * @see QdbBlob#update(ByteBuffer, long)
     */
    public final void put(final ByteBuffer content, final long expiryTime) throws QdbException {
        final qdb_error_t qdbError = qdb.put(session, alias, content, content.limit(), (expiryTime == 0) ? 0 : (System.currentTimeMillis() / 1000) + expiryTime);
        if (qdbError != qdb_error_t.error_ok) {
            throw new QdbException(qdbError);
        }
    }
    
    /**
     * Atomically compares the blob's content with comparand and updates it to newContent if, and only if, they match.
     * 
     * @param newContent A {@link ByteBuffer} representing the blob’s content to be updated in case of match.
     * @param comparand A {@link ByteBuffer} representing the blob’s content to be compared to.
     * @return Always returns the original value of the blob.
     * @throws QdbException If the blob does not exist.
     */
    public final ByteBuffer compareAndSwap(final ByteBuffer newContent, final ByteBuffer comparand) throws QdbException {
    	return this.compareAndSwap(newContent, comparand, 0L);
    }
    
    /**
     * Atomically compares the blob's content with comparand and updates it to newContent if, and only if, they match.
     * 
     * @param newContent A {@link ByteBuffer} representing the blob’s content to be updated in case of match.
     * @param comparand A {@link ByteBuffer} representing the blob’s content to be compared to.
     * @param expiryTime The absolute expiry time of the blob, in seconds, relative to epoch.
     * @return Always returns the original value of the blob.
     * @throws QdbException If the blob does not exist.
     */
    public final ByteBuffer compareAndSwap(final ByteBuffer newContent, final ByteBuffer comparand, final long expiryTime) throws QdbException {
    	this.checkByteBuffer(newContent);
    	this.checkByteBuffer(comparand);
    	final error_carrier error = new error_carrier();
    	final ByteBuffer value = qdb.compare_and_swap(session, alias, newContent, newContent.limit(), comparand, comparand.limit(), (expiryTime == 0) ? 0 : (System.currentTimeMillis() / 1000) + expiryTime, error);
    	return value;
    }
    
    /**
     * Updates the content of the blob.<br>
     * Alias beginning with "qdb" are reserved and cannot be used. 
     * 
     * @param content A {@link ByteBuffer} representing the blob’s content to be updated.
     * @throws QdbException If provided content is null or empty.
     * @see QdbBlob#put(ByteBuffer)
     */
    public final void update(final ByteBuffer content) throws QdbException {
        this.update(content, this.expiryTime);
    }
    
    /**
     * Updates the content of the blob.<br>
     * Alias beginning with "qdb" are reserved and cannot be used. 
     * 
     * @param content A {@link ByteBuffer} representing the blob’s content to be updated.
     * @param expiryTime The absolute expiry time of the blob, in seconds, relative to epoch.
     * @throws QdbException If provided content is null or empty.
     * @see QdbBlob#put(ByteBuffer, long)
     */
    public final void update(final ByteBuffer content, final long expiryTime) throws QdbException {
    	this.checkByteBuffer(content);
        final qdb_error_t qdbError = qdb.update(session, alias, content, content.limit(), (expiryTime == 0) ? 0 : (System.currentTimeMillis() / 1000) + expiryTime);
        if (qdbError != qdb_error_t.error_ok) {
            throw new QdbException(qdbError);
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
     * @return true if integer was successfully removed, false if not.
     */
    public final boolean remove() {
        final qdb_error_t qdbError = qdb.remove(session, alias);
        return (qdbError == qdb_error_t.error_ok);
    }
    
    /**
     * Removes the blob if it's content matches comparand.
     * 
     * @param comparand A {@link ByteBuffer} representing the blob’s content to be compared to.
     * @return true if the blob was actually removed, false if not.
     * @throws QdbException If provided comparand is null or empty or alias doesn't exists.
     */
    public final boolean removeIf(final ByteBuffer comparand) throws QdbException {
    	this.checkByteBuffer(comparand);
    	final qdb_error_t qdbError = qdb.remove_if(session, alias, comparand, comparand.limit());
    	if (qdbError != qdb_error_t.error_ok) {
    		if (qdbError == qdb_error_t.error_unmatched_content) {
    			return false;
    		} else {
    			throw new QdbException(qdbError);
    		}
        } else {
        	return true;
        }
    }
    
    /**
     * Check if provided buffer is null or empty.
     * 
     * @param buffer {@link ByteBuffer} to check.
     * @throws QdbException If provided buffer is null or empty.
     */
    private final void checkByteBuffer(final ByteBuffer buffer) throws QdbException {
    	if ((buffer == null) || (buffer.limit() == 0)) {
    		throw new QdbException(EMPTY_BUFFER);
    	}
    }
}
