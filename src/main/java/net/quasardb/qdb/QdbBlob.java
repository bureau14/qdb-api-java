package net.quasardb.qdb;

import java.nio.ByteBuffer;

import net.quasardb.qdb.jni.SWIGTYPE_p_qdb_session;
import net.quasardb.qdb.jni.error_carrier;
import net.quasardb.qdb.jni.qdb;
import net.quasardb.qdb.jni.qdb_error_t;

import net.quasardb.qdb.QdbExpirableEntry;

/**
 * Represents a blob in a quasardb database. Blob stands for Binary Large Object, meaning that you can store arbitrary data in this blob.
 * 
 * @author &copy; <a href="https://www.quasardb.net">quasardb</a> - 2015
 * @version 2.0.0
 * @since 2.0.0
 */
public class QdbBlob extends QdbExpirableEntry {
	private static final String EMPTY_BUFFER = "ByteBuffer shouldn't be null or empty.";
    
    /**
     * Create an empty Blob associated with given alias.
     * 
     * @param session
     * @param alias
     */
    protected QdbBlob(final SWIGTYPE_p_qdb_session session, final String alias) {
        super(session, alias);
    }
    
    /**
     * Gets the current value.
     *
     * @return the current value
     * @throws QdbException 
     */
    public final ByteBuffer get() throws QdbException {
    	final error_carrier error = new error_carrier();
        final ByteBuffer value = qdb.get(session, getAlias(), error);
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
        final ByteBuffer value = qdb.get_and_remove(session, getAlias(), error);
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
    	final ByteBuffer value = qdb.get_and_update(session, getAlias(), content, content.limit(), (expiryTime == 0) ? 0 : (System.currentTimeMillis() / 1000) + expiryTime, error);
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
        this.put(content, 0);
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
        final qdb_error_t qdbError = qdb.put(session, getAlias(), content, content.limit(), (expiryTime == 0) ? 0 : (System.currentTimeMillis() / 1000) + expiryTime);
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
    	final error_carrier error = new error_carrier();
    	final ByteBuffer value = qdb.compare_and_swap(session, getAlias(), newContent, newContent.limit(), comparand, comparand.limit(), (expiryTime == 0) ? 0 : (System.currentTimeMillis() / 1000) + expiryTime, error);
        if (error.getError() != qdb_error_t.error_ok) {
            throw new QdbException(error.getError());
        }
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
        this.update(content, 0);
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
        final qdb_error_t qdbError = qdb.update(session, getAlias(), content, content.limit(), (expiryTime == 0) ? 0 : (System.currentTimeMillis() / 1000) + expiryTime);
        if (qdbError != qdb_error_t.error_ok) {
            throw new QdbException(qdbError);
        }
    }
        
    /**
     * Removes the blob if it's content matches comparand.
     * 
     * @param comparand A {@link ByteBuffer} representing the blob’s content to be compared to.
     * @return true if the blob was actually removed, false if not.
     * @throws QdbException If provided comparand is null or empty or alias doesn't exists.
     */
    public final boolean removeIf(final ByteBuffer comparand) throws QdbException {
    	final qdb_error_t qdbError = qdb.remove_if(session, getAlias(), comparand, comparand.limit());
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
}
