package net.quasardb.qdb;

import java.nio.ByteBuffer;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;

import net.quasardb.qdb.jni.SWIGTYPE_p_qdb_session;
import net.quasardb.qdb.jni.error_carrier;
import net.quasardb.qdb.jni.qdb;
import net.quasardb.qdb.jni.qdb_error_t;

import net.quasardb.qdb.QdbEntry;

/**
 * Represents a deque of blob in the quasardb database.<br>
 * It's a double-ended deque, you can both endeque and dedeque from the front and the back.
 *
 * @see AbstractCollection
 * @see Deque
 * @since   2.0.0
 */
public class QdbDeque extends QdbEntry {

    /**
     * Create an empty deque associated with provided alias.
     *
     * @param session TODO
     * @param alias TODO
     * @since 2.0.0
     */
    protected QdbDeque(SWIGTYPE_p_qdb_session session, String alias) {
        super(session, alias);
    }

    /**
     * Inserts the specified element at the front of this deque.
     *
     * @param e the element to add
     * @throws QdbException TODO
     * @since 2.0.0
     */
    public void addFirst(ByteBuffer e) throws QdbException {
        if (e == null) {
            throw new NullPointerException();
        }
        final qdb_error_t qdbError = qdb.deque_push_front(session, getAlias(), e, e.limit());
        if (qdbError != qdb_error_t.error_ok) {
            throw new QdbException(qdbError);
        }
    }

    /**
     * Inserts the specified element at the end of this deque.
     *
     *
     * @param e the element to add
     * @throws NullPointerException if the specified element is null
     * @throws QdbException TODO
     * @since 2.0.0
     */
    public void addLast(ByteBuffer e) throws QdbException {
        final qdb_error_t qdbError = qdb.deque_push_back(session, getAlias(), e, e.limit());
        if (qdbError != qdb_error_t.error_ok) {
            throw new QdbException(qdbError);
        }
    }

    /**
     *
     * @since 2.0.0
     * @return TODO
     * @throws QdbException TODO
     */
    public ByteBuffer pollFirst() throws QdbException {
        final error_carrier error = new error_carrier();
        final ByteBuffer result = qdb.deque_pop_front(session, getAlias(), error);
        if (error.getError() != qdb_error_t.error_ok) {
            throw new QdbException(error.getError());
        }
        return result; // return null when empty
    }

    /**
     *
     * @since 2.0.0
     * @return TODO
     * @throws QdbException TODO
     */
    public ByteBuffer pollLast() throws QdbException  {
        final error_carrier error = new error_carrier();
        final ByteBuffer result = qdb.deque_pop_back(session, getAlias(), error);
        if (error.getError() != qdb_error_t.error_ok) {
            throw new QdbException(error.getError());
        }
        return result; // return null when empty
    }

    /**
     *
     * @since 2.0.0
     * @return TODO
     * @throws QdbException TODO
     */
    public ByteBuffer peekFirst() throws QdbException {
        final error_carrier error = new error_carrier();
        final ByteBuffer result = qdb.deque_front(session, getAlias(), error);
        if (error.getError() != qdb_error_t.error_ok) {
            throw new QdbException(error.getError());
        }
        return result; // return null when empty
    }

    /**
     *
     * @since 2.0.0
     * @return TODO
     * @throws QdbException TODO
     */
    public ByteBuffer peekLast() throws QdbException {
        final error_carrier error = new error_carrier();
        final ByteBuffer result = qdb.deque_back(session, getAlias(), error);
        if (error.getError() != qdb_error_t.error_ok) {
            throw new QdbException(error.getError());
        }
        return result; // return null when empty
    }

    /**
     *
     * @since 2.0.0
     * @return TODO
     * @throws QdbException TODO
     */
    public long size() throws QdbException {

        final error_carrier error = new error_carrier();
        final long result = qdb.deque_size(session, getAlias(), error);
        if (error.getError() != qdb_error_t.error_ok) {
            throw new QdbException(error.getError());
        }
        return result;
    }

    /**
     *
     * @param i TODO
     * @since 2.0.0
     * @return TODO
     * @throws QdbException TODO
     */
    public ByteBuffer get(long i) throws QdbException {
        final error_carrier error = new error_carrier();
        final ByteBuffer result = qdb.deque_get_at(session, getAlias(), i, error);
        if (error.getError() != qdb_error_t.error_ok) {
            throw new QdbException(error.getError());
        }
        return result; // return null when empty
    }

    /**
     *
     * @since 2.0.0
     * @return TODO
     * @throws QdbException TODO
     */
    public boolean isEmpty() throws QdbException {
        return size() == 0;
    }

}
