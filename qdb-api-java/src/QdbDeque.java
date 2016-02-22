package net.quasardb.qdb;

import java.nio.ByteBuffer;
import java.util.*;
import net.quasardb.qdb.jni.*;

/**
 * A deque in the database.
 * Deque stands for "double-ended queue", you can both endeque and dedeque from the front and the back.
 */
public final class QdbDeque extends QdbEntry {
    protected QdbDeque(SWIGTYPE_p_qdb_session session, String alias) {
        super(session, alias);
    }

    /**
     * Inserts the specified element at the front of this deque.
     *
     * @param content The content of the item to be added to the deque
     * @throws QdbException TODO
     */
    public void addFirst(ByteBuffer content) {
        if (content == null) {
            throw new NullPointerException();
        }
        final qdb_error_t err = qdb.deque_push_front(session, alias, content, content.limit());
        QdbExceptionThrower.throwIfError(err);
    }

    /**
     * Inserts the specified element at the end of this deque.
     *
     *
     * @param content The content of the item to be added to the deque
     * @throws NullPointerException if the specified element is null
     * @throws QdbException TODO
     */
    public void addLast(ByteBuffer content) {
        final qdb_error_t err = qdb.deque_push_back(session, alias, content, content.limit());
        QdbExceptionThrower.throwIfError(err);
    }

    /**
     *
     * @return TODO
     * @throws QdbException TODO
     */
    public ByteBuffer pollFirst() {
        final error_carrier error = new error_carrier();
        final ByteBuffer result = qdb.deque_pop_front(session, alias, error);
        QdbExceptionThrower.throwIfError(error.getError());
        return result; // return null when empty
    }

    /**
     *
     * @return TODO
     * @throws QdbException TODO
     */
    public ByteBuffer pollLast() {
        final error_carrier error = new error_carrier();
        final ByteBuffer result = qdb.deque_pop_back(session, alias, error);
        QdbExceptionThrower.throwIfError(error.getError());
        return result; // return null when empty
    }

    /**
     *
     * @return TODO
     * @throws QdbException TODO
     */
    public ByteBuffer peekFirst() {
        final error_carrier error = new error_carrier();
        final ByteBuffer result = qdb.deque_front(session, alias, error);
        QdbExceptionThrower.throwIfError(error.getError());
        return result; // return null when empty
    }

    /**
     *
     * @return TODO
     * @throws QdbException TODO
     */
    public ByteBuffer peekLast() {
        final error_carrier error = new error_carrier();
        final ByteBuffer result = qdb.deque_back(session, alias, error);
        QdbExceptionThrower.throwIfError(error.getError());
        return result; // return null when empty
    }

    /**
     *
     * @return TODO
     * @throws QdbException TODO
     */
    public long size() {

        final error_carrier error = new error_carrier();
        final long result = qdb.deque_size(session, alias, error);
        QdbExceptionThrower.throwIfError(error.getError());
        return result;
    }

    /**
     *
     * @param i TODO
     * @return TODO
     * @throws QdbException TODO
     */
    public ByteBuffer get(long i) {
        final error_carrier error = new error_carrier();
        final ByteBuffer result = qdb.deque_get_at(session, alias, i, error);
        QdbExceptionThrower.throwIfError(error.getError());
        return result; // return null when empty
    }

    /**
     *
     * @return TODO
     * @throws QdbException TODO
     */
    public boolean isEmpty() {
        return size() == 0;
    }
}
