package net.quasardb.qdb;

import java.nio.ByteBuffer;
import net.quasardb.qdb.jni.*;

/**
 * A deque in the database.
 * Deque stands for "double-ended queue", you can both enqueue and dequeue from the front and the back.
 */
public final class QdbDeque extends QdbEntry {
    // Protected constructor. Call QdbCluster.deque() to get an instance.
    protected QdbDeque(QdbSession session, String alias) {
        super(session, alias);
    }

    /**
     * Retrieves the item at the back of the queue. The queue must already exist.
     *
     * @return The content of the item, or null if the deque was empty.
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public QdbBuffer back() {
        session.throwIfClosed();
        error_carrier error = new error_carrier();
        ByteBuffer value = qdb.deque_back(session.handle(), alias, error);
        if (error.getError() == qdb_error_t.error_container_empty)
            return null;
        QdbExceptionFactory.throwIfError(error);
        return new QdbBuffer(session, value);
    }

    /**
     * Retrieves the value of the queue at the specified index. The queue must already exist.
     *
     * @param index The zero-based index you wish to retrieve.
     * @return The content of the item.
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbOutOfBoundsException If the index is negative, or greater or equal than deque size.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public QdbBuffer get(long index) {
        session.throwIfClosed();
        error_carrier error = new error_carrier();
        ByteBuffer value = qdb.deque_get_at(session.handle(), alias, index, error);
        QdbExceptionFactory.throwIfError(error);
        return new QdbBuffer(session, value);
    }

    /**
     * Retrieves the item at the front of the queue. The queue must already exist.
     *
     * @return The content of the item, or null if the deque was empty.
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public QdbBuffer front() {
        session.throwIfClosed();
        error_carrier error = new error_carrier();
        ByteBuffer value = qdb.deque_front(session.handle(), alias, error);
        if (error.getError() == qdb_error_t.error_container_empty)
            return null;
        QdbExceptionFactory.throwIfError(error);
        return new QdbBuffer(session, value);
    }

    /**
     * Removes and retrieves the item at the back of the queue. The queue must already exist.
     *
     * @return The content of the removed item, or null if the deque was empty.
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public QdbBuffer popBack() {
        session.throwIfClosed();
        error_carrier error = new error_carrier();
        ByteBuffer value = qdb.deque_pop_back(session.handle(), alias, error);
        if (error.getError() == qdb_error_t.error_container_empty)
            return null;
        QdbExceptionFactory.throwIfError(error);
        return new QdbBuffer(session, value);
    }

    /**
     * Removes and retrieves the item at the front of the queue. The queue must already exist.
     *
     * @return The content of the removed item, or null if the deque was empty.
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public QdbBuffer popFront() {
        session.throwIfClosed();
        error_carrier error = new error_carrier();
        ByteBuffer value = qdb.deque_pop_front(session.handle(), alias, error);
        if (error.getError() == qdb_error_t.error_container_empty)
            return null;
        QdbExceptionFactory.throwIfError(error);
        return new QdbBuffer(session, value);
    }

    /**
     * Inserts the content at the back of the queue. Creates the queue if it does not exist.
     *
     * @param content The content that will be added to the queue.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public void pushBack(ByteBuffer content) {
        session.throwIfClosed();
        qdb_error_t err = qdb.deque_push_back(session.handle(), alias, content, content.limit());
        QdbExceptionFactory.throwIfError(err);
    }

    /**
     * Inserts the content at the front of the queue. Creates the queue if it does not exist.
     *
     * @param content The content that will be added to the queue.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public void pushFront(ByteBuffer content) {
        session.throwIfClosed();
        qdb_error_t err = qdb.deque_push_front(session.handle(), alias, content, content.limit());
        QdbExceptionFactory.throwIfError(err);
    }

    /**
     * Retrieves the size of the queue. The queue must already exist.
     *
     * @return The number of items in the deque.
     * @throws QdbAliasNotFoundException If an entry matching the provided alias cannot be found.
     * @throws QdbClusterClosedException If QdbCluster.close() has been called.
     * @throws QdbIncompatibleTypeException If the alias has a type incompatible for this operation.
     * @throws QdbReservedAliasException If the alias name or prefix is reserved for quasardb internal use.
     */
    public long size() {
        session.throwIfClosed();
        error_carrier error = new error_carrier();
        long value = qdb.deque_size(session.handle(), alias, error);
        QdbExceptionFactory.throwIfError(error);
        return value;
    }
}
