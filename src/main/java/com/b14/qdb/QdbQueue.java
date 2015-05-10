package com.b14.qdb;

import java.nio.ByteBuffer;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.b14.qdb.jni.SWIGTYPE_p_qdb_session;
import com.b14.qdb.jni.error_carrier;
import com.b14.qdb.jni.qdb;
import com.b14.qdb.jni.qdb_error_t;

/**
 * Represents a queue of blob in the quasardb database.<br>
 * It's a double-ended queue, you can both enqueue and dequeue from the front and the back.
 * 
 * @see AbstractCollection
 * @see Deque
 * @since   2.0.0
 */
public class QdbQueue implements Deque<ByteBuffer> {
	private final transient SWIGTYPE_p_qdb_session session;
    private final String alias;
    
	/**
	 * Create an empty queue associated with provided alias.
	 * 
	 * @param session
	 * @param alias
	 * @since 2.0.0
	 */
	protected QdbQueue(SWIGTYPE_p_qdb_session session, String alias) {
		this.session = session;
		this.alias = alias;
	}

	/**
     * Gets the alias (i.e. its "key") of the integer in the database.
     * 
     * @return The alias.
     * @since 2.0.0
     */
    public final String alias() {
        return this.alias;
    }
	
    /**
     * Inserts the specified element at the front of this deque.
     *
     * @param e the element to add
     * @throws NullPointerException if the specified element is null
     * @since 2.0.0
     */
	public void addFirst(ByteBuffer e) {
		if (e == null) {
            throw new NullPointerException();
		}
		final qdb_error_t qdbError = qdb.queue_push_front(session, alias, e, e.limit());
		if (qdbError != qdb_error_t.error_ok) {
            throw new NullPointerException(qdb.make_error_string(qdbError));
        }
	}

	/**
     * Inserts the specified element at the end of this deque.
     *
     * <p>This method is equivalent to {@link #add}.
     *
     * @param e the element to add
     * @throws NullPointerException if the specified element is null
     * @since 2.0.0
     */
	public void addLast(ByteBuffer e) {
		if (e == null) {
            throw new NullPointerException();
		}
		final qdb_error_t qdbError = qdb.queue_push_back(session, alias, e, e.limit());
		if (qdbError != qdb_error_t.error_ok) {
            throw new NullPointerException(qdb.make_error_string(qdbError));
        }
	}

	/**
     * Inserts the specified element at the front of this deque.
     *
     * @param e the element to add
     * @return <tt>true</tt> (as specified by {@link Deque#offerFirst})
     * @throws NullPointerException if the specified element is null
     * @since 2.0.0
     */
	public boolean offerFirst(ByteBuffer e) {
		addFirst(e);
        return true;
	}

	/**
     * Inserts the specified element at the end of this deque.
     *
     * @param e the element to add
     * @return <tt>true</tt> (as specified by {@link Deque#offerLast})
     * @throws NullPointerException if the specified element is null
     * @since 2.0.0
     */
	public boolean offerLast(ByteBuffer e) {
		addLast(e);
        return true;
	}

	/**
     * @throws NoSuchElementException {@inheritDoc}
     * @since 2.0.0
     */
	public ByteBuffer removeFirst() {
		ByteBuffer x = pollFirst();
        if (x == null)
            throw new NoSuchElementException();
        return x;
	}

	/**
     * @throws NoSuchElementException {@inheritDoc}
     * @since 2.0.0
     */
	public ByteBuffer removeLast() {
		ByteBuffer x = pollLast();
        if (x == null)
            throw new NoSuchElementException();
        return x;
	}

	/**
	 * {@inheritDoc}
     * @since 2.0.0
	 */
	public ByteBuffer pollFirst() {
		final error_carrier error = new error_carrier();
		final ByteBuffer result = qdb.queue_pop_front(session, alias, error);
        return result; // return null when empty
	}

	/**
	 * {@inheritDoc}
     * @since 2.0.0
	 */
	public ByteBuffer pollLast() {
		final error_carrier error = new error_carrier();
		final ByteBuffer result = qdb.queue_pop_back(session, alias, error);
		return result; // return null when empty
	}

	/**
     * @throws NoSuchElementException {@inheritDoc}
     * @since 2.0.0
     */
	public ByteBuffer getFirst() {
		ByteBuffer x = peekFirst();
        if (x == null)
            throw new NoSuchElementException();
        return x;
	}

	/**
     * @throws NoSuchElementException {@inheritDoc}
     * @since 2.0.0
     */
	public ByteBuffer getLast() {
		ByteBuffer x = peekLast();
        if (x == null)
            throw new NoSuchElementException();
        return x;
	}

	/**
	 * {@inheritDoc}
     * @since 2.0.0
	 */
	public ByteBuffer peekFirst() {
		final error_carrier error = new error_carrier();
		final ByteBuffer result = qdb.queue_front(session, alias, error);
        return result; // return null when empty
	}

	/**
	 * {@inheritDoc}
     * @since 2.0.0
	 */
	public ByteBuffer peekLast() {
		final error_carrier error = new error_carrier();
		final ByteBuffer result = qdb.queue_back(session, alias, error);
		return result; // return null when empty
	}

	/**
	 * {@inheritDoc}
     * @since 2.0.0
	 */
	public boolean removeFirstOccurrence(Object o) {
		throw new UnsupportedOperationException();
	}

	/**
	 * {@inheritDoc}
     * @since 2.0.0
	 */
	public boolean removeLastOccurrence(Object o) {
		throw new UnsupportedOperationException();
	}

	/**
     * Inserts the specified element at the end of this deque.
     *
     * <p>This method is equivalent to {@link #offerLast}.
     *
     * @param e the element to add
     * @return true (as specified by {@link java.util.Queue#offer})
     * @throws NullPointerException if the specified element is null
     */
	public boolean offer(ByteBuffer e) {
		return offerLast(e);
	}

	/**
     * Retrieves and removes the head of the queue represented by this deque.
     *
     * This method differs from {@link #poll poll} only in that it throws an
     * exception if this deque is empty.
     *
     * <p>This method is equivalent to {@link #removeFirst}.
     *
     * @return the head of the queue represented by this deque
     * @throws NoSuchElementException {@inheritDoc}
     */
    public ByteBuffer remove() {
        return removeFirst();
    }

    /**
     * Retrieves and removes the head of the queue represented by this deque
     * (in other words, the first element of this deque), or returns
     * <tt>null</tt> if this deque is empty.
     *
     * <p>This method is equivalent to {@link #pollFirst}.
     *
     * @return the head of the queue represented by this deque, or
     *         <tt>null</tt> if this deque is empty
     */
    public ByteBuffer poll() {
        return pollFirst();
    }

    /**
     * Retrieves, but does not remove, the head of the queue represented by
     * this deque.  This method differs from {@link #peek peek} only in
     * that it throws an exception if this deque is empty.
     *
     * <p>This method is equivalent to {@link #getFirst}.
     *
     * @return the head of the queue represented by this deque
     * @throws NoSuchElementException {@inheritDoc}
     */
    public ByteBuffer element() {
        return getFirst();
    }

    /**
     * Retrieves, but does not remove, the head of the queue represented by
     * this deque, or returns <tt>null</tt> if this deque is empty.
     *
     * <p>This method is equivalent to {@link #peekFirst}.
     *
     * @return the head of the queue represented by this deque, or
     *         <tt>null</tt> if this deque is empty
     */
    public ByteBuffer peek() {
        return peekFirst();
    }

    /**
     * Pushes an element onto the stack represented by this deque.  In other
     * words, inserts the element at the front of this deque.
     *
     * <p>This method is equivalent to {@link #addFirst}.
     *
     * @param e the element to push
     * @throws NullPointerException if the specified element is null
     */
    public void push(ByteBuffer e) {
        addFirst(e);
    }

    /**
     * Pops an element from the stack represented by this deque.  In other
     * words, removes and returns the first element of this deque.
     *
     * <p>This method is equivalent to {@link #removeFirst()}.
     *
     * @return the element at the front of this deque (which is the top
     *         of the stack represented by this deque)
     * @throws NoSuchElementException {@inheritDoc}
     */
    public ByteBuffer pop() {
        return removeFirst();
    }

    /**
	 * {@inheritDoc}
	 * @since 2.0.0
	 */
	public Iterator<ByteBuffer> descendingIterator() {
		return new DeqIterator(false);
	}

	/**
	 * {@inheritDoc}
	 * @since 2.0.0
	 */
	public boolean isEmpty() {
		return peek() == null;
	}

	/**
	 * {@inheritDoc}
	 * @since 2.0.0
	 */
	public Object[] toArray() {
		throw new UnsupportedOperationException();
	}

	/**
	 * {@inheritDoc}
	 * @since 2.0.0
	 */
	public <T> T[] toArray(T[] a) {
		throw new UnsupportedOperationException();
	}

	/**
	 * {@inheritDoc}
	 * @since 2.0.0
	 */
	public boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	/**
	 * {@inheritDoc}
	 * @since 2.0.0
	 */
	public boolean addAll(Collection<? extends ByteBuffer> c) {
		throw new UnsupportedOperationException();
	}

	/**
	 * {@inheritDoc}
	 * @since 2.0.0
	 */
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	/**
	 * {@inheritDoc}
	 * @since 2.0.0
	 */
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	/**
	 * {@inheritDoc}
	 * @since 2.0.0
	 */
	public void clear() {
		throw new UnsupportedOperationException();
	}

	/**
     * Inserts the specified element at the end of this deque.
     *
     * <p>This method is equivalent to {@link #addLast}.
     *
     * @param e the element to add
     * @return <tt>true</tt> (as specified by {@link Collection#add})
     * @throws NullPointerException if the specified element is null
     */
    public boolean add(ByteBuffer e) {
        addLast(e);
        return true;
    }

    /**
	 * {@inheritDoc}
	 * @since 2.0.0
	 */
	public boolean remove(Object o) {
		return removeFirstOccurrence(o);
	}

	/**
	 * {@inheritDoc}
	 * @since 2.0.0
	 */
	public boolean contains(Object o) {
		throw new UnsupportedOperationException();
	}

	/**
	 * {@inheritDoc}
	 * @since 2.0.0
	 */
	public int size() {
		throw new UnsupportedOperationException();
	}

	/**
	 * {@inheritDoc}
	 * @since 2.0.0
	 */
	public Iterator<ByteBuffer> iterator() {
		return new DeqIterator(true);
	}

	private class DeqIterator implements Iterator<ByteBuffer> {
		private final boolean ascending;
		private ByteBuffer nextEntry = null;
		private ByteBuffer lastEntry = null;

		private DeqIterator(boolean ascending) {
			this.ascending = ascending;
		}
		
		private final void fetch() throws QdbException {
			final error_carrier error = new error_carrier();
			if (ascending) {
				nextEntry = qdb.queue_pop_front(session, alias, error);
			} else {
				nextEntry = qdb.queue_pop_back(session, alias, error);
			}
		}
		
		/**
         * {@inheritDoc}
         */
		public boolean hasNext() {
			try {
                if (nextEntry == null) {
                    fetch();
                }
                return nextEntry != null;
            } catch (QdbException e) {
                return false;
            }
		}

		/**
         * {@inheritDoc}
         */
		public ByteBuffer next() {
			if (hasNext()) {
                // Remember the lastEntry
                lastEntry = nextEntry;

                // Reset nextEntry to force fetching the next available entry
                nextEntry = null;
                return lastEntry;
            } else {
                throw new NoSuchElementException();
            }
		}
		
		/**
         * {@inheritDoc}
         */
        public void remove() {
            throw new UnsupportedOperationException("Not yet implemented");
        }
	}
}
