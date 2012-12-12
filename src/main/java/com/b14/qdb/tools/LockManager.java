/**
 * Copyright (c) 2009-2011, Bureau 14 SARL
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *    * Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in the
 *      documentation and/or other materials provided with the distribution.
 *    * Neither the name of the University of California, Berkeley nor the
 *      names of its contributors may be used to endorse or promote products
 *      derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY BUREAU 14 AND CONTRIBUTORS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE REGENTS AND CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.b14.qdb.tools;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Simple lock management
 * 
 * @deprecated
 * @param <K> the type of the object to be locked
 * @version qdb 0.6.0
 * @since qdb 0.6.0
 */
public class LockManager<K> {
    private final ConcurrentHashMap<K, ReentrantLock> locks = new ConcurrentHashMap<K, ReentrantLock>();
    private final LockFactory lockFactory = new LockFactory();

    /**
     * Lock the object
     * 
     * @param key the key
     */
    public void lock(K key) {
        ReentrantLock lock = lockFactory.getLock();

        while (true) {
            ReentrantLock oldLock = locks.putIfAbsent(key, lock);
            if (oldLock == null) {
                return;
            }
            // there was a lock
            oldLock.lock();
            // now we have it. Because of possibility that someone had it for remove, we don't re-use directly
            lockFactory.release(oldLock);
        }
    }

    /**
     * Unlock the object
     * 
     * @param key the object
     */
    public void unLock(K key) {
        ReentrantLock lock = locks.remove(key);
        lockFactory.release(lock);
    }

    /**
     * Factory/pool
     */
    public static class LockFactory {
        private static final int CAPACITY = 100;
        private static final ArrayList<ReentrantLock> LOCKS = new ArrayList<ReentrantLock>(CAPACITY);

        private ReentrantLock getLock() {
            ReentrantLock qLock = null;
            synchronized (LOCKS) {
                if (!LOCKS.isEmpty()) {
                    qLock = LOCKS.remove(0);
                }
            }
            ReentrantLock lock = qLock != null ? qLock : new ReentrantLock();
            lock.lock();
            return lock;
        }

        private void release(ReentrantLock lock) {
            lock.unlock();
            synchronized (LOCKS) {
                if (LOCKS.size() <= CAPACITY) {
                    LOCKS.add(lock);
                }
            }
        }
    }
}
