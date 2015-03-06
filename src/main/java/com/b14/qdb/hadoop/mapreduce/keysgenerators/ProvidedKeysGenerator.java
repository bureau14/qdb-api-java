/**
 * Copyright (c) 2009-2015, quasardb SAS
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
 *    * Neither the name of quasardb nor the names of its contributors may
 *      be used to endorse or promote products derived from this software
 *      without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY QUASARDB AND CONTRIBUTORS ``AS IS'' AND ANY
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
package com.b14.qdb.hadoop.mapreduce.keysgenerators;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.b14.qdb.Quasardb;

/**
 * A keys generator that returns the list of provided keys. 
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @see IKeysGenerator
 * @version master
 * @since 1.3.0
 */
public class ProvidedKeysGenerator implements IKeysGenerator {
    private static final String ENTRY_SEPARATOR = ",";
    private Set<String> keys = null;
    private String initString = null;
    
    /**
     * No argument constructor for de-serialization
     */
    public ProvidedKeysGenerator() {
    }

    /**
     * Provide the keys directly (don't look up in quasardb)
     * 
     * @param keys the keys to M/R over
     */
    public ProvidedKeysGenerator(Collection<String> keys) {
        this.keys = new HashSet<String>(keys);
    }

    /**
     * {@inheritDoc}
     */
    public String getInitString() {
        return this.initString;
    }

    /**
     * {@inheritDoc}
     */
    public void init(String initString) {
        if (initString == null) {
            throw new IllegalArgumentException("initString cannot be null");
        }
        this.initString = initString;
        
        this.keys = new HashSet<String>();
        String[] providedKeys = initString.split(ProvidedKeysGenerator.ENTRY_SEPARATOR);
        if (!providedKeys[0].equals(initString)) {
            for (String providedKey : providedKeys) {
                keys.add(providedKey);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public Collection<String> getKeys(Quasardb qdb)  {
        if (keys == null) {
            throw new IllegalStateException("generator not initialized");
        }
        return new HashSet<String>(keys);
    }

    /**
     * {@inheritDoc}
     */
    @Override 
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((keys == null) ? 0 : keys.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override 
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof ProvidedKeysGenerator)) {
            return false;
        }
        ProvidedKeysGenerator other = (ProvidedKeysGenerator) obj;
        if (keys == null) {
            if (other.keys != null) {
                return false;
            }
        } else if (!keys.equals(other.keys)) {
            return false;
        }
        return true;
    }
}
