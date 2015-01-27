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
package com.b14.qdb;

/**
 * A quasardb entry is a <i>&lt;alias ; value&gt;</i> pair.
 *
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2014
 * @version master
 * @since 1.0.0
 */
public class QuasardbEntry<V> {
    String alias;
    V value;
    
    public QuasardbEntry() {
    }
    
    /**
     * Copy constructor.
     * 
     * @param alias the alias which represent the quasardb entry
     * @param value the value associated to the alias
     */
    public QuasardbEntry(String alias, V value) {
        this.alias = alias;
        this.value = value;
    }
    
    /**
     * Get alias for the Quasardb entry 
     * 
     * @return alias for the entry
     */
    public String getAlias() {
        return alias;
    }
    
    /**
     * Set alias for the Quasardb entry  
     * 
     * @param alias
     */
    public void setAlias(String alias) {
        this.alias = alias;
    }
    
    /**
     * Get value for the Quasardb entry 
     * 
     * @return value for the Quasardb entry
     */
    public V getValue() {
        return value;
    }
    
    /**
     * Set value for the Quasardb entry 
     * 
     * @param value value for the Quasardb entry
     */
    public void setValue(V value) {
        this.value = value;
    }
}
