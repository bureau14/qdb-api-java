/**
 * Copyright (c) 2009-2013, Bureau 14 SARL
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
 *    * Neither the name of Bureau 14 nor the names of its contributors may
 *      be used to endorse or promote products derived from this software
 *      without specific prior written permission.
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
package com.b14.qdb.batch;

/**
 * A Quasardb batch operation is at least an operation type associate with an alias.<br> 
 * Some operation types need a value and/or a compare value.<br>
 * <br>
 * Operation types which doesn't need a value are :
 * <ul>
 * <li>GET</li>
 * <li>REMOVE</li>
 * <li>GET_REMOVE</li>
 * </ul>
 * 
 * Operation types which needs a value are :
 * <ul>
 * <li>PUT</li>
 * <li>UPDATE</li>
 * <li>GET_UPDATE</li>
 * <li>REMOVE_IF</li>
 * </ul>
 * 
 * Operation type which needs a compare value are :
 * <ul>
 * <li>CAS</li>
 * </ul>
 * 
 * Please note that :
 * <ul>
 * <li>one invalid entry in operations array can invalidate all submitted batch operations.
 * <br>For example : if one "PUT operation" is invalid (aka put with null value), all submitted operations will have an error.
 * </li>
 * <li>operation on reserved aliases are not allowed (see previous item)</li>
 * </ul>
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2014
 * @version master
 * @since 1.1.0
 */
public class Operation<V> {
    private TypeOperation type;
    private String alias;
    private V value;
    private V compareValue;

    public Operation() {
    }

    /**
     * Copy constructor : build a Quasardb batch operation with an operation type and an alias
     * 
     * @param type Operation type on alias. See {@link TypeOperation}
     * @param alias Alias associated with the given operation type
     */
    public Operation(TypeOperation type, String alias) {
        this.type = type;
        this.alias = alias;
    }
    
    /**
     * Copy constructor : build a Quasardb batch operation with an operation type, an alias and a value
     * 
     * @param type Operation type on alias. See {@link TypeOperation}
     * @param alias Alias associated with the given operation type
     * @param value Value associated with the given operation type and alias
     */
    public Operation(TypeOperation type, String alias, V value) {
        this.type = type;
        this.alias = alias;
        this.value = value;
    }
    
    /**
     * Copy constructor : build a Quasardb batch operation with an operation type, an alias, a value and a compare value
     * 
     * @param type Operation type on alias. See {@link TypeOperation}
     * @param entry alias associated with the given operation type
     * @param value Value associated with the given operation type and alias
     * @param compareValue Value to compare with the given value that is associated with the given operation type and alias. 
     */
    public Operation(TypeOperation type, String alias, V value, V compareValue) {
        this.type = type;
        this.alias = alias;
        this.value = value;
        this.compareValue = compareValue;
    }
    
    /**
     * Get operation's type
     * 
     * @return Operation type to associated with the current batch operation. See {@link TypeOperation}
     */
    public TypeOperation getType() {
        return type;
    }
    
    /**
     * Set operation's type
     * 
     * @param type Operation type to associated with the current batch operation. See {@link TypeOperation}
     */
    public void setType(TypeOperation type) {
        this.type = type;
    }
    
    /**
     * Get operation's alias
     * 
     * @return Alias associated with the current batch operation.
     */
    public String getAlias() {
        return alias;
    }

    /**
     * Set operation's alias
     * 
     * @param alias Alias associated with the current batch operation.
     */
    public void setAlias(String alias) {
        this.alias = alias;
    }

    /**
     * Get the value associated with the current batch operation.
     * 
     * @return Value associated with the current batch operation.
     */
    public V getValue() {
        return value;
    }

    /**
     * Set the value associated with the current batch operation.
     * 
     * @param value Value associated with the current batch operation.
     */
    public void setValue(V value) {
        this.value = value;
    }

    /**
     * Get the compare value associated with the current batch operation.
     * 
     * @return Compare value associated with the current batch operation.
     */
    public V getCompareValue() {
        return compareValue;
    }

    /**
     * Set the compare value associated with the current batch operation.
     * 
     * @param compareValue Compare value associated with the current batch operation.
     */
    public void setCompareValue(V compareValue) {
        this.compareValue = compareValue;
    }
    
}
