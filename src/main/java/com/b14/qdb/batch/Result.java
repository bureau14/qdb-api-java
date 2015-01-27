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
package com.b14.qdb.batch;

/**
 * A Quasardb batch operation result is :
 * <ul>
 * <li>A success status.</li>
 * <li>The alias which was associated with the current operation result.</li>
 * <li>The operation type which was associated with the current operation result.</li>
 * <li>A value which can be the result of the current operation.</li>
 * <li>An error which can be the result of the current operation.</li>
 * </ul>
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2014
 * @param <V> Object type associated with the current operation.
 * @version master
 * @since 1.1.0
 */
public class Result<V> {
    private boolean success;
    private String alias;
    private TypeOperation typeOperation;
    private V value;
    private String error;
    
    public Result() {
    }
    
    /**
     * Are this current operation successful ?
     * 
     * @return true if current operation is successful. false in all other cases.
     */
    public boolean isSuccess() {
        return success;
    }
    
    /**
     * Set if current operation is successful or not.
     * 
     * @param success Success status for current operation.
     */
    public void setSuccess(boolean success) {
        this.success = success;
    }
    
    /**
     * Get alias associated with the current operation.
     * 
     * @return alias associated with the current operation.
     */
    public String getAlias() {
        return alias;
    }
    
    /**
     * Set alias associated with the current operation.
     * 
     * @param alias Alias associated with the current operation.
     */
    public void setAlias(String alias) {
        this.alias = alias;
    }
    
    /**
     * Get operation type associated with the current operation.
     * 
     * @return Operation type associated with the current operation.
     */
    public TypeOperation getTypeOperation() {
        return typeOperation;
    }
    
    /**
     * Set operation type associated with the current operation.
     * 
     * @param typeOperation Operation type associated with the current operation.
     */
    public void setTypeOperation(TypeOperation typeOperation) {
        this.typeOperation = typeOperation;
    }
    
    /**
     * Get result value for the current operation.
     * 
     * @return Result value for the current operation.
     */
    public V getValue() {
        return value;
    }
    
    /**
     * Set result value for the current operation.
     * 
     * @param value Result value for the current operation.
     */
    public void setValue(V value) {
        this.value = value;
    }

    /**
     * Get result error for the current operation.
     * 
     * @return Result error for the current operation.
     */
    public String getError() {
        return error;
    }

    /**
     * Set result error for the current operation.
     * 
     * @param error Result error for the current operation.
     */
    public void setError(String error) {
        this.error = error;
    }
}
