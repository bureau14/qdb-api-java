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

import java.util.ArrayList;
import java.util.List;

/**
 * Quasardb run batch operations results.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2014
 * @version master
 * @since 1.1.0
 */
public class Results {
    private boolean success;
    private List<Result<?>> results = new ArrayList<Result<?>>();
    
    public Results() {
    }
    
    /**
     * Are all submitted batch operations in success ?
     * 
     * @return true if all submitted operations are successful. false in all other cases.  
     */
    public boolean isSuccess() {
        return success;
    }
    
    /**
     * Set if all submitted batch operations are successful or not.
     * 
     * @param success true if all submitted operations are successful. false in all other cases.
     */
    public void setSuccess(boolean success) {
        this.success = success;
    }
    
    /**
     * Get all results of submitted operations.
     * 
     * @return all results for submitted batch operations.
     */
    public List<Result<?>> getResults() {
        return results;
    }
    
    /**
     * Set all results of submitted operations.
     * 
     * @param results results for submitted batch operations.
     */
    public void setResults(List<Result<?>> results) {
        this.results = results;
    }
}
