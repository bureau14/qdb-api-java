package com.b14.qdb.batch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class QdbBatchResult {
    private boolean success;
    private long nbOperations;
    private long nbSuccess;
    private List<Result> results = new ArrayList<Result>();
    
    /**
     * @return the success
     */
    public boolean isSuccess() {
        return success;
    }
    
    /**
     * @param success the success to set
     */
    public void setSuccess(boolean success) {
        this.success = success;
    }
    
    /**
     * @return the nbOperations
     */
    public long getNbOperations() {
        return nbOperations;
    }
    
    /**
     * @param nbOperations the nbOperations to set
     */
    public void setNbOperations(long nbOperations) {
        this.nbOperations = nbOperations;
    }
    
    /**
     * @return the nbSuccess
     */
    public long getNbSuccess() {
        return nbSuccess;
    }
    /**
     * @param nbSuccess the nbSuccess to set
     */
    public void setNbSuccess(long nbSuccess) {
        this.nbSuccess = nbSuccess;
    }
    
    /**
     * 
     * @return
     */
    public Collection<Result> getResults() {
        return Collections.unmodifiableCollection(results);
    }
    
    /**
     * 
     * @param result
     */
    public void addResult(Result result) {
        results.add(result);
    }
}
