package com.b14.qdb.entities;

import com.owlike.genson.annotation.JsonProperty;

public class Operations implements java.io.Serializable {
    private static final long serialVersionUID = -7025011222312385692L;
    
    CompareAndSwap compare_and_swap;
    Find find;
    FindUpdate find_update;
    Put put;
    Remove remove;
    RemoveAll remove_all;
    Update update;
    
    public Operations(@JsonProperty("compare_and_swap") CompareAndSwap compare_and_swap, 
                 @JsonProperty("find") Find find,
                 @JsonProperty("find_update") FindUpdate find_update,
                 @JsonProperty("put") Put put,
                 @JsonProperty("remove") Remove remove,
                 @JsonProperty("remove_all") RemoveAll remove_all,
                 @JsonProperty("update") Update update) {
        super();
        this.compare_and_swap=compare_and_swap;
        this.find=find;
        this.find_update=find_update;
        this.put=put;
        this.remove=remove;
        this.remove_all=remove_all;
        this.update=update;
    }

    public CompareAndSwap getCompare_and_swap() {
        return compare_and_swap;
    }

    public void setCompare_and_swap(CompareAndSwap compare_and_swap) {
        this.compare_and_swap = compare_and_swap;
    }

    public Find getFind() {
        return find;
    }

    public void setFind(Find find) {
        this.find = find;
    }

    public FindUpdate getFind_update() {
        return find_update;
    }

    public void setFind_update(FindUpdate find_update) {
        this.find_update = find_update;
    }

    public Put getPut() {
        return put;
    }

    public void setPut(Put put) {
        this.put = put;
    }

    public Remove getRemove() {
        return remove;
    }

    public void setRemove(Remove remove) {
        this.remove = remove;
    }

    public RemoveAll getRemove_all() {
        return remove_all;
    }

    public void setRemove_all(RemoveAll remove_all) {
        this.remove_all = remove_all;
    }

    public Update getUpdate() {
        return update;
    }

    public void setUpdate(Update update) {
        this.update = update;
    }
    
}
