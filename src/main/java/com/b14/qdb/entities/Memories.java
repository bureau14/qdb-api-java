package com.b14.qdb.entities;

import com.owlike.genson.annotation.JsonProperty;

public class Memories implements java.io.Serializable {
    private static final long serialVersionUID = -2649767686268390331L;
    
    Memory physmem;
    Memory vm;

    public Memories(@JsonProperty("physmem") Memory physmem,
                    @JsonProperty("vm") Memory vm) {
        super();
        this.physmem=physmem;
        this.vm=vm;
    }

    public Memory getPhysmem() {
        return physmem;
    }

    public void setPhysmem(Memory physmem) {
        this.physmem = physmem;
    }

    public Memory getVm() {
        return vm;
    }

    public void setVm(Memory vm) {
        this.vm = vm;
    }

}
