package com.b14.qdb;

import java.nio.ByteBuffer;

public class QdbEntry {
    private String key;
    private transient ByteBuffer value;
    
    /**
     * 
     * @param key
     * @param value
     */
    public QdbEntry(String key, ByteBuffer value) {
        this.key = key;
        this.value = value;
    }

    /**
     * @return the key
     */
    public String getKey() {
        return key;
    }

    /**
     * @param key the key to set
     */
    public void setKey(String key) {
        this.key = key;
    }

    /**
     * @return the value
     */
    public ByteBuffer getValue() {
        return value;
    }

    /**
     * @param value the value to set
     */
    public void setValue(ByteBuffer value) {
        this.value = value;
    }
}
