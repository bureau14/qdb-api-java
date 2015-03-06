package com.b14.qdb.hadoop.mapreduce.examples.wordcounter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class WordCountResult implements Writable {

    private String word;
    private int count;

    /**
     * @param word
     * @param count
     */
    public WordCountResult(String word, int count) {
        this.word = word;
        this.count = count;
    }

    /**
     * Default CTOR for Hadoop's de-serialization 
     */
    public WordCountResult() {}
    
    /**
     * @return the word
     */
    public String getWord() {
        return word;
    }

    /**
     * @return the count
     */
    public int getCount() {
        return count;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    public void readFields(DataInput in) throws IOException {
        word = in.readUTF();
        count = in.readInt();
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    public void write(DataOutput out) throws IOException {
        out.writeUTF(word);
        out.write(count);
    }
}
