package com.b14.qdb.hadoop.mapreduce.examples.standarddeviation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class People implements Writable {
    private String gender;
    private double height;
    private double weight;
    
    public People (String gender, double height, double weight) {
        this.gender = gender;
        this.height = height;
        this.weight = weight;
    }
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    public void readFields(DataInput in) throws IOException {
       this.gender = in.readUTF();
       this.height = in.readDouble();
       this.weight = in.readDouble();
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    public void write(DataOutput out) throws IOException {
        out.writeUTF(gender);
        out.writeDouble(height);
        out.writeDouble(weight);
    }
    
    /**
     * @return the gender
     */
    public String getGender() {
        return gender;
    }
    
    /**
     * @return the height
     */
    public double getHeight() {
        return height;
    }
    
    /**
     * @return the weith
     */
    public double getWeight() {
        return weight;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((gender == null) ? 0 : gender.hashCode());
        long temp;
        temp = Double.doubleToLongBits(height);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(weight);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        People other = (People) obj;
        if (gender == null) {
            if (other.gender != null)
                return false;
        } else if (!gender.equals(other.gender))
            return false;
        if (Double.doubleToLongBits(height) != Double
                .doubleToLongBits(other.height))
            return false;
        if (Double.doubleToLongBits(weight) != Double
                .doubleToLongBits(other.weight))
            return false;
        return true;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "People [gender=" + gender + ", height=" + height + ", weight=" + weight + "]";
    }    
}
