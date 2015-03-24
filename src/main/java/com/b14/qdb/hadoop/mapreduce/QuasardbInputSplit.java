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

package com.b14.qdb.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import com.b14.qdb.QuasardbNode;

/**
 * quasardb specific extension of {@link InputSplit}.
 * <p>
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @see InputSplit
 * @see Writable
 * @version master
 * @since 1.3.0
 */
public class QuasardbInputSplit extends InputSplit implements Writable {
    private static Pattern VALID_IPV4_PATTERN = null;
    private static Pattern VALID_IPV6_PATTERN = null;
    private static final String ipv4Pattern = "(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])";
    private static final String ipv6Pattern = "([0-9a-f]{1,4}:){7}([0-9a-f]){1,4}";
    static {
        try {
            VALID_IPV4_PATTERN = Pattern.compile(ipv4Pattern, Pattern.CASE_INSENSITIVE);
            VALID_IPV6_PATTERN = Pattern.compile(ipv6Pattern, Pattern.CASE_INSENSITIVE);
        } catch (PatternSyntaxException e) {
            e.printStackTrace();
        }
    }
    
    private String[] inputs;
    private QuasardbNode[] nodes;
    
    /**
     * Default constructor for reflection.
     * 
     * @since 1.3.0
     */
    public QuasardbInputSplit() {
    }

    /**
     * Build an {@link InputSplit} with provided keys and nodes locations
     * 
     * @param split collection of keys which have been provided or computed
     * @param nodes collection of {@link QuasardbNode}
     * @since 1.3.0
     */
    public QuasardbInputSplit(List<String> split, QuasardbNode[] nodes) {
       if (split != null) {
           this.inputs = split.toArray(new String[split.size()]);
       }
       if (nodes != null) {
           this.nodes = nodes.clone();
       }
    }

    /**
     * Retrieve a keys collection which data will be fetched by the record reader
     * 
     * @return collection of keys
     * @since 1.3.0
     */
    public synchronized Collection<String> getInputs() {
        if (this.inputs != null) {
            return Arrays.asList(this.inputs.clone());
        } else {
            return new ArrayList<String>();
        }
    }
    
    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    public void readFields(final DataInput input) throws IOException {
        if (input == null) {
            throw new IOException("No input provided");
        }
        
        // Topologies
        try {
            this.nodes = new QuasardbNode[input.readInt()];
            for (int i = 0; i < nodes.length; i++) {
                String node = input.readUTF();
                this.nodes[i] = new QuasardbNode(node.substring(0, node.indexOf(':')), Integer.parseInt(node.substring(node.indexOf(':') + 1, node.length())));
            }
        } catch (IndexOutOfBoundsException e) {
            throw new IOException("Wrong provided input format => " + e.getMessage());
        } catch (NumberFormatException e) {
            throw new IOException("Wrong port format for provided input => " + e.getMessage());
        }
        
        // Inputs
        this.inputs = new String[input.readInt()];
        for (int i = 0; i < this.inputs.length; i++) {
            this.inputs[i] = input.readUTF();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    public void write(final DataOutput output) throws IOException {
        if ((this.nodes == null) || (this.inputs == null)) {
            throw new IOException("QuasardbInputSplit instance is invalid because nodes or inputs are null.");
        }
        
        // Topologies
        output.writeInt(this.nodes.length);
        for (QuasardbNode node : this.nodes) {
            output.writeUTF(node.toString());
        }
        
        // Inputs
        output.writeInt(this.inputs.length);
        for (String input: this.inputs) {
            output.writeUTF(input);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    @Override
    public long getLength() {
        if (this.inputs != null) {
            return this.inputs.length;
        } else {
            return 0L;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @since 1.3.0
     */
    @Override
    public String[] getLocations() {
        if (this.nodes != null) {
            String[] sLocs = new String[this.nodes.length];
            for (int i = 0; i < this.nodes.length; i++) {
                sLocs[i] = this.nodes[i].getHostName();
                
                // Hadoop doesn't want IP address...
                if (isIpAddress(sLocs[i])) {
                    try {
                        sLocs[i] = InetAddress.getByName(sLocs[i]).getHostName();
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    }
                }
            }
            return sLocs;
        } else {
            return new String[] {};
        }
    }
    
    /**
     * Retrieve quasardb node locations for given inputs.<br><br>
     * Similar to {@link InputSplit#getLocations()} except that you get <i>&lt;hostname + port&gt;</i> instead of only <i>hostname</i>.
     * 
     * @return quasardb locations
     * @since 1.3.0
     */
    public QuasardbNode[] getQdbLocations() {
        if (this.nodes != null) {
            return this.nodes.clone();
        } else {
            return new QuasardbNode[] {};
        }
    }
    
    /**
     * Determine if the given string is a valid IPv4 or IPv6 address.<br>
     * This method uses pattern matching to see if the given string could be a valid IP address.
     *
     * @param ipAddress A string that is to be examined to verify whether or not it could be a valid IP address.
     * @return <code>true</code> if the string is a value that is a valid IP address, <code>false</code> otherwise.
     * @since 1.3.0
     */
    private boolean isIpAddress(String ipAddress) {
      Matcher m1 = VALID_IPV4_PATTERN.matcher(ipAddress);
      if (m1.matches()) {
        return true;
      }
      Matcher m2 = VALID_IPV6_PATTERN.matcher(ipAddress);
      return m2.matches();
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     * @since 1.3.0
     */
    @Override
    public String toString() {
        return "QuasardbInputSplit [inputs=" + Arrays.toString(inputs) + ", nodes=" + Arrays.toString(nodes) + "]";
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#hashCode()
     * @since 1.3.0
     */
    @Override 
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(inputs);
        result = prime * result + Arrays.hashCode(nodes);;
        return result;
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#equals(Object)
     * @since 1.3.0
     */
    @Override 
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof QuasardbInputSplit)) {
            return false;
        }
        QuasardbInputSplit other = (QuasardbInputSplit) obj;
        if (!Arrays.equals(inputs, other.inputs)) {
            return false;
        }
        if (!Arrays.equals(nodes, other.nodes)) {
            return false;
        }
        return true;
    }
}
