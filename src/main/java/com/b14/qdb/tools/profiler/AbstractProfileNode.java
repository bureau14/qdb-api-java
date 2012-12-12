/**
 * Copyright (c) 2009-2011, Bureau 14 SARL
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
 *    * Neither the name of the University of California, Berkeley nor the
 *      names of its contributors may be used to endorse or promote products
 *      derived from this software without specific prior written permission.
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

package com.b14.qdb.tools.profiler;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedList;

/**
 * Abstract base class for all node implementations in this package. 
 * 
 */
abstract class AbstractProfileNode<T> implements IObjectProfileNode, Comparable<T> {
    // IObjectProfileNode:
    public final int size() {
        return m_size;
    }

    public final IObjectProfileNode parent() {
        return m_parent;
    }
    
    public final IObjectProfileNode[] path() {
        IObjectProfileNode[] path = m_path;
        if (path != null){
            return path;
        } else {                
            final LinkedList <IObjectProfileNode> _path = new LinkedList<IObjectProfileNode>();
            for (IObjectProfileNode node = this; node != null; node = node.parent()) {
                _path.addFirst(node);
            }
            path = new IObjectProfileNode[_path.size()];
            _path.toArray(path);
            m_path = path; 
            return path;
        }
    }
    
    public final IObjectProfileNode root() {
        IObjectProfileNode node = this;
        for (IObjectProfileNode parent = parent(); parent != null; node = parent, parent = parent.parent());
        return node;
    }
    
    public final int pathlength() {
        final IObjectProfileNode[] path = m_path;
        if (path != null) {
            return path.length;
        } else {                
            int result = 0;
            for (IObjectProfileNode node = this; node != null; node = node.parent()) {
                ++result;
            }
            return result;
        }
    }
    
    public final String dump() {
        final StringWriter sw = new StringWriter();
        final PrintWriter out = new PrintWriter(sw);
        final INodeVisitor visitor = ObjectProfileVisitors.newDefaultNodePrinter(out, null, null, ObjectProfiler.SHORT_TYPE_NAMES);
        traverse(null, visitor);
        out.flush();
        return sw.toString();
    }
    
    // Comparable
    public final int compareTo(final Object obj) {
        return ((AbstractProfileNode<?>) obj).m_size - m_size;
    }
    
    public String toString() {
        return super.toString() + ": name = " + name() + ", size = " + size();
    }
 
    AbstractProfileNode(final IObjectProfileNode parent) {
        m_parent = parent;
    }
    
    int m_size;
    static final IObjectProfileNode[] EMPTY_OBJECTPROFILENODE_ARRAY = new IObjectProfileNode[0];   
    private final IObjectProfileNode m_parent;
    private transient IObjectProfileNode[] m_path;
}
