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

import java.util.Arrays;
   
/**
 * A non-shell profile tree node implementation. This implementation trades off
 * some object orientation "niceness" to achieve more memory compactness.
 */
final class ObjectProfileNode<T> extends AbstractProfileNode<T> {
    public Object object() {
        return m_obj;
    }
    
    public String name() {
        return m_link == null?ObjectProfiler.INPUT_OBJECT_NAME:m_link.name();
    }

    public IObjectProfileNode shell() {
        return m_shell;
    }
    
    public IObjectProfileNode[] children() {
        return m_children;
    }

    public int refcount() {
        return m_refcount;
    }
    
    public boolean traverse(final INodeFilter filter, final INodeVisitor visitor) {
        if ((visitor != null) && ((filter == null) || filter.accept(this))) {
            visitor.previsit(this);
            final IObjectProfileNode[] children = m_children;
            for (int i = 0; i < children.length; ++ i) {
                children[i].traverse(filter, visitor);
            }
            visitor.postvisit(this);
            return true;
        }
        return false;
    }

    /**
     * This method manages the vector in m_children field for an unfinished node.
     */
    void addFieldRef(final IObjectProfileNode node) {
        // [m_size is the child count]
        IObjectProfileNode[] children = m_children;
        final int childrenLength = children.length;
        if (m_size >= childrenLength) {
            final IObjectProfileNode[] newchildren = new IObjectProfileNode[Math.max (1, childrenLength << 1)];
            System.arraycopy(children, 0, newchildren, 0, childrenLength);
            m_children = children = newchildren;
        }
        children [m_size++] = node;
    }
    
    /**
     * This method is called once on every node to lock it down into its
     * immutable and most compact representation during phase 2 of profile
     * tree construction.
     */
    void finish() {
        final int childCount = m_size; // m_size is the child count for a non-shell node
        if (childCount > 0) {
            if (childCount < m_children.length) {
                final IObjectProfileNode[] newadj = new IObjectProfileNode[childCount];
                System.arraycopy(m_children, 0, newadj, 0, childCount);
                m_children = newadj;
            }
            Arrays.sort(m_children);
            int size = 0;
            for (int i = 0; i < childCount; ++ i) {
                size += m_children[i].size();
            }
            m_size = size; // m_size is the full node size for all nodes
        }
    }

    ObjectProfileNode(final ObjectProfileNode<?> parent, final Object obj, final ILink link) {
        super(parent);
        m_obj = obj;
        m_link = link;
        m_refcount = 1;
        m_children = EMPTY_OBJECTPROFILENODE_ARRAY;
    }

    final ILink m_link;
    final Object m_obj;
    int m_refcount;
    AbstractShellProfileNode<?> m_shell;
    IObjectProfileNode[] m_children;
}
