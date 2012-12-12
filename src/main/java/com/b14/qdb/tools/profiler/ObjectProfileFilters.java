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

/**
 * A Factory for a few stock node filters. See the implementation for details.
 * 
 */
public abstract class ObjectProfileFilters {
    
    /**
     * Factory method for creating a visitor that only accepts profile nodes
     * with sizes larger than a given threshold value.
     * 
     * @param threshold node size in bytes
     */
    public static ObjectProfileNode.INodeFilter newSizeFilter(final int threshold) {
        return new SizeFilter(threshold);
    }
    
    /**
     * Factory method for creating a visitor that accepts a profile node only if
     * it is at least the k-th largest child of its parent for a given value
     * of k. E.g., newRankFilter(1) will prune the profile tree so that only the
     * largest child is visited for every node. 
     * 
     * @param rank acceptable size rank [must be >= 0]
     */
    public static ObjectProfileNode.INodeFilter newRankFilter(final int rank) {
        return new RankFilter(rank);
    }
    
    /**
     * Factory method for creating a visitor that accepts a profile node only if
     * its size is larger than a given threshold relative to the size of the root
     * node (i.e., size of the entire profile tree). 
     * 
     * @param threshold size fraction threshold
     */
    public static ObjectProfileNode.INodeFilter newSizeFractionFilter(final double threshold) {
        return new SizeFractionFilter(threshold);
    }

    /**
     * Factory method for creating a visitor that accepts a profile node only if
     * its size is larger than a given threshold relative to the size of its
     * parent node. This is useful for pruning the profile tree to show the
     * largest contributors at every tree level.
     * 
     * @param threshold size fraction threshold
     */    
    public static ObjectProfileNode.INodeFilter newParentSizeFractionFilter(final double threshold) {
        return new ParentSizeFractionFilter(threshold);
    }   
    
    private ObjectProfileFilters() {} // this class is not extendible

    private static final class SizeFilter implements IObjectProfileNode.INodeFilter {
        public boolean accept(final IObjectProfileNode node) {
            return node.size() >= m_threshold;
        }
  
        SizeFilter(final int threshold) {
            m_threshold = threshold;
        }
        
        private final int m_threshold;
    } // end of nested class
    
    private static final class RankFilter implements IObjectProfileNode.INodeFilter {
        public boolean accept(final IObjectProfileNode node) {
            final IObjectProfileNode parent = node.parent();
            if (parent == null) {
                return true;
            }
            final IObjectProfileNode[] siblings = parent.children();
            for (int r = 0, rLimit = Math.min(siblings.length, m_threshold); r< rLimit; ++r) {
                if (siblings [r] == node) {
                    return true;
                }
            }
            return false;
        }
        
        RankFilter(final int threshold) {
            m_threshold = threshold;
        }

        private final int m_threshold;
    }
    
    private static final class SizeFractionFilter implements IObjectProfileNode.INodeFilter {
        public boolean accept(final IObjectProfileNode node) {
            if (node.size() >= m_threshold * node.root().size()) {
                return true;
            } else {
                return false;
            }
        }
     
        SizeFractionFilter(final double threshold) {
            m_threshold = threshold;
        }
         
        private final double m_threshold;
    } // end of nested class
    
    private static final class ParentSizeFractionFilter implements IObjectProfileNode.INodeFilter {
        public boolean accept(final IObjectProfileNode node) {
            final IObjectProfileNode parent = node.parent ();
            if (parent == null) {
                return true; // always accept root node
            } else if (node.size () >= m_threshold * parent.size ()) {
                return true;
            } else {
                return false;
            }
        }
        
        ParentSizeFractionFilter(final double threshold) {
            m_threshold = threshold;
        }
 
        private final double m_threshold;
    } // end of nested class
}
