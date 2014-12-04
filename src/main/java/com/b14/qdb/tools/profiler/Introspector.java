/**
 * Copyright (c) 2009-2013, Bureau 14 SARL
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
 *    * Neither the name of Bureau 14 nor the names of its contributors may
 *      be used to endorse or promote products derived from this software
 *      without specific prior written permission.
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

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import sun.misc.Unsafe;

/**
 * This class could be used for any object contents/memory layout printing.
 * <br>
 * <br>
 * How shall we find a memory layout of an object?
 * <ul>
 * <li>Obtain all object fields, including its parent class fields, recursively calling Class.getDeclaredFields on the class and its superclasses.</li>
 * <li>For all non-static fields <i>(Field.getModifiers() & Modifiers.STATIC)</i> obtain an offset of a field in its parent object using <b>Unsafe.objectFieldOffset</b> and a shallow field size : predefined values for primitives and either 4 or 8 bytes for object references.</li>
 * <li>For arrays, call Unsafe.arrayBaseOffset and Unsafe.arrayIndexScale. The total shallow size of an array would be <b>offset + scale * Array.getLength(array)</b> and, of course, a reference to the array itself (see previous point).</li>
 * <li>Do not forget that there may be a circular references in the object graph, so you will need to track all previously visited objects (IdentityHashMap is recommended for such cases).</li>
 * </ul>
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2014
 * @version master
 * @since 1.1.1
 */
@SuppressWarnings("restriction")
public class Introspector {
    private static final Unsafe unsafe;

    /** Size of any Object reference */
    private static final int objectRefSize = 8;
    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);

            // Java Object reference size is quite a virtual value. 
            // It may be equal to 4 or 8 bytes depending on your JVM settings and on the amount of memory you have given to your JVM. 
            // It is always 8 bytes for heaps over 32G, but for smaller heaps it is 4 bytes unless you will turn off -XX:-UseCompressedOops JVM setting (Not sure in what JVM release this feature was added or turned on by default). 
            // As a result, the safest way to obtain an Object reference size is to find a size of an element in Object[] array: unsafe.arrayIndexScale( Object[].class ). 
            // Unsafe.addressSize proved to be not so useful for this purpose.
            //
            // A small implementation note on 4 byte references on under 32G heaps. 
            // A normal 4 byte pointer could address any byte in 4G address space. 
            // If we will assume that all allocated objects will be aligned by 8 bytes boundary, we won’t need 3 lowest bits in our 32 bit pointers anymore (these bits will always be equal to zeroes). 
            // This means that we can store 35 bit addresses in 32 bit value:
            // 32_bit_reference = ( int ) ( actual_64_bit_pointer >> 3 )
            // 35 bits allow you to address 32 bit * 8 = 4G * 8 = 32G address space.
            // 
            // objectRefSize = unsafe.arrayIndexScale(Object[].class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Sizes of all primitive values */
    private static final Map<Class<?>, Integer> primitiveSizes;
    static {
        primitiveSizes = new HashMap<Class<?>, Integer>(10);
        primitiveSizes.put(byte.class, 1);
        primitiveSizes.put(char.class, 2);
        primitiveSizes.put(int.class, 4);
        primitiveSizes.put(long.class, 8);
        primitiveSizes.put(float.class, 4);
        primitiveSizes.put(double.class, 8);
        primitiveSizes.put(boolean.class, 1);
    }


    // We need to keep track of already visited objects in order to support cycles in the object graphs
    private IdentityHashMap<Object, Boolean> m_visited = new IdentityHashMap<Object, Boolean>(100);
    
    /**
     * Get object information for any Java object. 
     * Do not pass primitives to this method because they will boxed and the information you will get will be related to a boxed version of your value.
     * 
     * @param obj Object to introspect
     * @return Object info
     * @throws IllegalAccessException
     * 
     * @version master
     * @since 1.1.1
     */
    public ObjectInfo introspect(final Object obj) throws IllegalAccessException {
        try {
            return introspect(obj, null);
        } finally { 
            // clean visited cache before returning in order to make this object reusable
            m_visited.clear();
        }
    }

    private ObjectInfo introspect(final Object obj, final Field fld) throws IllegalAccessException {
        // Use Field type only if the field contains null. 
        // In this case we will at least know what's expected to be stored in this field. 
        // Otherwise, if a field has interface type, we won't see what's really stored in it.
        // Besides, we should be careful about primitives, because they are passed as boxed values in this method (first arg is object) - for them we should still rely on the field type.
        boolean isPrimitive = fld != null && fld.getType().isPrimitive();
        
        // Will be set to true if we have already seen this object
        boolean isRecursive = false; 
        if (!isPrimitive) {
            if (m_visited.containsKey(obj)) {
                isRecursive = true;
            }
            m_visited.put(obj, true);
        }

        final Class<?> type = (fld == null || (obj != null && !isPrimitive)) ? obj.getClass() : fld.getType();
        int arraySize = 0;
        int baseOffset = 0;
        int indexScale = 0;
        if (type.isArray() && obj != null) {
            baseOffset = unsafe.arrayBaseOffset(type); // get offset of a first element in the array
            indexScale = unsafe.arrayIndexScale(type); // get size of an element in the array
            arraySize = baseOffset + indexScale * Array.getLength(obj);
        }

        final ObjectInfo root;
        if (fld == null) {
            root = new ObjectInfo("", type.getCanonicalName(), getContents(obj, type), 0, getShallowSize(type), arraySize, baseOffset, indexScale);
        } else {
            final int offset = (int) unsafe.objectFieldOffset(fld);
            root = new ObjectInfo(fld.getName(), type.getCanonicalName(), getContents(obj, type), offset, getShallowSize(type), arraySize, baseOffset, indexScale);
        }

        if (!isRecursive && obj != null) {
            if (isObjectArray(type)) {
                // Introspect object arrays
                final Object[] ar = (Object[]) obj;
                for (final Object item : ar) {
                    if (item != null) {
                        root.addChild(introspect(item, null));
                    }
                }
            } else {
                for (final Field field : getAllFields(type)) {
                    if ((field.getModifiers() & Modifier.STATIC) != 0) {
                        continue;
                    }
                    field.setAccessible(true);
                    root.addChild(introspect(field.get(obj), field));
                }
            }
        }

        root.sort(); // sort by offset
        return root;
    }

    /**
     * Get all fields for this class, including all superclasses fields
     * 
     * @param type
     * @return
     * 
     * @version master
     * @since 1.1.1
     */
    private static List<Field> getAllFields(final Class<?> type) {
        if (type.isPrimitive()) {
            return Collections.emptyList();
        }
        Class<?> cur = type;
        final List<Field> res = new ArrayList<Field>(10);
        while (true) {
            Collections.addAll(res, cur.getDeclaredFields());
            if (cur == Object.class) {
                break;
            }
            cur = cur.getSuperclass();
        }
        return res;
    }

    /**
     * Check if it is an array of objects. 
     * I suspect there must be a more API-friendly way to make this check.
     * 
     * @param type
     * @return
     * 
     * @version master
     * @since 1.1.1
     */
    private static boolean isObjectArray(final Class<?> type) {
        if (!type.isArray()) {
            return false;
        }
        if (type == byte[].class 
                || type == boolean[].class
                || type == char[].class 
                || type == short[].class
                || type == int[].class 
                || type == long[].class
                || type == float[].class 
                || type == double[].class) {
            return false;
        }
        return true;
    }

    /**
     * Advanced toString logic
     * 
     * @param val
     * @param type
     * @return
     * 
     * @version master
     * @since 1.1.1
     */
    private static String getContents(final Object val, final Class<?> type) {
        if (val == null) {
            return "null";
        }
        if (type.isArray()) {
            if (type == byte[].class)
                return Arrays.toString((byte[]) val);
            else if (type == boolean[].class)
                return Arrays.toString((boolean[]) val);
            else if (type == char[].class)
                return Arrays.toString((char[]) val);
            else if (type == short[].class)
                return Arrays.toString((short[]) val);
            else if (type == int[].class)
                return Arrays.toString((int[]) val);
            else if (type == long[].class)
                return Arrays.toString((long[]) val);
            else if (type == float[].class)
                return Arrays.toString((float[]) val);
            else if (type == double[].class)
                return Arrays.toString((double[]) val);
            else
                return Arrays.toString((Object[]) val);
        }
        return val.toString();
    }

    /**
     * Obtain a shallow size of a field of given class (primitive or object reference size)
     * 
     * @param type
     * @return
     * 
     * @version master
     * @since 1.1.1
     */
    private static int getShallowSize(final Class<?> type) {
        if (type.isPrimitive()) {
            final Integer res = primitiveSizes.get(type);
            return res != null ? res : 0;
        } else
            return objectRefSize;
    }
}
