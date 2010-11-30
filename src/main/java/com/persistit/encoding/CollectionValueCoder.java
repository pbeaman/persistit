/*
 * Copyright (c) 2005 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 * 
 * Created on Jun 14, 2005
 */
package com.persistit.encoding;

import java.util.AbstractSequentialList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.persistit.Value;
import com.persistit.exception.ConversionException;

/**
 * <p>
 * Implements ValueCoder, ValueRenderer and ValueDisplayer for selected classes
 * in the Java Collections API. By default an instance of this coder is
 * registered for each of the following concrete classes:
 * <ul>
 * <li>java.util.ArrayList</li>
 * <li>java.util.LinkedList</li>
 * <li>java.util.Stack</li>
 * <li>java.util.Vector</li>
 * <li>java.util.Properties</li>
 * <li>java.util.HashMap</li>
 * <li>java.util.HashSet</li>
 * <li>java.util.Hashtable</li>
 * <li>java.util.TreeMap</li>
 * <li>java.util.TreeSet</li>
 * <li>java.util.LinkedHashMap (Java 1.4+)</li>
 * <li>java.util.LinkedHashSet (Java 1.4+)</li>
 * </ul>
 * </p>
 * <p>
 * The stored data consist only of items accessible through the
 * <tt>Collection</tt> or <tt>Map</tt> interfaces. For example, implementation
 * details such as the current loadFactor for a <tt>HashMap</tt> are not stored.
 * For any <tt>Collection</tt> (including <tt>List</tt> and <tt>Set</tt>), the
 * stored format is simply a list of all the values in the order returned by an
 * <tt>Iterator</tt>. For a <tt>Map</tt> the stored format is simply a key/value
 * pair for each entry in the order returned by the <tt>Iterator</tt> returned
 * by the <tt>entrySet()</tt> method of the <tt>Map</tt>.
 * </p>
 * <p>
 * Because customer-written <tt>Collection</tt> and <tt>Map</tt> implementations
 * may contain additional internal state that may also need to be stored when
 * they are serialized, this coder is registered only for specific
 * implementations provided by the JRE. You may register and use this ValueCoder
 * for any custom <tt>Collection</tt> or <tt>Map</tt> that does not need to
 * serialize additional data.
 * </p>
 * <p>
 * Note that for <tt>ArrayList</tt>, <tt>Vector</tt>, <tt>Stack</tt>, and other
 * <tt>List</tt> implementations that do not extend
 * <tt>AbstractSequentialList</tt>, the serialization logic uses the
 * <tt>get(index)</tt> method of <tt>List</tt> to acquire each member rather
 * than constructing an <tt>Interator</tt>. Subclasses of
 * <tt>AbstractSequentialList</tt> are serialized by using an <tt>Iterator</tt>
 * because access by index may be inefficient.
 * </p>
 * 
 * @since 1.1
 * @version 1.1
 */
public class CollectionValueCoder implements ValueRenderer, ValueDisplayer {

    /**
     * <p>
     * Encodes the supplied <tt>Object</tt> into the supplied <tt>Value</tt>.
     * This method will be called only if this <tt>ValueCoder</tt> has been
     * registered with the current {@link CoderManager} to encode objects having
     * the class of the supplied object.
     * </p>
     * <p>
     * Upon completion of this method, the backing byte array of the
     * <tt>Value</tt> and its size should be updated to reflect the serialized
     * collection. Use the methods {@link Value#getEncodedBytes},
     * {@link Value#getEncodedSize} and {@link Value#setEncodedSize} to
     * manipulate the byte array directly. More commonly, the implementation of
     * this method will simply call the appropriate <tt>put</tt> methods to
     * write the interior field values into the <tt>Value</tt> object.
     * </p>
     * 
     * @param value
     *            The <tt>Value</tt> to which the interior data of the supplied
     *            <tt>Object</tt> should be encoded
     * @param object
     *            The object value to encode. This parameter will never be
     *            <tt>null</tt> because Persistit encodes nulls with a built-in
     *            encoding.
     * @param context
     *            An arbitrary object that can optionally be supplied by the
     *            application to convey an application-specific context for the
     *            operation. (See {@link CoderContext}.) The default value is
     *            <tt>null</tt>.
     */
    public void put(Value value, Object object, CoderContext context)
            throws ConversionException {
        if (object instanceof Map) {
            for (Iterator iter = ((Map) object).entrySet().iterator(); iter
                    .hasNext();) {
                Map.Entry entry = (Map.Entry) iter.next();
                value.put(entry.getKey(), context);
                value.put(entry.getValue(), context);
            }
        } else if (object instanceof List
                && !(object instanceof AbstractSequentialList)) {
            List list = (List) object;
            int size = list.size();
            for (int index = 0; index < size; index++) {
                value.put(list.get(index), context);
            }
        } else if (object instanceof Collection) {
            for (Iterator iter = ((Collection) object).iterator(); iter
                    .hasNext();) {
                value.put(iter.next(), context);
            }
        } else {
            throw new ConversionException(
                    "CollectionValueCoder cannot encode an object of type "
                            + object.getClass().getName());
        }
    }

    /**
     * <p>
     * Creates an instance of the supplied class, populates its state by
     * decoding the supplied <tt>Value</tt>, and returns it. This method will be
     * called only if this <tt>ValueCoder</tt> has been registered with the
     * current {@link CoderManager} to encode objects having supplied
     * <tt>Class</tt> value. Persistit will never call this method to decode a
     * value that was <tt>null</tt> when written because null values are handled
     * by built-in encoding logic.
     * </p>
     * 
     * @param value
     *            The <tt>Value</tt> from which interior fields of the object
     *            are to be retrieved
     * @param clazz
     *            The class of the object to be returned.
     * @param context
     *            An arbitrary object that can optionally be supplied by the
     *            application to convey an application-specific context for the
     *            operation. (See {@link CoderContext}.) The default value is
     *            <tt>null</tt>.
     * @return An <tt>Object</tt> having the same class as the suppled
     *         <tt>clazz</tt> parameter.
     * @throws ConversionException
     */
    public Object get(Value value, Class clazz, CoderContext context)
            throws ConversionException {
        try {
            Object target = clazz.newInstance();
            value.registerEncodedObject(target);
            render(value, target, clazz, context);
            return target;
        } catch (InstantiationException ce) {
            throw new ConversionException(ce
                    + " while decoding a Collection value");
        } catch (IllegalAccessException iae) {
            throw new ConversionException(iae
                    + " while decoding a Collection value");
        }
    }

    /**
     * <p>
     * Populates the state of the supplied (mutable) target <tt>Object</tt> by
     * decoding the supplied <tt>Value</tt>. This method will be called only if
     * this <tt>ValueRenderer</tt> has been registered with the current
     * {@link CoderManager} to encode objects having the supplied <tt>Class</tt>
     * value. Persistit will never call this method to decode a value that was
     * <tt>null</tt> when written because null values are handled by built-in
     * encoding logic.
     * </p>
     * 
     * @param value
     *            The <tt>Value</tt> from which interior fields of the object
     *            are to be retrieved
     * 
     * @param target
     *            The object into which the decoded value is to be written
     * 
     * @param clazz
     *            The class of the object that was originally encoded into
     *            Value.
     * 
     * @param context
     *            An arbitrary object that can optionally be supplied by the
     *            application to convey an application-specific context for the
     *            operation. (See {@link CoderContext}.) The default value is
     *            <tt>null</tt>.
     * 
     * @throws ConversionException
     */
    public void render(Value value, Object target, Class clazz,
            CoderContext context) throws ConversionException {
        if (target instanceof Map) {
            if (Map.class.isAssignableFrom(clazz)) {
                Map map = (Map) target;
                for (;;) {
                    if (!value.hasMoreItems()) {
                        break;
                    }
                    Object itemKey = value.get(null, context);
                    if (!value.hasMoreItems()) {
                        throw new ConversionException(
                                "Encoded Map entry has missing value");
                    }
                    Object itemValue = value.get(null, context);
                    map.put(itemKey, itemValue);
                }
            } else
                throw new ConversionException("Cannot convert a "
                        + clazz.getName() + " to a Map");
        } else if (target instanceof Collection) {
            if (Collection.class.isAssignableFrom(clazz)) {
                Collection collection = (Collection) target;
                for (;;) {
                    if (!value.hasMoreItems()) {
                        break;
                    }
                    Object itemValue = value.get(null, context);
                    collection.add(itemValue);
                }
            } else
                throw new ConversionException("Cannot convert "
                        + clazz.getName() + " to a Collection");

        } else
            throw new ConversionException(
                    "CollectionValueCoder cannot render to an object of class "
                            + target.getClass().getName());
    }

    /**
     * <p>
     * Writes a String representation of the value into a supplied
     * <tt>StringBuilder</tt>. This is used in utility programs to display
     * stored content without actually deserialized Objects represented by the
     * value.
     * </p>
     * <p>
     * This method will be called only if this <tt>ValueDisplayer</tt> has been
     * registered with the current {@link CoderManager} to encode objects having
     * the supplied <tt>Class</tt> value. Persistit will never call this method
     * to decode a value that was <tt>null</tt> when written because null values
     * are handled by built-in encoding logic.
     * </p>
     * 
     * @param value
     *            The <tt>Value</tt> from which interior fields of the object
     *            are to be retrieved
     * 
     * @param target
     *            The <tt>StringBuilder</tt> into which the decoded value is to
     *            be written
     * 
     * @param clazz
     *            The class of the object that was originally encoded into
     *            Value.
     * 
     * @param context
     *            An arbitrary object that can optionally be supplied by the
     *            application to convey an application-specific context for the
     *            operation. (See {@link CoderContext}.) The default value is
     *            <tt>null</tt>.
     * 
     * @throws ConversionException
     */
    public void display(Value value, StringBuilder target, Class clazz,
            CoderContext context) throws ConversionException {
        if (Map.class.isAssignableFrom(clazz)) {
            target.append('[');
            boolean first = true;
            for (;;) {
                if (!value.hasMoreItems()) {
                    break;
                }

                if (first)
                    first = false;
                else
                    target.append(',');

                value.decodeDisplayable(true, target);
                target.append("->");

                if (!value.hasMoreItems()) {
                    throw new ConversionException(
                            "Encoded Map entry has missing value");
                }

                value.decodeDisplayable(true, target);
            }
            target.append(']');
        } else if (Collection.class.isAssignableFrom(clazz)) {
            target.append('[');
            boolean first = true;

            for (;;) {
                if (!value.hasMoreItems()) {
                    break;
                }

                if (first)
                    first = false;
                else
                    target.append(',');

                value.decodeDisplayable(true, target);
            }
            target.append(']');
        } else
            throw new ConversionException(
                    "Cannot display value that is neither a Map nor a Collection");
    }

}
