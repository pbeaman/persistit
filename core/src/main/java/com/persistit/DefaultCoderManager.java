/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.persistit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Stack;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import com.persistit.encoding.CoderManager;
import com.persistit.encoding.CollectionValueCoder;
import com.persistit.encoding.KeyCoder;
import com.persistit.encoding.SerialValueCoder;
import com.persistit.encoding.ValueCoder;

/**
 * <p>
 * A simple <code>CoderManager</code> implementation that associates each
 * registered <code>Class</code> with its corresponding {@link KeyCoder} or
 * {@link ValueCoder}. See {@link CoderManager} for more information.
 * </p>
 * <p>
 * This implementation builds a <code>HashMap</code> to associate each
 * registered <code>Class</code> with its <code>KeyCoder</code> or
 * <code>ValueCoder</code>. Registering a class <code>MyClass</code> has no
 * effect on the encoding or decoding of subclasses of <code>MyClass</code>.
 * Each subclass needs to be registered separately, even if the
 * <code>ValueCoder</code> or <code>KeyCoder</code> is the same for each
 * subclass. A custom <code>CoderManager</code> implementation could modify this
 * behavior.
 * </p>
 * 
 * @version 1.0
 */
public final class DefaultCoderManager implements CoderManager {
    private static final Class[] DEFAULT_CLASSES = new Class[] { ArrayList.class, LinkedList.class, Stack.class,
            Vector.class, Properties.class, HashMap.class, HashSet.class, Hashtable.class, TreeMap.class,
            TreeSet.class, };

    private static final String[] DEFAULT_14_CLASSES = new String[] { "java.util.LinkedHashMap",
            "java.util.LinkedHashSet", };

    private static final String ENUM_CLASS_NAME = "java.lang.Enum";

    private final static String ENUM_VALUE_CODER_CLASS_NAME = "com.persistit.encoding.EnumValueCoder";

    private static Class ENUM_CLASS = null;
    // Since an EnumValueCoder has no state, there needs to be only one
    // instance.
    private static ValueCoder ENUM_VALUE_CODER = null;

    static {
        try {
            ENUM_CLASS = Class.forName(ENUM_CLASS_NAME);
            ENUM_VALUE_CODER = (ValueCoder) Class.forName(ENUM_VALUE_CODER_CLASS_NAME).newInstance();
        } catch (ClassNotFoundException cnfe) {
        } catch (InstantiationException ie) {
        } catch (IllegalAccessException oae) {
        }
    }

    private final Persistit _persistit;
    private HashMap _keyCodersByClass = new HashMap();
    private HashMap _valueCodersByClass = new HashMap();
    private ClassSelector[] _serialOverrides = new ClassSelector[0];

    /**
     * Construct a new <code>DefaultCoderManager</code>, using the
     * <code>serialOverride</code> system configuration property to determine
     * which classes, if any, must employ standard Java serialization.
     * 
     * @see #DefaultCoderManager(String)
     * @throws IllegalArgumentException
     *             if the format of the <code>serialOverride</code> property is
     *             invalid
     */
    public DefaultCoderManager(Persistit persistit) {
        this(persistit, persistit.getProperty(Persistit.SERIAL_OVERRIDE_PROPERTY));
    }

    /**
     * Construct a new <code>DefaultCoderManager</code> that uses standard Java
     * serialization for any class whose name conforms to the supplied
     * <code>patterns</code> string. See {@link #getSerialOverridePatterns()}
     * for details.
     * 
     * @param patterns
     *            Specifies class names of classes that always use standard Java
     *            serialization rather a custom <code>ValueCoder</code>
     *            implementation.
     * 
     * @throws IllegalArgumentException
     *             if the format of the supplied <code>patterns</code> string is
     *             invalid
     */
    public DefaultCoderManager(Persistit persistit, String patterns) {
        _persistit = persistit;
        if (patterns == null) {
            if (_serialOverrides == null || _serialOverrides.length != 0) {
                _serialOverrides = new ClassSelector[0];
            }
        } else {
            int count = 0;
            if (patterns.length() > 0)
                count = 1;
            for (int p = -1; (p = patterns.indexOf(',', p + 1)) != -1;) {
                count++;
            }
            int q = 0;
            ClassSelector[] overrides = new ClassSelector[count];
            for (int index = 0; index < count; index++) {
                int p = patterns.indexOf(',', q);
                if (p == -1)
                    p = patterns.length();
                overrides[index] = new ClassSelector(patterns.substring(q, p));
                q = p + 1;
            }
            _serialOverrides = overrides;
            for (Iterator iter = _valueCodersByClass.keySet().iterator(); iter.hasNext();) {
                Class clazz = (Class) iter.next();
                if (isSerialOverride(clazz)) {
                    iter.remove();
                }
            }
        }
        registerDefaultCoveredClasses();
    }

    /**
     * Returns the parent <code>CoderManager</code>.
     * 
     * @return The parent
     */
    @Override
    public CoderManager getParent() {
        return null;
    }

    /**
     * Associates a {@link KeyCoder} with a <code>Class</code> that it will
     * encode.
     * 
     * @param clazz
     *            The <code>Class</code>
     * @param coder
     *            The <code>KeyCoder</code>
     */
    @Override
    public synchronized KeyCoder registerKeyCoder(Class clazz, KeyCoder coder) {
        return (KeyCoder) _keyCodersByClass.put(clazz, coder);
    }

    /**
     * Removes any <code>KeyCoder</code> registered for the supplied
     * <code>Class</code>
     * 
     * @param clazz
     *            The <code>Class</code> that will no longer have a
     *            <code>KeyCoder</code>
     * @return The removed <code>KeyCoder</code>, or <code>null</code> if there
     *         was none.
     */
    @Override
    public synchronized KeyCoder unregisterKeyCoder(Class clazz) {
        return (KeyCoder) _keyCodersByClass.remove(clazz);
    }

    /**
     * Unregisters the supplied <code>KeyCoder</code> from all
     * <code>Class</code>es it was previously registered to handle.
     * 
     * @param coder
     *            The <code>KeyCoder</code>
     * @return The count of <code>Class</code>es for which this
     *         <code>KeyCoder</code> was previously registered.
     */
    @Override
    public synchronized int unregisterKeyCoder(KeyCoder coder) {
        int count = 0;
        for (Iterator iter = _keyCodersByClass.keySet().iterator(); iter.hasNext();) {
            Class clazz = (Class) iter.next();
            if (coder.equals(_keyCodersByClass.get(clazz))) {
                iter.remove();
                count++;
            }
        }
        return count;
    }

    private final static class ClassComparator implements Comparator {
        public boolean equals(Object o1, Object o2) {
            return o1.equals(o2);
        }

        @Override
        public int compare(Object o1, Object o2) {
            Class c1 = (Class) o1;
            Class c2 = (Class) o2;
            return c1.getName().compareTo(c2.getName());
        }
    }

    /**
     * Create a <code>Map</code> of <code>Class</code>es to registered
     * <code>ValueCoder</code>s. The map is an unmodifiable, is sorted by class
     * name, and does not change with subsequent registrations or
     * unregistrations of <code>ValueCoder</code>s.
     * 
     * @return <code>Map</code> of <code>Class</code>es to
     *         <code>ValueCoder</code>s.
     */
    @Override
    public Map getRegisteredValueCoders() {
        Map newMap = new TreeMap(new ClassComparator());
        newMap.putAll(_valueCodersByClass);
        return Collections.unmodifiableMap(newMap);
    }

    /**
     * Create a <code>Map</code> of <code>Class</code>es to registered
     * <code>KeyCoder</code> s. The map is an unmodifiable, and is sorted by
     * class name, and does not change with subsequent registrations or
     * unregistrations of <code>KeyCoder</code>s.
     * 
     * @return <code>Map</code> of <code>Class</code>es to <code>KeyCoder</code>
     *         s.
     */
    @Override
    public Map getRegisteredKeyCoders() {
        Map newMap = new TreeMap(new ClassComparator());
        newMap.putAll(_keyCodersByClass);
        return Collections.unmodifiableMap(newMap);
    }

    /**
     * Returns the <code>KeyCoder</code> registered for the supplied
     * <code>Class</code>, or <code>null</code> if no <code>KeyCoder</code> is
     * registered.
     * 
     * @param clazz
     *            The <code>Class</code>
     * @return The <code>KeyCoder</code> or <code>null</code> if there is none.
     */
    @Override
    public synchronized KeyCoder lookupKeyCoder(Class clazz) {
        return (KeyCoder) _keyCodersByClass.get(clazz);
    }

    /**
     * Attempt to register the provided <code>ValueCoder</code> to encode and
     * decode instances of supplied <code>Class</code>. If the class is included
     * in the serial override set (see {@link #getSerialOverridePatterns()} and
     * if the <code>ValueCoder</code> is not an instance of
     * {@link SerialValueCoder}, the request is ignored.
     * 
     * @param clazz
     *            The Class for which this <code>ValueCoder</code> will provide
     *            encoding and decoding services
     * 
     * @param coder
     *            The ValueCoder to register
     * 
     * @return The <code>ValueCoder</code> that was removed, or
     *         <code>null</code> if there was none.
     */
    @Override
    public synchronized ValueCoder registerValueCoder(Class clazz, ValueCoder coder) {
        return (ValueCoder) _valueCodersByClass.put(clazz, coder);
    }

    /**
     * Removes any <code>ValueCoder</code> registered for the supplied
     * <code>Class</code>
     * 
     * @param clazz
     *            The <code>Class</code> that will no longer have a
     *            <code>ValueCoder</code>
     * @return The removed <code>ValueCoder</code>, or <code>null</code> if
     *         there was none.
     */
    @Override
    public synchronized ValueCoder unregisterValueCoder(Class clazz) {
        return (ValueCoder) _valueCodersByClass.remove(clazz);
    }

    /**
     * Unregisters the supplied <code>ValueCoder</code> from all
     * <code>Class</code>es it was previously registered to handle.
     * 
     * @param coder
     *            The <code>ValueCoder</code>
     * @return The count of <code>Class</code>es for which this
     *         <code>ValueCoder</code> was previously registered.
     */
    @Override
    public synchronized int unregisterValueCoder(ValueCoder coder) {
        int count = 0;
        for (Iterator iter = _valueCodersByClass.keySet().iterator(); iter.hasNext();) {
            Class clazz = (Class) iter.next();
            if (coder.equals(_valueCodersByClass.get(clazz))) {
                iter.remove();
                count++;
            }
        }
        return count;
    }

    /**
     * Return the <code>ValueCoder</code> registered for the supplied
     * <code>Class</code> , or <code>null</code> if no <code>ValueCoder</code>
     * is registered.
     * 
     * @param clazz
     *            The <code>Class</code>
     * @return The <code>ValueCoder</code> or <code>null</code> if there is
     *         none.
     */
    @Override
    public synchronized ValueCoder lookupValueCoder(Class clazz) {
        return (ValueCoder) _valueCodersByClass.get(clazz);
    }

    /**
     * Return a <code>ValueCoder</code> for the supplied <code>Class</code>. If
     * there is none registered, implicitly create either a
     * {@link DefaultValueCoder} or a {@link SerialValueCoder} and register it.
     * 
     * @param clazz
     *            The class for which a <code>ValueCoder</code> is needed
     * @return The <code>ValueCoder</code>
     */
    @Override
    public synchronized ValueCoder getValueCoder(Class clazz) {
        ValueCoder coder = lookupValueCoder(clazz);
        if (coder == null) {
            boolean serialOverride = isSerialOverride(clazz);
            if (serialOverride) {
                coder = new SerialValueCoder(clazz);
            } else if (ENUM_CLASS != null && ENUM_VALUE_CODER != null && ENUM_CLASS.isAssignableFrom(clazz)) {
                coder = ENUM_VALUE_CODER;
            } else {
                coder = new DefaultValueCoder(_persistit, clazz);
            }
            registerValueCoder(clazz, coder);
        }
        return coder;
    }

    /**
     * <p>
     * Returns the serialization override pattern specified by the
     * <code>serialOverride</code> property. The pattern specifies a collection
     * of names of classes that are required to be serialized by standard Java
     * serialization rather than by a default or custom <code>ValueCoder</code>.
     * </p>
     * <p>
     * The result is a comma-delimited sequence of explicit class names and/or
     * partial names that include wildcards. There are two wildcards: "**"
     * standard for any character sequence, while "*" stands for any character
     * sequence that having no periods. For example, the string
     * 
     * <pre>
     * <code>
     *      java.util.ArrayList,java.util.*Map,java.lang.*,javax.swing.**
     * </code>
     * </pre>
     * 
     * includes <code>java.util.ArrayList</code>,
     * <code>java.util.AbstractMap</code>, <code>java.util.HashMap</code>, etc.,
     * all the classs in the <code>java.lang</code> package (but not
     * <code>java.lang.ref</code> or <code>java.lang.reflect</code>), and all
     * classes in <code>javax.swing</code> and all its subpackages.
     * </p>
     * 
     * @return The serialization override pattern
     */
    public synchronized String getSerialOverridePatterns() {
        StringBuilder sb = new StringBuilder();
        for (int index = 0; index < _serialOverrides.length; index++) {
            if (sb.length() > 0)
                sb.append(',');
            sb.append(_serialOverrides[index]);
        }
        return sb.toString();
    }

    /**
     * Tests whether the supplied class is included in the serial override set.
     * This is a set of classes specified by the <code>serialOverride</code>
     * property that are always serialized by standard Java serialization. Any
     * attempt to register a custom <code>ValueCoder</code> or a
     * <code>DefaultObjectCoder</code> for such a class will be ignored.
     * 
     * @see #getSerialOverridePatterns()
     * 
     * @param clazz
     *            The class to test
     * 
     * @return <code>true</code> if and only if the specified class is selected
     *         by the <code>serialOverride</code> property
     */
    public boolean isSerialOverride(Class clazz) {
        while (Serializable.class.isAssignableFrom(clazz)) {
            String className = clazz.getName();
            for (int index = 0; index < _serialOverrides.length; index++) {
                if (_serialOverrides[index].isSelected(className))
                    return true;
            }
            clazz = clazz.getSuperclass();
        }
        return false;
    }

    /**
     * Registers <code>ValueCoder</code> implementations for selected
     * collections classes in the Java API. These are somewhat faster and more
     * space-efficient than default java serialization of the same classes. This
     * method is called by Persistit during initialization.
     */
    private void registerDefaultCoveredClasses() {
        ValueCoder coder = new CollectionValueCoder();

        for (int index = 0; index < DEFAULT_CLASSES.length; index++) {
            Class clazz = DEFAULT_CLASSES[index];
            if (!isSerialOverride(clazz)) {
                registerValueCoder(DEFAULT_CLASSES[index], coder);
            }
        }
        for (int index = 0; index < DEFAULT_14_CLASSES.length; index++) {
            try {
                Class clazz = Class.forName(DEFAULT_14_CLASSES[index]);
                if (!isSerialOverride(clazz)) {
                    registerValueCoder(clazz, coder);
                }
            } catch (Exception e) {
                // ignore if we are below JDK 1.4.
            }
        }
    }

    private static class ClassSelector {
        private String _startsWith;
        private String _endsWith;
        private boolean _exact;
        private boolean _multiPath;

        private ClassSelector(String pattern) {
            int p = pattern.indexOf('*');
            int q = pattern.lastIndexOf('*');
            if (p == -1) {
                _startsWith = pattern;
                _endsWith = "";
                _exact = true;
            } else if (q - p <= 1) {
                _startsWith = pattern.substring(0, p);
                _endsWith = pattern.substring(q + 1);
                _exact = false;
                _multiPath = q > p;
            } else {
                throw new IllegalArgumentException("Class selector pattern must be in the form "
                        + " xxx, xxx*yyy or xxx**yyy");
            }
        }

        private boolean isSelected(String className) {
            if (!className.startsWith(_startsWith) || !className.endsWith(_endsWith)) {
                return false;
            }
            int p = _startsWith.length();
            int q = className.length() - _endsWith.length();
            if (p > q || (_exact && p < q)) {
                return false;
            }
            for (int index = p; index < q; index++) {
                char c = className.charAt(index);
                if (c == '.') {
                    if (!_multiPath)
                        return false;
                } else {
                    if (!Character.isJavaIdentifierPart(c))
                        return false;
                }
            }
            return true;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(_startsWith);
            if (!_exact) {
                sb.append('*');
                if (_multiPath)
                    sb.append('*');
                sb.append(_endsWith);
            }
            return sb.toString();
        }

    }
}
