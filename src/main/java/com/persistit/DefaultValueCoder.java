/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.persistit;

import java.io.Externalizable;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.ObjectStreamField;
import java.io.Serializable;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import com.persistit.encoding.CoderContext;
import com.persistit.encoding.CoderManager;
import com.persistit.encoding.HandleCache;
import com.persistit.encoding.ValueCoder;
import com.persistit.encoding.ValueRenderer;
import com.persistit.exception.ConversionException;

/**
 * <p>
 * A {@link ValueCoder} that uses reflection to access and modify the fields of
 * an object. This implementation provides the default serialization mechanism
 * for Persistit version 1.1. See <a
 * href="../../../Object_Serialization_Notes.html"> Persistit JSA 1.1 Object
 * Serialization</a> for details.
 * </p>
 * <p>
 * <code>DefaultValueCoder</code> may only be used to serialize and deserialize
 * serializable objects (i.e., instances of classes that implement
 * <code>java.io.Serializable</code>). To the extent possible, the semantics of
 * serialization within this <code>DefaultValueCoder</code> match the <a href=
 * "http://java.sun.com/j2se/1.5.0/docs/guide/serialization/spec/serial-arch.html"
 * > Java Object Serialization Specification</a>.
 * </p>
 * </p> In particular, for Serializable classes, objects are constructed using
 * the no-argument constructor of the nearest non-serializable superclass, as
 * required by the specification. (This behavior can be modified with the
 * <code>constructorOverride</code> system property.) To do so, this
 * implementation invokes a platform-specific, non-public API method of
 * <code>java.io.ObjectStreamClass</code> or
 * </code>java.io.ObjectInputStream</code>. </p>
 * <p>
 * This implementation invokes the <code>readExternal</code> and
 * <code>writeExternal</code> methods of <code>Externalizable</code> classes,
 * and the <code>readObject</code>, <code>writeObject</code>,
 * <code>readResolve</code> and <code>writeReplace</code> methods of
 * <code>Serializable</code> classes as required by the specification. A special
 * extended <code>ObjectInputStream</code> or <code>ObjectOutputStream</code>
 * implementation is provided when necessary. Although the semantics of the
 * specification are followed, the format of serialized data is optimized for
 * Persistit and does not conform to the specification.
 * </p>
 * <p>
 * Currently the <code>readObjectNoData</code> method, the <code>readLine</code>
 * method, and the <code>PutField</code>/<code>GetField</code> API elements are
 * not implemented by <code>DefaultValueCoder</code>.
 * </p>
 * 
 * @since 1.1
 * @version 1.1
 */
public class DefaultValueCoder implements ValueRenderer, HandleCache {
    private final static Object[] EMPTY_OBJECT_ARRAY = {};
    private final static Class[] EMPTY_CLASS_ARRAY = {};
    private final static Class[] OOS_CLASS_ARRAY = { ObjectOutputStream.class };
    private final static Class[] OIS_CLASS_ARRAY = { ObjectInputStream.class };

    private final static Comparator FIELD_COMPARATOR = new Comparator() {
        @Override
        public int compare(final Object o1, final Object o2) {
            final Field f1 = (Field) o1;
            final Field f2 = (Field) o2;
            if (f1.getType().isPrimitive() && !f2.getType().isPrimitive()) {
                return -1;
            }
            return f1.getName().compareTo(f2.getName());
        }
    };

    private Class _clazz;
    private Persistit _persistit;
    private boolean _serializable;
    private boolean _externalizable;

    private ObjectStreamClass _classDescriptor;
    private Builder _valueBuilder;

    private Method _readResolveMethod = null;
    private Method _writeReplaceMethod = null;
    private Method _readObjectMethod = null;
    private Method _writeObjectMethod = null;

    private ValueRenderer _superClassValueRenderer = null;

    private final static String GET_14_NEW_INSTANCE_METHOD_NAME = "newInstance";

    private final static Class[] GET_14_NEW_INSTANCE_METHOD_TYPES = EMPTY_CLASS_ARRAY;

    private final static String GET_13_NEW_INSTANCE_METHOD_NAME = "allocateNewObject";

    private final static Class[] GET_13_NEW_INSTANCE_METHOD_TYPES = { Class.class, Class.class, };

    private Method _newInstanceMethod;
    private Object[] _newInstanceArguments;
    private Constructor _newInstanceConstructor;
    private volatile int _handle;

    /**
     * <p>
     * Contructs a DefaultValueCoder for the specified <code>clientClass</code>.
     * The resulting coder is capable of efficiently serializing and
     * deserializing instances of the class in Persistit records.
     * </p>
     * <p>
     * The resulting <code>DefaultValueCoder</code> serializes and deserializes
     * the fields defined by standard Java serialization (see the <a href=
     * "http://java.sun.com/j2se/1.5.0/docs/guide/serialization/spec/serial-arch.html"
     * > Java Object Serialization Specification</a>. If there is a final static
     * field <code>serialPersistitFields</code> then it defines the fields to be
     * serialized. Otherwise all non-static, non-transient fields, including
     * those defined in superclasses, are included.
     * </p>
     * 
     * @param clientClass
     *            A (<code>java.io.Serializable</code>) <code>Class</code> of
     *            objects to be serialized and deserialized by this
     *            <code>DefaultValueCoder</code>
     * 
     * @throws SecurityException
     *             if permission to make a non-public field accessible is not
     *             granted by the current security policy
     * 
     * @throws ConversionException
     *             if the <code>clientClass</code> does not implement
     *             <code>java.io.Serializable</code>, or if the attempt to find
     *             an appropriate method for constructing deserialized objects
     *             fails.
     */
    public DefaultValueCoder(final Persistit persistit, final Class clientClass) throws SecurityException {
        init(persistit, clientClass, true);

        try {
            AccessController.doPrivileged(new PrivilegedExceptionAction() {
                @Override
                public Object run() {
                    List list;

                    if (_classDescriptor != null) {
                        final ObjectStreamField[] osFields = _classDescriptor.getFields();

                        //
                        // The Sun implementation has already sorted this
                        // array to conform to serialization order, but the
                        // sort order is not defined by the spec so we'll
                        // just do it again to be sure this works on all VMs.
                        //
                        Arrays.sort(osFields);
                        list = new ArrayList(osFields.length);
                        for (int index = 0; index < osFields.length; index++) {
                            Field field;
                            final String name = osFields[index].getName();
                            try {
                                field = clientClass.getDeclaredField(name);
                                list.add(field);
                            } catch (final NoSuchFieldException nsfe) {
                                throw new ConversionException(clientClass + " unmatched serializable field '" + name
                                        + "' declared");
                            }
                        }
                    } else {
                        final Field[] fields = clientClass.getDeclaredFields();
                        Arrays.sort(fields, FIELD_COMPARATOR);
                        list = new ArrayList(fields.length);

                        for (int index = 0; index < fields.length; index++) {
                            final int modifier = fields[index].getModifiers();
                            if (!Modifier.isTransient(modifier) && !Modifier.isStatic(modifier)) {
                                list.add(fields[index]);
                            }
                        }
                    }
                    final Field[] fields = (Field[]) list.toArray(new Field[list.size()]);

                    _valueBuilder = new Builder("value", fields, clientClass);
                    lookupDefaultConstructor(persistit.getConfiguration().isConstructorOverride());
                    lookupSerializationMethods();
                    return null;
                }
            });
        } catch (final PrivilegedActionException pae) {
            throw (RuntimeException) pae.getException();
        }
    }

    /**
     * <p>
     * Contructs a DefaultValueCoder for the specified <code>clientClass</code>.
     * The resulting coder is capable of efficiently serializing and
     * deserializing instances of the class in Persistit records. The resulting
     * <code>DefaultValueCoder</code> serializes and deserializes only the
     * specified <code>fields</code>.
     * </p>
     * 
     * @param clientClass
     *            A (<code>java.io.Serializable</code>) <code>Class</code> of
     *            objects to be serialized and deserialized by this
     *            <code>DefaultValueCoder</code>
     * 
     * @param fields
     *            An array of Fields of this class to serialize and deserialize.
     * 
     * @throws SecurityException
     *             if permission to make a non-public field accessible is not
     *             granted by the current security policy
     * 
     * @throws ConversionException
     *             if the <code>clientClass</code> does not implement
     *             <code>java.io.Serializable</code>, or if the attempt to find
     *             an appropriate method for constructing deserialized objects
     *             fails.
     */
    public DefaultValueCoder(final Persistit persistit, final Class clientClass, final Field[] fields) {
        init(persistit, clientClass, false);
        try {
            AccessController.doPrivileged(new PrivilegedExceptionAction() {
                @Override
                public Object run() {
                    _valueBuilder = new Builder("value", fields, clientClass);
                    lookupDefaultConstructor(persistit.getConfiguration().isConstructorOverride());
                    lookupSerializationMethods();
                    return null;
                }
            });
        } catch (final PrivilegedActionException pae) {
            throw (RuntimeException) pae.getException();
        }
    }

    DefaultValueCoder(final Persistit persistit, final Class clientClass, final Builder valueBuilder) {
        init(persistit, clientClass, false);

        try {
            AccessController.doPrivileged(new PrivilegedExceptionAction() {
                @Override
                public Object run() {
                    _valueBuilder = valueBuilder;
                    lookupDefaultConstructor(true);
                    return null;
                }
            });
        } catch (final PrivilegedActionException pae) {
            throw (RuntimeException) pae.getException();
        }
    }

    /**
     * Performs unprivileged initialization logic common to both constructors.
     * 
     * @param clientClass
     * @param mustBeSerializable
     */
    private void init(final Persistit persistit, final Class clientClass, final boolean mustBeSerializable) {
        _clazz = clientClass;
        _persistit = persistit;
        _serializable = Serializable.class.isAssignableFrom(clientClass);
        if (_serializable) {
            _externalizable = Externalizable.class.isAssignableFrom(clientClass);
            _classDescriptor = ObjectStreamClass.lookup(_clazz);
        } else if (mustBeSerializable) {
            throw new ConversionException("Not Serializable: " + clientClass.getName());
        }

        final Class superClass = clientClass.getSuperclass();
        if (superClass != null && Serializable.class.isAssignableFrom(superClass)) {
            ValueCoder coder = null;
            final CoderManager cm = _persistit.getCoderManager();
            if (cm != null) {
                coder = cm.lookupValueCoder(superClass);
            }
            if (!(coder instanceof DefaultValueCoder)) {
                coder = new DefaultValueCoder(persistit, superClass);
            }
            if (coder instanceof ValueRenderer) {
                _superClassValueRenderer = (ValueRenderer) coder;
            }
        }
    }

    private void lookupDefaultConstructor(final boolean constructorOverride) {
        AccessController.doPrivileged(new PrivilegedAction() {
            @Override
            public Object run() {
                if (_externalizable || !_serializable || constructorOverride) {
                    Constructor constructor = null;
                    try {
                        constructor = _clazz.getDeclaredConstructor(EMPTY_CLASS_ARRAY);
                    } catch (final NoSuchMethodException nsme) {
                    }
                    if (_externalizable && (constructor == null || !Modifier.isPublic(constructor.getModifiers()))) {
                        throw new ConversionException("Externalizable class " + _clazz.getName()
                                + " requires a public no-argument constructor");
                    } else if (constructor == null) {
                        throw new ConversionException("Class " + _clazz.getName()
                                + " requires a no-argument constructor");
                    }
                    constructor.setAccessible(true);
                    _newInstanceConstructor = constructor;
                } else {
                    _newInstanceMethod = null;

                    try {
                        _newInstanceMethod = ObjectStreamClass.class.getDeclaredMethod(GET_14_NEW_INSTANCE_METHOD_NAME,
                                GET_14_NEW_INSTANCE_METHOD_TYPES);
                        _newInstanceArguments = EMPTY_OBJECT_ARRAY;
                    } catch (final NoSuchMethodException nsme) {
                        Class nonSerializableSuperclass = _clazz;
                        while (nonSerializableSuperclass != null
                                && Serializable.class.isAssignableFrom(nonSerializableSuperclass)) {
                            nonSerializableSuperclass = nonSerializableSuperclass.getSuperclass();
                        }
                        try {
                            _newInstanceMethod = ObjectInputStream.class.getDeclaredMethod(
                                    GET_13_NEW_INSTANCE_METHOD_NAME, GET_13_NEW_INSTANCE_METHOD_TYPES);
                            _newInstanceArguments = new Class[] { _clazz, nonSerializableSuperclass, };
                        } catch (final NoSuchMethodException nsme2) {
                        }
                    }
                    if (_newInstanceMethod != null) {
                        _newInstanceMethod.setAccessible(true);
                    } else {
                        throw new UnsupportedOperationException("Unable to find serialization constructor "
                                + "method for class " + _clazz.getName());
                    }
                }
                return null;
            }
        });
    }

    /**
     * Return the <code>class</code> that this <code>ObjectCoder</code> serves.
     * 
     * @return The <code>class</code>
     */
    public Class getClientClass() {
        return _clazz;
    }

    private static Accessor lookupAccessor(final Class clazz, final String name) {
        String baseName;

        final char ch = name.charAt(0);
        if (Character.isLetter(ch) && Character.isLowerCase(ch)) {
            baseName = new Character(Character.toUpperCase(ch)) + name.substring(1);
        } else {
            baseName = name;
        }

        Method getMethod = null;
        Method setMethod = null;
        final Method[] methods = clazz.getMethods();
        //
        // First search for an appropriate accessor. Pattern is like a bean:
        // getFoo or isFoo; isFoo is a valid accessor only if its return
        // type is boolean.
        //
        for (int index = 0; index < methods.length; index++) {
            final Method m = methods[index];
            final String n = m.getName();

            if (n.startsWith("get") && n.regionMatches(3, baseName, 0, baseName.length())
                    && m.getParameterTypes().length == 0 && m.getReturnType() != Void.class
                    && Modifier.isPublic(m.getModifiers()) && !Modifier.isStatic(m.getModifiers())
                    && !Modifier.isAbstract(m.getModifiers())) {
                getMethod = m;
            }

            else if (n.startsWith("is") && n.regionMatches(2, baseName, 0, baseName.length()) && getMethod == null
                    && m.getReturnType() == boolean.class && m.getParameterTypes().length == 1
                    && Modifier.isPublic(m.getModifiers()) && !Modifier.isStatic(m.getModifiers())
                    && !Modifier.isAbstract(m.getModifiers())) {
                getMethod = m;
            }
        }
        //
        // Now look for an appropriate setter. We want one that takes the
        // same type or more general type than the getter, which is why we
        // search for this after resolving the getter. We will choose the
        // most general setter that matches the getter's type.
        //
        if (getMethod != null) {
            for (int index = 0; index < methods.length; index++) {
                final Method m = methods[index];
                final String n = m.getName();

                if (n.startsWith("set") && n.regionMatches(3, baseName, 0, baseName.length())
                        && m.getParameterTypes().length == 1 && Modifier.isPublic(m.getModifiers())
                        && !Modifier.isStatic(m.getModifiers()) && !Modifier.isAbstract(m.getModifiers())) {
                    final Class c = m.getParameterTypes()[0];
                    if (m.getReturnType() == Void.TYPE && c == getMethod.getReturnType()) {
                        setMethod = m;
                    }
                }
            }
        }

        if (getMethod != null && setMethod != null) {
            return new PropertyAccessor(getMethod, setMethod);
        }

        Class c = clazz;
        while (c != null) {
            final Field field = lookupField(name, c);
            if (field != null && !Modifier.isTransient(field.getModifiers())
                    && !Modifier.isStatic(field.getModifiers())) {
                return accessorInstance(field);
            }
            c = c.getSuperclass();
        }

        throw new IllegalArgumentException("Class " + clazz.getName() + " has no accessible field or property named "
                + name);
    }

    private static Field lookupField(final String name, final Class clazz) {
        try {
            return clazz.getDeclaredField(name);
        } catch (final NoSuchFieldException nsfe) {
            return null;
        }
    }

    private static Accessor accessorInstance(final Field field) {
        Accessor accessor = null;
        if (field == null) {
            accessor = new NoFieldAccessor();
        } else if (field.getType().isPrimitive()) {
            if (field.getType() == boolean.class) {
                accessor = new BooleanFieldAccessor();
            } else if (field.getType() == byte.class) {
                accessor = new ByteFieldAccessor();
            } else if (field.getType() == short.class) {
                accessor = new ShortFieldAccessor();
            } else if (field.getType() == char.class) {
                accessor = new CharFieldAccessor();
            } else if (field.getType() == int.class) {
                accessor = new IntFieldAccessor();
            } else if (field.getType() == long.class) {
                accessor = new LongFieldAccessor();
            } else if (field.getType() == float.class) {
                accessor = new FloatFieldAccessor();
            } else if (field.getType() == double.class) {
                accessor = new DoubleFieldAccessor();
            } else {
                accessor = new ObjectFieldAccessor();
            }
        } else {
            accessor = new ObjectFieldAccessor();
        }
        accessor._field = field;
        return accessor;
    }

    static abstract class Accessor {
        Field _field;

        @Override
        public String toString() {
            return "Accessor[" + _field.getName() + "]";
        }

        void fromKey(final Object object, final Key key) throws Exception {
            final Object arg = key.decode();
            _field.set(object, arg);
        }

        void toKey(final Object object, final Key key) throws Exception {
            final Object arg = _field.get(object);
            key.append(arg);
        }

        abstract void fromValue(Object object, Value value) throws Exception;

        abstract void toValue(Object object, Value value) throws Exception;

        protected void cantModifyFinalField() {
            throw new ConversionException("Can not modify final field " + _field.getName());
        }
    }

    private static class PropertyAccessor extends Accessor {
        Method _getMethod;
        Method _setMethod;

        private PropertyAccessor(final Method getMethod, final Method setMethod) {
            _getMethod = getMethod;
            _setMethod = setMethod;
        }

        @Override
        public String toString() {
            return "Accessor[" + _getMethod.getName() + "/" + _setMethod.getName() + "]";
        }

        @Override
        void fromKey(final Object object, final Key key) throws Exception {
            final Object arg = key.decode();
            _setMethod.invoke(object, new Object[] { arg });
        }

        @Override
        void toKey(final Object object, final Key key) throws Exception {
            Object arg = null;
            arg = _getMethod.invoke(object, EMPTY_OBJECT_ARRAY);
            key.append(arg);
        }

        @Override
        void fromValue(final Object object, final Value value) throws Exception {
            final Object arg = value.get(null, null);
            _setMethod.invoke(object, new Object[] { arg });
        }

        @Override
        void toValue(final Object object, final Value value) throws Exception {
            final Object arg = _getMethod.invoke(object, EMPTY_OBJECT_ARRAY);
            value.put(arg);
        }
    }

    private static class ObjectFieldAccessor extends Accessor {
        @Override
        void fromValue(final Object object, final Value value) throws Exception {
            final Object arg = value.get(null, null);
            _field.set(object, arg);
        }

        @Override
        void toValue(final Object object, final Value value) throws Exception {
            final Object arg = _field.get(object);
            value.put(arg);
        }
    }

    private final static class BooleanFieldAccessor extends Accessor {
        @Override
        void toValue(final Object object, final Value value) throws Exception {
            value.put(_field.getBoolean(object));
        }

        @Override
        void fromValue(final Object object, final Value value) throws Exception {
            _field.setBoolean(object, value.getBoolean());
        }
    }

    private final static class ByteFieldAccessor extends Accessor {
        @Override
        void toValue(final Object object, final Value value) throws Exception {
            value.put(_field.getByte(object));
        }

        @Override
        void fromValue(final Object object, final Value value) throws Exception {
            _field.setByte(object, value.getByte());
        }
    }

    private final static class ShortFieldAccessor extends Accessor {
        @Override
        void toValue(final Object object, final Value value) throws Exception {
            value.put(_field.getShort(object));
        }

        @Override
        void fromValue(final Object object, final Value value) throws Exception {
            _field.setShort(object, value.getShort());
        }
    }

    private final static class CharFieldAccessor extends Accessor {
        @Override
        void toValue(final Object object, final Value value) throws Exception {
            value.put(_field.getChar(object));
        }

        @Override
        void fromValue(final Object object, final Value value) throws Exception {
            _field.setChar(object, value.getChar());
        }
    }

    private final static class IntFieldAccessor extends Accessor {
        @Override
        void toValue(final Object object, final Value value) throws Exception {
            value.put(_field.getInt(object));
        }

        @Override
        void fromValue(final Object object, final Value value) throws Exception {
            _field.setInt(object, value.getInt());
        }
    }

    private final static class LongFieldAccessor extends Accessor {
        @Override
        void toValue(final Object object, final Value value) throws Exception {
            value.put(_field.getLong(object));
        }

        @Override
        void fromValue(final Object object, final Value value) throws Exception {
            _field.setLong(object, value.getLong());
        }
    }

    private final static class FloatFieldAccessor extends Accessor {
        @Override
        void toValue(final Object object, final Value value) throws Exception {
            value.put(_field.getFloat(object));
        }

        @Override
        void fromValue(final Object object, final Value value) throws Exception {
            _field.setFloat(object, value.getFloat());
        }
    }

    private final static class DoubleFieldAccessor extends Accessor {
        @Override
        void toValue(final Object object, final Value value) throws Exception {
            value.put(_field.getDouble(object));
        }

        @Override
        void fromValue(final Object object, final Value value) throws Exception {
            _field.setDouble(object, value.getDouble());
        }
    }

    private final static class NoFieldAccessor extends Accessor {
        @Override
        void toValue(final Object object, final Value value) {
        }

        @Override
        void fromValue(final Object object, final Value value) {
        }
    }

    /**
     * <p>
     * A component of a <code>DefaultValueCoder</code> that reads and writes
     * data values to and from properties or fields of a client object.
     * </p>
     * <p>
     * Instances of this class implement <code>CoderContext</code> and therefore
     * may be supplied to the {@link Key#append(Object, CoderContext)} and
     * {@link Key#decode(Object, CoderContext)} methods.
     * 
     */
    public static class Builder implements CoderContext {
        private final static long serialVersionUID = 1;
        private final String _name;
        private final Accessor[] _accessors;
        private final String[] _accessorNames;

        Builder(final String name, final String[] accessorNames, final Class clazz) {
            _name = name;
            _accessorNames = new String[accessorNames.length];
            System.arraycopy(accessorNames, 0, _accessorNames, 0, accessorNames.length);
            _accessors = new Accessor[accessorNames.length];

            for (int index = 0; index < accessorNames.length; index++) {
                final String accessorName = accessorNames[index];
                final Accessor accessor = lookupAccessor(clazz, accessorName);
                _accessors[index] = accessor;
            }
            makeAccessorsAccessible(this);
        }

        Builder(final String name, final Field[] fields, final Class clazz) {
            _name = name;
            _accessorNames = new String[fields.length];
            _accessors = new Accessor[fields.length];

            for (int index = 0; index < fields.length; index++) {
                _accessorNames[index] = fields[index].getName();
                _accessors[index] = accessorInstance(fields[index]);
            }
            makeAccessorsAccessible(this);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder(_name);
            sb.append("[");
            for (int index = 0; index < _accessorNames.length; index++) {
                if (index > 0)
                    sb.append(",");
                sb.append(_accessorNames[index]);
                if (_accessors[index] instanceof PropertyAccessor)
                    sb.append("()");
            }
            sb.append("]");
            return sb.toString();
        }

        public String getName() {
            return _name;
        }

        public int getSize() {
            return _accessorNames.length;
        }

        Accessor getAccessor(final int index) {
            return _accessors[index];
        }

        public String getAccessorName(final int index) {
            return _accessorNames[index];
        }
    }

    private void lookupSerializationMethods() {
        if (_serializable) {
            _readResolveMethod = lookupInheritableMethod("readResolve", EMPTY_CLASS_ARRAY, Object.class);

            _writeReplaceMethod = lookupInheritableMethod("writeReplace", EMPTY_CLASS_ARRAY, Object.class);

            if (!_externalizable) {
                _readObjectMethod = lookupPrivateMethod("readObject", OIS_CLASS_ARRAY, void.class);

                _writeObjectMethod = lookupPrivateMethod("writeObject", OOS_CLASS_ARRAY, void.class);
            }
        }
    }

    private Method lookupPrivateMethod(final String name, final Class[] arguments, final Class returnType) {
        Method method = null;
        try {
            method = _clazz.getDeclaredMethod(name, arguments);
        } catch (final Exception e) {
        }
        if (method != null) {
            final int modifiers = method.getModifiers();
            if (method.getReturnType() == returnType && !Modifier.isStatic(modifiers)
                    && !Modifier.isAbstract(modifiers) && Modifier.isPrivate(modifiers)) {
                method.setAccessible(true);
                return method;
            }
        }
        return null;
    }

    private Method lookupInheritableMethod(final String name, final Class[] arguments, final Class returnType) {
        Class cl = _clazz;

        boolean privateOk = true;
        boolean packageOk = true;

        for (;;) {
            Method method = null;
            try {
                method = _clazz.getDeclaredMethod(name, arguments);
            } catch (final Exception e) {
            }
            if (method != null) {
                final int modifiers = method.getModifiers();
                if (method.getReturnType() == returnType
                        && !Modifier.isStatic(modifiers)
                        && !Modifier.isAbstract(modifiers)
                        && (Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers) || privateOk
                                && Modifier.isPrivate(modifiers) || packageOk && !Modifier.isPrivate(modifiers))) {
                    method.setAccessible(true);
                    return method;
                }
            }
            final Class scl = cl.getSuperclass();

            if (scl == null || scl == Object.class) {
                return null;
            }

            if (!packageEquals(cl, scl))
                packageOk = false;
            privateOk = false;
            cl = scl;
        }
    }

    /**
     * Return true if classes are defined in the same runtime package, false
     * otherwise. (From ObjectStreamClass)
     */
    private static boolean packageEquals(final Class cl1, final Class cl2) {
        return cl1.getClassLoader() == cl2.getClassLoader() && getPackageName(cl1).equals(getPackageName(cl2));
    }

    /**
     * Return package name of given class. (From ObjectStreamClass)
     */
    private static String getPackageName(final Class cl) {
        String s = cl.getName();
        int i = s.lastIndexOf('[');
        if (i >= 0) {
            s = s.substring(i + 2);
        }
        i = s.lastIndexOf('.');
        return (i >= 0) ? s.substring(0, i) : "";
    }

    private static void makeAccessorsAccessible(final Builder builder) {
        AccessController.doPrivileged(new PrivilegedAction() {
            @Override
            public Object run() {
                final ArrayList list = new ArrayList();

                final Accessor[] accessors = builder._accessors;

                for (int index = 0; index < accessors.length; index++) {
                    final Accessor a = accessors[index];

                    if (a instanceof PropertyAccessor) {
                        final PropertyAccessor pa = (PropertyAccessor) a;
                        if (pa._setMethod != null) {
                            list.add(pa._setMethod);
                        }
                        if (pa._getMethod != null) {
                            list.add(pa._getMethod);
                        }
                    } else {
                        list.add(a._field);
                    }
                }

                //
                // Make this call with the accumulated Fields, Methods and
                // Constructor
                // because it's faster: the code only needs to check privilege
                // once.
                //
                if (list.size() > 0) {
                    AccessibleObject.setAccessible(
                            (AccessibleObject[]) list.toArray(new AccessibleObject[list.size()]), true);
                }
                return null;
            }
        });
    }

    /**
     * <p>
     * Encodes the supplied <code>Object</code> into the supplied
     * <code>Value</code>. This method will be called only if this
     * <code>ValueCoder</code> has been registered with the current
     * {@link CoderManager} to encode objects having the class of the supplied
     * object.
     * </p>
     * <p>
     * Upon completion of this method, the backing byte array of the
     * <code>Value</code> and its size should be updated to reflect the
     * serialized object. Use the methods {@link Value#getEncodedBytes},
     * {@link Value#getEncodedSize} and {@link Value#setEncodedSize} to
     * manipulate the byte array directly. More commonly, the implementation of
     * this method will simply call the appropriate <code>put</code> methods to
     * write the interior field values into the <code>Value</code> object.
     * </p>
     * 
     * @param value
     *            The <code>Value</code> to which the interior data of the
     *            supplied <code>Object</code> should be encoded
     * @param object
     *            The object value to encode. This parameter will never be
     *            <code>null</code> because Persistit encodes nulls with a
     *            built-in encoding.
     * @param context
     *            An arbitrary object that can optionally be supplied by the
     *            application to convey an application-specific context for the
     *            operation. (See {@link CoderContext}.) The default value is
     *            <code>null</code>.
     */
    @Override
    public void put(final Value value, final Object object, final CoderContext context) throws ConversionException {
        if (_superClassValueRenderer != null) {
            _superClassValueRenderer.put(value, object, context);
        }
        if (object instanceof Externalizable) {
            try {
                ((Externalizable) object).writeExternal(value.getObjectOutputStream());
            } catch (final Exception e) {
                throw new ConversionException("Invoking writeExternal for " + _clazz, e);
            }
        } else if (_writeObjectMethod != null) {
            invokeMethod(value, _writeObjectMethod, object, new Object[] { value.getObjectOutputStream() }, true);
        } else {
            putDefaultFields(value, object);
        }
    }

    /**
     * Invoke the object's writeReplace method, if there is one.
     * 
     * @param value
     *            The <code>Value</code> into which the object is being
     *            serialized
     * 
     * @param object
     *            The object being serialized
     * 
     * @return The replacement determined by the object's
     *         <code>writeReplace</code>, if present, or otherwise the object
     *         itself.
     */
    Object writeReplace(final Value value, final Object object) {
        if (_writeReplaceMethod != null) {
            return invokeMethod(value, _writeReplaceMethod, object, EMPTY_OBJECT_ARRAY, false);
        } else {
            return object;
        }
    }

    private Object invokeMethod(final Value value, final Method method, final Object object, final Object[] args,
            final boolean setStackFields) throws ConversionException {
        final DefaultValueCoder saveCoder = value.getCurrentCoder();
        final Object saveObject = value.getCurrentObject();
        value.setCurrentCoder(setStackFields ? this : null);
        value.setCurrentObject(setStackFields ? object : null);
        try {
            final Object result = method.invoke(object, args);
            return result;
        } catch (final Exception e) {
            throw new ConversionException("Invoking " + method + " for " + _clazz, e);
        } finally {
            value.setCurrentCoder(saveCoder);
            value.setCurrentObject(saveObject);
        }
    }

    private Object invokeMethod(final Method method, final Object object, final Object[] args,
            final boolean setStackFields) throws ConversionException {
        try {
            final Object result = method.invoke(object, args);
            return result;
        } catch (final Exception e) {
            throw new ConversionException("Invoking " + method + " for " + _clazz, e);
        }
    }

    /**
     * Construct a new instance of the client class using internal, non-public
     * API methods of the Java platform. This method is package-private to
     * discourage inappropriate use.
     */
    Object newInstance() {
        try {
            if (_newInstanceConstructor != null) {
                return _newInstanceConstructor.newInstance(EMPTY_OBJECT_ARRAY);
            }
            if (_newInstanceMethod != null) {
                return _newInstanceMethod.invoke(_classDescriptor, _newInstanceArguments);
            } else {
                return _clazz.newInstance();
            }
        } catch (final Exception e) {
            throw new ConversionException("Instantiating " + _clazz.getName(), e);

        }
    }

    /**
     * Writes the fields of the supplied object to the supplied
     * <code>Value</code>. This method is called directly by {@link #put} unless
     * the class defines a <code>writeObject</code> method for custom
     * serialization. This method is also called indirectly by the
     * <code>defaultWriteObject</code> method of the
     * <code>ObjectOutputStream</code> passed to <code>writeObject</code> is
     * called. The collection and ordering of fields to be written is determined
     * by the Java Serialization Specification.
     * 
     * @param value
     *            The <code>Value</code> into which fields should be put
     * @param object
     *            The object whose fields are to be serialized
     * 
     * @throws ConversionException
     */
    public void putDefaultFields(final Value value, final Object object) throws ConversionException {
        Accessor accessor = null;
        try {
            final Accessor[] accessors = _valueBuilder._accessors;
            for (int index = 0; index < accessors.length; index++) {
                accessor = accessors[index];
                accessors[index].toValue(object, value);
            }
        } catch (final Exception e) {
            throw new ConversionException("Encoding " + accessor.toString() + " for " + _clazz, e);
        }
    }

    /**
     * <p>
     * Creates an instance of the supplied class, populates its state by
     * decoding the supplied <code>Value</code>, and returns it. This method
     * will be called only if this <code>ValueCoder</code> has been registered
     * with the current {@link CoderManager} to encode objects having supplied
     * <code>Class</code> value. Persistit will never call this method to decode
     * a value that was <code>null</code> when written because null values are
     * handled by built-in encoding logic.
     * </p>
     * 
     * @param value
     *            The <code>Value</code> from which interior fields of the
     *            object are to be retrieved
     * 
     * @param clazz
     *            The class of the object to be returned.
     * 
     * @param context
     *            An arbitrary object that can optionally be supplied by the
     *            application to convey an application-specific context for the
     *            operation. (See {@link CoderContext}.) The default value is
     *            <code>null</code>.
     * 
     * @return An <code>Object</code> having the same class as the suppled
     *         <code>clazz</code> parameter.
     * 
     * @throws ConversionException
     */
    @Override
    public Object get(final Value value, final Class clazz, final CoderContext context) throws ConversionException {
        if (clazz != _clazz)
            throw new ClassCastException("Client class " + _clazz.getName() + " does not match requested class "
                    + clazz.getName());

        final Object instance = newInstance();
        value.registerEncodedObject(instance);
        render(value, instance, clazz, context);
        return readResolve(value, instance);
    }

    Object readResolve(final Value value, Object instance) {
        if (_readResolveMethod != null) {
            instance = invokeMethod(value, _readResolveMethod, instance, EMPTY_OBJECT_ARRAY, false);
        }
        return instance;
    }

    Object readResolve(Object instance) {
        if (_readResolveMethod != null) {
            instance = invokeMethod(_readResolveMethod, instance, EMPTY_OBJECT_ARRAY, false);
        }
        return instance;
    }

    /**
     * <p>
     * Populates the state of the supplied (mutable) target <code>Object</code>
     * by decoding the supplied <code>Value</code>. This method will be called
     * only if this <code>ValueRenderer</code> has been registered with the
     * current {@link CoderManager} to encode objects having the supplied
     * <code>Class</code> value. Persistit will never call this method to decode
     * a value that was <code>null</code> when written because null values are
     * handled by built-in encoding logic.
     * </p>
     * 
     * @param value
     *            The <code>Value</code> from which interior fields of the
     *            object are to be retrieved
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
     *            <code>null</code>.
     * 
     * @throws ConversionException
     */
    @Override
    public void render(final Value value, final Object target, final Class clazz, final CoderContext context)
            throws ConversionException {
        if (target == null) {
            throw new IllegalArgumentException("Target object must not be null");
        }
        if (_superClassValueRenderer != null) {
            _superClassValueRenderer.render(value, target, clazz.getSuperclass(), context);
        }

        if (target instanceof Externalizable) {
            try {
                ((Externalizable) target).readExternal(value.getObjectInputStream());
            } catch (final Exception e) {
                throw new ConversionException("Invoking readExternal for " + _clazz, e);
            }
        } else if (_readObjectMethod != null) {
            invokeMethod(value, _readObjectMethod, target, new Object[] { value.getObjectInputStream() }, true);
        } else {
            renderDefaultFields(value, target);
        }
    }

    /**
     * Reads the default fields - i.e., the fields identified as non-transient
     * non-static fields through introspection. This method is called by the
     * specialized ObjectInputStream used by the {@link Value} object.
     * 
     * @param value
     *            The Value from which to read field data
     * @param target
     *            The object whose fields will be set
     * 
     * @throws ConversionException
     */
    void renderDefaultFields(final Value value, final Object target) throws ConversionException {
        Accessor accessor = null;
        try {
            final Accessor[] accessors = _valueBuilder._accessors;
            for (int index = 0; index < accessors.length; index++) {
                accessor = accessors[index];
                accessor.fromValue(target, value);
            }
        } catch (final Exception e) {
            throw new ConversionException("Decoding " + accessor.toString() + " for " + _clazz, e);
        }
    }

    /**
     * Return the <code>Builder</code> that copies data values between a
     * <code>Value</code> and a client object.
     * 
     * @return The Builder
     */
    public Builder getValueBuilder() {
        return _valueBuilder;
    }

    /**
     * Return a String description of this <code>DefaultValueCoder</code>. The
     * String includes the client class name, and the property or field names
     * identifying the properties and/or fields this coder accesses and
     * modifies.
     * 
     * @return A String description.
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("DefaultValueCoder(");
        sb.append(_clazz.getName());
        sb.append(",");
        sb.append(getValueBuilder().toString());
        sb.append(")");
        return sb.toString();
    }

    @Override
    public synchronized void setHandle(final int handle) {
        if (_handle != 0 && _handle != handle) {
            throw new IllegalStateException("Attempt to change handle from " + _handle + " to " + handle);
        }
        _handle = handle;
    }

    @Override
    public int getHandle() {
        return _handle;
    }
}
