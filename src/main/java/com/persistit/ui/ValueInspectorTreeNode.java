/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
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
 * Created on Mar 27, 2005
 */
package com.persistit.ui;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.Timer;
import java.util.TimerTask;
import java.util.WeakHashMap;

import javax.swing.SwingUtilities;
import javax.swing.tree.TreeNode;

import com.persistit.Key;
import com.persistit.Util;

/**
 * @author Peter Beaman
 * @version 1.0
 */
class ValueInspectorTreeNode implements TreeNode {
    public final static long TO_STRING_TIMEOUT = 1000;
    public final static int MAX_DISPLAYABLE_LENGTH = 512;

    private static WeakHashMap _fieldArrayMap = new WeakHashMap();

    private String _displayable;
    private String _fieldName;
    private Object _object;
    private String _toString;
    private ValueInspectorTreeNode _parent;
    private int _index = -1;
    private Class _type;
    private ValueInspectorTreeNode[] _children;

    ValueInspectorTreeNode(ValueInspectorTreeNode parent, Object object,
            String name, Class type) {
        _parent = parent;
        _object = object;
        _type = type;
        _fieldName = name;
        int childCount = 0;

        if (type.isArray()) {
            if (object != null)
                childCount = arrayLength(type, object);
        } else if (!type.isPrimitive()) {
            if (object != null)
                childCount = getFields(type).length;
            if (type.getSuperclass() != null
                    && type.getSuperclass() != Object.class) {
                childCount++;
            }
        }

        _children = new ValueInspectorTreeNode[childCount];
    }

    ValueInspectorTreeNode(ValueInspectorTreeNode parent, Object object,
            String name, Class type, int index) {
        this(parent, object, name, type);
        _index = index;
    }

    String displayable(String fieldName, Class type, Object object,
            int arrayIndex) {
        StringBuilder sb = new StringBuilder();

        if (arrayIndex >= 0) {
            sb.append('[');
            sb.append(arrayIndex);
            sb.append(']');
        } else {
            sb.append(fieldName);
        }
        if (type.isPrimitive()) {
            if (arrayIndex == -1) {
                sb.append(" (");
                sb.append(type.getName());
                sb.append(")");
            }

            sb.append(" = ");

            if (type == boolean.class || type == float.class
                    || type == double.class) {
                sb.append(object.toString());
            } else if (type == byte.class) {
                sb.append(object);
                sb.append(" [0x");
                Util.hex(sb, ((Byte) object).byteValue(), 2);
                sb.append("]");
            } else if (type == short.class) {
                sb.append(object);
                sb.append(" [0x");
                Util.hex(sb, ((Short) object).shortValue(), 4);
                sb.append("]");
            } else if (type == char.class) {
                sb.append(object);
                sb.append(" [0x");
                Util.hex(sb, ((Character) object).charValue(), 4);
                sb.append("]");
            } else if (type == int.class) {
                sb.append(object);
                sb.append(" [0x");
                Util.hex(sb, ((Integer) object).intValue(), 8);
                sb.append("]");
            } else if (type == long.class) {
                sb.append(object);
                sb.append(" [0x");
                Util.hex(sb, ((Long) object).longValue(), 16);
                sb.append("]");
            } else
                throw new RuntimeException("type=" + type);
        } else if (_parent != null) {
            if (!type.isPrimitive() || _index < 0) {
                sb.append(" (");
                if (type.isArray()) {
                    sb.append(componentTypeName(type));
                    sb.append('[');
                    sb.append(arrayLength(type, object));
                    sb.append(']');
                    for (int index = dimensions(type); index-- > 1;) {
                        sb.append("[]");
                    }
                } else if (object == null) {
                    sb.append(type.getName());
                } else {
                    sb.append(object.getClass().getName());
                }
                sb.append(")");
            }
            if (object == null) {
                sb.append(" = null");
            } else if (object instanceof String) {
                sb.append(" = \"");
                Util.appendQuotedString(sb, object.toString(), 0,
                        MAX_DISPLAYABLE_LENGTH - sb.length());
                sb.append('\"');
            } else if (object instanceof Date) {
                sb.append(" = ");
                sb.append(Key.SDF.format((Date) object));
            } else if (type == Boolean.class || type == Byte.class
                    || type == Short.class || type == Character.class
                    || type == Integer.class || type == Long.class
                    || type == Float.class || type == Double.class
                    || type == BigInteger.class || type == BigDecimal.class) {
                sb.append(" = ");
                sb.append(object);
            }

            if (object != null && !object.getClass().isPrimitive()) {
                sb.append("  (id=");

                sb.append(System.identityHashCode(object));
                sb.append(")");
            }
        }
        if (sb.length() > MAX_DISPLAYABLE_LENGTH) {
            sb.setLength(MAX_DISPLAYABLE_LENGTH);
            sb.append("...");
        }
        return sb.toString();
    }

    int dimensions(Class type) {
        int count = 0;
        while (type.isArray()) {
            count++;
            type = type.getComponentType();
        }
        return count;
    }

    String componentTypeName(Class type) {
        if (type.isArray()) {
            return componentTypeName(type.getComponentType());
        }
        return type.getName();
    }

    static int arrayLength(Class type, Object object) {
        if (type.isArray()) {
            Class elementType = type.getComponentType();
            if (elementType.isPrimitive()) {
                if (elementType == boolean.class) {
                    return ((boolean[]) object).length;
                }
                if (elementType == byte.class) {
                    return ((byte[]) object).length;
                }
                if (elementType == short.class) {
                    return ((short[]) object).length;
                }
                if (elementType == char.class) {
                    return ((char[]) object).length;
                }
                if (elementType == int.class) {
                    return ((int[]) object).length;
                }
                if (elementType == long.class) {
                    return ((long[]) object).length;
                }
                if (elementType == float.class) {
                    return ((float[]) object).length;
                }
                if (elementType == double.class) {
                    return ((double[]) object).length;
                }
            } else {
                return ((Object[]) object).length;
            }
        }
        return 0;
    }

    public Enumeration children() {
        if (_object == null || _type.isPrimitive())
            return null;
        return new ChildEnumeration();
    }

    public boolean getAllowsChildren() {
        return true;
    }

    public TreeNode getChildAt(int childIndex) {
        if (_object == null || _type.isPrimitive())
            return null;
        if (_children[childIndex] == null) {
            if (_type.isArray()) {
                _children[childIndex] = getArrayChild(childIndex);
            } else {
                _children[childIndex] = getFieldValue(childIndex);
            }
        }
        return _children[childIndex];
    }

    public int getChildCount() {
        return _children.length;
    }

    public int getIndex(TreeNode node) {
        if (_parent == null)
            return -1;
        ValueInspectorTreeNode[] siblings = _parent._children;
        for (int index = 0; index < siblings.length; index++) {
            if (node == siblings[index])
                return index;
        }
        return -1;
    }

    public TreeNode getParent() {
        return _parent;
    }

    public boolean isLeaf() {
        return _children.length == 0;
    }

    public String toString() {
        if (_displayable == null) {
            _displayable = displayable(_fieldName, _type, _object, _index);
        }
        return _displayable;
    }

    String getEvalString() {
        return _toString;
    }

    void evalString(Runnable runnable) {
        new ToStringRunner(_object, runnable).start();
    }

    private static String evalString(Object object) {
        if (object == null)
            return "null";
        if (object.getClass().isArray()) {
            StringBuilder sb = new StringBuilder();
            Class elementType = object.getClass().getComponentType();
            boolean primitive = elementType.isPrimitive();
            int size = arrayLength(object.getClass(), object);
            sb.append("{");
            for (int index = 0; index < size; index++) {
                if (index > 0)
                    sb.append(", ");
                if (primitive) {
                    if (elementType == boolean.class) {
                        sb.append(((boolean[]) object)[index] ? Boolean.TRUE
                                : Boolean.FALSE);
                    } else if (elementType == byte.class) {
                        sb.append(((byte[]) object)[index]);
                    } else if (elementType == short.class) {
                        sb.append(((short[]) object)[index]);
                    } else if (elementType == char.class) {
                        sb.append((int) ((char[]) object)[index]);
                    } else if (elementType == int.class) {
                        sb.append(((int[]) object)[index]);
                    } else if (elementType == long.class) {
                        sb.append(((long[]) object)[index]);
                    } else if (elementType == float.class) {
                        sb.append(((float[]) object)[index]);
                    } else if (elementType == double.class) {
                        sb.append(((double[]) object)[index]);
                    } else
                        throw new RuntimeException();
                } else {
                    sb.append(evalString(((Object[]) object)[index]));
                }
            }
            sb.append("}");
            return sb.toString();
        }
        if (object instanceof String) {
            StringBuilder sb = new StringBuilder();
            sb.append('\"');
            Util.appendQuotedString(sb, object.toString(), 0, Integer.MAX_VALUE);
            sb.append('\"');
            return sb.toString();
        } else {
            try {
                return object.toString();
            } catch (Throwable t) {
                return t + " while invoking " + object.getClass().getName()
                        + ".toString()";
            }
        }
    }

    private ValueInspectorTreeNode getFieldValue(int index) {
        if (_type.getSuperclass() != null
                && _type.getSuperclass() != Object.class) {
            if (index == 0) {
                return new ValueInspectorTreeNode(this, _object, _type
                        .getSuperclass().toString(), _type.getSuperclass());
            } else {
                index--;
            }
        }
        Object childValue = null;
        Field field = getFields(_type)[index];
        try {
            childValue = field.get(_object);
        } catch (IllegalAccessException iae) {
            childValue = "{{" + iae.toString() + "}}";
        }
        return _children[index] = new ValueInspectorTreeNode(this, childValue,
                field.getName(), field.getType());
        // childValue.getClass());
    }

    private ValueInspectorTreeNode getArrayChild(int index) {
        Object element;
        Class elementType = _type.getComponentType();
        if (elementType.isPrimitive()) {
            if (elementType == boolean.class) {
                element = ((boolean[]) _object)[index] ? Boolean.TRUE
                        : Boolean.FALSE;
            } else if (elementType == byte.class) {
                element = new Byte(((byte[]) _object)[index]);
            } else if (elementType == short.class) {
                element = new Short(((short[]) _object)[index]);
            } else if (elementType == char.class) {
                element = new Character(((char[]) _object)[index]);
            } else if (elementType == int.class) {
                element = new Integer(((int[]) _object)[index]);
            } else if (elementType == long.class) {
                element = new Long(((long[]) _object)[index]);
            } else if (elementType == float.class) {
                element = new Float(((float[]) _object)[index]);
            } else if (elementType == double.class) {
                element = new Double(((double[]) _object)[index]);
            } else
                throw new RuntimeException();
        } else {
            element = ((Object[]) _object)[index];
        }

        Class type = elementType.isPrimitive() || element == null ? elementType
                : element.getClass();

        return new ValueInspectorTreeNode(this, element, "[" + index + "]",
                type, index);
    }

    private Field[] getFields(Class type) {
        Field[] array = (Field[]) _fieldArrayMap.get(type.getName());
        if (array == null) {
            array = type.getDeclaredFields();
            ArrayList selected = new ArrayList();
            for (int index = 0; index < array.length; index++) {
                Field field = array[index];
                if (!Modifier.isStatic(field.getModifiers())) {
                    selected.add(field);
                }
            }
            array = (Field[]) selected.toArray(new Field[selected.size()]);
            Arrays.sort(array, new FieldComparator());
            AccessibleObject.setAccessible(array, true);
            _fieldArrayMap.put(type.getName(), array);
        }
        return array;
    }

    private class FieldComparator implements Comparator {
        public int compare(Object a, Object b) {
            Field fieldA = (Field) a;
            Field fieldB = (Field) b;
            String nameA = fieldA.getName();
            String nameB = fieldB.getName();
            return (nameA.compareTo(nameB));
        }
    }

    private class ChildEnumeration implements Enumeration {
        int _enumerationIndex = 0;

        public boolean hasMoreElements() {
            return _enumerationIndex < _children.length;
        }

        public Object nextElement() {
            return getChildAt(_enumerationIndex++);
        }
    }

    private class ToStringRunner extends Thread {
        private Object _object;
        private Runnable _runnable;
        private boolean _done;

        ToStringRunner(Object object, Runnable runnable) {
            _object = object;
            _runnable = runnable;
        }

        public void run() {
            try {
                _toString = null;
                Timer timer = new Timer();

                timer.schedule(new TimerTask() {
                    public void run() {
                        if (!_done) {
                            _toString = "???";
                            // Note: This is a deprecated method that is
                            // generally considered unsafe. Nonetheless,
                            // we need to stop the runaway Thread.
                            ToStringRunner.this.stop();
                        }
                    }
                }, TO_STRING_TIMEOUT);
                _toString = evalString(_object);
                _done = true;
                timer.cancel();
            } finally {
                SwingUtilities.invokeLater(_runnable);
            }
        }
    }
}
