package com.persistit.jmx;

import java.util.Date;

import javax.management.ReflectionException;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

class SimpleAdapter implements Adapter {
    OpenType _openType;
    Class _actualClass;

    SimpleAdapter(OpenType openType) throws OpenDataException {
        _openType = openType;
        try {
            _actualClass = Class.forName(openType.getClassName());
        } catch (ClassNotFoundException cnfe) {
            throw new OpenDataException("Unknown actual class for " + openType);
        }
    }

    public boolean isComposite() {
        return false;
    }

    public boolean isArray() {
        return false;
    }

    public OpenType getOpenType() {
        return _openType;
    }

    public Class getActualClass() {
        return _actualClass;
    }

    public Adapter getElementAdapter() {
        return null;
    }

    public Object wrapValue(Object value) throws OpenDataException,
            ReflectionException

    {
        // Implied conversions
        if (_openType == SimpleType.DATE && value instanceof Long) {
            long longValue = ((Long) value).longValue();
            if (longValue == 0) {
                value = null;
            } else {
                value = new Date(longValue);
            }
        }

        return value;
    }

    public void toXml(StringBuilder sb) {
        sb.append("<type name=\"");
        sb.append(PersistitOpenMBean.prettyType(_openType.getTypeName()));
        sb.append("\"/>\r\n");
    }

}
