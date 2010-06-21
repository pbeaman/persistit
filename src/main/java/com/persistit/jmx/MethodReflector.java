package com.persistit.jmx;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import javax.management.MBeanParameterInfo;
import javax.management.ReflectionException;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenMBeanOperationInfo;
import javax.management.openmbean.OpenMBeanOperationInfoSupport;
import javax.management.openmbean.OpenMBeanParameterInfo;
import javax.management.openmbean.OpenType;

import com.persistit.Management;

/**
 * 
 */
class MethodReflector {
    private final static Class[] EMPTY_CLASS_ARRAY = new Class[0];
    private final static Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    private String _name;
    private Adapter _adapter;
    private Method _method;
    private OpenMBeanOperationInfo _info;

    MethodReflector(String name, String description,
            OpenMBeanParameterInfo[] signature, Adapter adapter, int impact)
            throws OpenDataException {
        _name = name;
        _adapter = adapter;

        OpenType openType = adapter.getOpenType();
        Class[] types = new Class[signature.length];
        List typeList = new ArrayList();
        for (int index = 0; index < types.length; index++) {
            Class clazz = PersistitOpenMBean.openTypeToClass(signature[index]
                    .getOpenType());
            typeList.add(clazz);
        }
        Class[] typeArray = (Class[]) typeList.toArray(new Class[typeList
                .size()]);

        _method = lookupMethod(name, typeArray);
        if (_method == null) {
            throw new OpenDataException("Method " + name + " not found");
        }
        Class actualClass = adapter.getActualClass();

        _info = new OpenMBeanOperationInfoSupport(name, description, signature,
                openType, impact);
    }

    Object invoke(Management management, Object[] params)
            throws ReflectionException {
        try {
            Object result = _method.invoke(management, params);
            return _adapter.wrapValue(result);
        } catch (Exception e) {
            throw new ReflectionException(e, "Exception while invoking "
                    + _name);
        }
    }

    Method lookupMethod(String name, Class[] paramTypes) {
        try {
            return Management.class.getMethod(name, paramTypes);
        } catch (NoSuchMethodException nsme) {
            return null;
        }
    }

    String getName() {
        return _name;
    }

    OpenType getOpenType() {
        return _adapter.getOpenType();
    }

    OpenMBeanOperationInfo getOperationInfo() {
        return _info;
    }

    public void toXml(StringBuilder sb) {
        sb.append("<method ");
        sb.append("name=\"" + _name + "\" ");
        sb.append("description=\"");
        sb.append(PersistitOpenMBean.xmlQuote(_info.getDescription()));
        sb.append("\" ");
        sb.append("impact=\"" + _info.getImpact() + "\">\r\n");
        sb.append("<returns>\r\n");
        _adapter.toXml(sb);
        sb.append("</returns>\r\n");
        MBeanParameterInfo[] signature = _info.getSignature();
        sb.append("<params>\r\n");
        for (int index = 0; index < signature.length; index++) {
            MBeanParameterInfo info = signature[index];
            sb.append("<param name=\"");
            sb.append(info.getName());
            sb.append("\" type=\"");
            sb.append(PersistitOpenMBean.prettyType(info.getType()));
            sb.append("\" description=\"");
            sb.append(PersistitOpenMBean.xmlQuote(info.getDescription()));
            sb.append("\" />\r\n");
        }
        sb.append("</params>\r\n");
        sb.append("</method>\r\n");
    }
}
