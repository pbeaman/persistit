package com.persistit.jmx;


import java.lang.reflect.Method;

import javax.management.ReflectionException;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenMBeanAttributeInfo;
import javax.management.openmbean.OpenMBeanAttributeInfoSupport;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

import com.persistit.Management;

/**
 * 
 */
class AttributeReflector
{
    private final static Class[] EMPTY_CLASS_ARRAY = new Class[0];
    private final static Object[] EMPTY_OBJECT_ARRAY = new Object[0];
    
    private String _name;
    private Adapter _adapter;
    
    private OpenMBeanAttributeInfo _info;
    
    private Method _getMethod;
    private Method _setMethod;
    
    AttributeReflector(
        String name, 
        String description, 
        Adapter adapter)
    throws OpenDataException
    {
        _name = name;
        _adapter = adapter;
        
        String camelName =
            Character.toUpperCase(name.charAt(0)) +
            name.substring(1);
        OpenType openType = adapter.getOpenType();
        
        boolean isIs = openType == SimpleType.BOOLEAN;
        String getMethodName = (isIs ? "is" : "get") + camelName;
        
        _getMethod = lookupMethod(getMethodName, EMPTY_CLASS_ARRAY);
        if (_getMethod == null)
        {
            _getMethod = lookupMethod(name, EMPTY_CLASS_ARRAY);
        }
        if (_getMethod == null)
        {
            throw new OpenDataException("Get method not found for attribute " + name);
        }
        Class actualClass = adapter.getActualClass();
        _setMethod = lookupMethod("set" + camelName, new Class[]{actualClass});
        
        _info = new OpenMBeanAttributeInfoSupport(
            name,
            description,
            openType,
            _getMethod != null,
            _setMethod != null,
            isIs);
    }
    
    
    Object get(Management management)
    throws ReflectionException
    {
        Object result = null;
        try
        {
            result = _getMethod.invoke(management, EMPTY_OBJECT_ARRAY);
            if (result == null) return null;
            return _adapter.wrapValue(result);
        }
        catch (Exception e)
        {
            throw new ReflectionException(e);
        }
    }
    
    void set(Management management, Object value)
    throws ReflectionException
    {
        try
        {
            if (_setMethod == null)
            {
                throw new NoSuchMethodException(
                    "Set method for attribute " + _name);
            }
            if (value != null && _adapter.isComposite())
            {
                throw new UnsupportedOperationException(
                    "Can't set Composite Data Item for attribute " + _name);
            }
            _setMethod.invoke(management, new Object[]{value});
        }
        catch (Exception e)
        {
            throw new ReflectionException(e);
        }
    }
    

    Method lookupMethod(String name, Class[] paramTypes)
    {
        try
        {
            return Management.class.getMethod(name, paramTypes);
        }
        catch (NoSuchMethodException nsme)
        {
            return null;
        }
    }
    
    String getName()
    {
        return _name;
    }
    
    OpenType getOpenType()
    {
        return _adapter.getOpenType();
    }
    
    OpenMBeanAttributeInfo getAttributeInfo()
    {
        return _info;
    }
    
    public void toXml(StringBuilder sb)
    {
        sb.append("<attribute name=\"");
        sb.append(_name);
        sb.append("\" ");
        sb.append("description=\"");
        sb.append(PersistitOpenMBean.xmlQuote(_info.getDescription()));
        sb.append("\">\r\n");
        _adapter.toXml(sb);
        sb.append("</attribute>\r\n");
    }
}
