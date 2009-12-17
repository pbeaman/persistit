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
 * Created on Nov 28, 2005
 */

package com.persistit.jmx;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import javax.management.ReflectionException;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularType;


class CompositeTypeAdapter
extends SimpleAdapter
{
    private static final long serialVersionUID = -3325019089424950343L;
    /**
     * 
     */
    private final static Object[] EMPTY_ARG_ARRAY = new Object[0];
    private final static Class[] EMPTY_CLASS_ARRAY = new Class[0];
    
    private String _typeName;
    private String _description;
    private String[] _attrNames;
    private OpenType[] _attrTypes;
    private String[] _attrDescriptions;
    private HashMap _nameToIndex;

    private int[] _sortedByName;
    private Method[] _getters;
    
    private TabularType _tabularType;
    
    CompositeTypeAdapter(
        String typeName,
        String description,
        String[] itemNames,
        String[] itemDescriptions,
        OpenType[] itemTypes,
        Class actualClass)
    throws OpenDataException
    {
        super(
            new CompositeType(
                typeName,
                description,
                itemNames,
                itemDescriptions,
                itemTypes));
        
        _typeName = typeName;
        _description = description;
        _attrNames = itemNames;
        _attrTypes = itemTypes;
        _attrDescriptions = itemDescriptions;
        _actualClass = actualClass;
        _nameToIndex = new HashMap();
        _getters = new Method[itemNames.length];
        
        TreeMap sortedMap = new TreeMap();
        
        for (int index = 0; index < itemNames.length; index++)
        {
            String name = itemNames[index];
            Integer key = new Integer(index);
            sortedMap.put(name, key);
            _nameToIndex.put(name, key);
            String methodName = 
                itemTypes[index] == SimpleType.BOOLEAN ? "is" : "get";
            methodName =
                methodName +
                Character.toUpperCase(name.charAt(0)) + 
                name.substring(1);
            
            Method getMethod = lookupGetMethod(actualClass, methodName);
            if (getMethod == null)
            {
                getMethod = lookupGetMethod(actualClass, name);
            }
            if (getMethod == null)
            {
                throw new OpenDataException("Get method not found for " + name);
            }
            _getters[index] = getMethod;
            _tabularType = new TabularType(
                typeName + "Array", 
                "(Array of) " + description,
                (CompositeType)getOpenType(),
                new String[]{itemNames[0]});
        }
        _sortedByName = new int[itemNames.length];
        int index = 0;
        for (Iterator iter = sortedMap.entrySet().iterator();
             iter.hasNext(); )
        {
            Map.Entry entry = (Map.Entry)iter.next();
            _sortedByName[index++] = ((Integer)entry.getValue()).intValue();
        }
    }
    
    public boolean isComposite()
    {
        return true;
    }
    
    TabularType getTabularType()
    {
        return _tabularType;
    }
    
    private Method lookupGetMethod(Class actualClass, String name)
    {
        try
        {
            return actualClass.getMethod(
                name,
                EMPTY_CLASS_ARRAY);
        }
        catch (NoSuchMethodException nsme)
        {
            return null;
        }
    }
    
    public Object wrapValue(Object value)
    throws OpenDataException, ReflectionException

    {
        Object[] valueArray = new Object[_attrNames.length];
        for (int index = 0; index < valueArray.length; index++)
        {
            valueArray[index] = get(index, value);
        }
        try
        {
            return new CompositeDataSupport(
                (CompositeType)getOpenType(), _attrNames, valueArray);
        }
        catch (OpenDataException ode)
        {
            throw ode;
        }
    }
    
    Object get(int index, Object target)
    throws ReflectionException
    {
        try
        {
            // is always one of the "simple" types.
            Object value = _getters[index].invoke(target, EMPTY_ARG_ARRAY);
            if (_attrTypes[index] == SimpleType.DATE && 
                value instanceof Long)
            {
                value = new Date(((Long)value).longValue());
            }
            
            return value;
        }
        catch (Exception e)
        {
            throw new PersistitJmxRuntimeException(e);
        }
    }
    
    public void toXml(StringBuilder sb)
    {
        sb.append("<type ");
        sb.append("name=\"");
        sb.append(_typeName);
        sb.append("\" ");
        
        sb.append(" description=\"");
        sb.append(PersistitOpenMBean.xmlQuote(_description));
        sb.append("\" >\r\n");
        
        for (int index = 0; index < _attrNames.length; index++)
        {
            sb.append("<attribute name=\"" + _attrNames[index] + "\" ");
            sb.append("type=\"");
            sb.append(PersistitOpenMBean.prettyType(_attrTypes[index].getTypeName()));
            sb.append("\" description=\"");
            sb.append(PersistitOpenMBean.xmlQuote(_attrDescriptions[index]));
            sb.append("\" />\r\n");
        }

        sb.append("</type>\r\n");
    }
}
