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
 */
package com.persistit;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;

import com.persistit.encoding.CoderContext;
import com.persistit.encoding.CoderManager;
import com.persistit.encoding.ValueCoder;
import com.persistit.exception.ConversionException;

/**
 * <p>
 * A {@link ValueCoder} that uses standard Java serialization to store and
 * retrieve object values. This class is related to {@link DefaultValueCoder}.
 * When Persistit serializes or deserializes an object for which there is no
 * registered <tt>ValueCoder</tt>, it implictly constructs either a
 * <tt>DefaultValueCoder</tt> or a <tt>SerialValueCoder</tt>, depending on
 * the value of the <tt>serialOverride</tt> property. See
 * <a href="../../../Object_Serialization_Notes.html">
 * Persistit JSA 1.1 Object Serialization</a> for details.
 * </p>
 * <p>
 * This class creates a new <tt>java.io.ObjectInputStream</tt> each time it
 * is called upon to deserialize an instance of its client class, or a
 * <tt>java.io.ObjectOutputStream</tt> to serialize an instance. The format of
 * the stored data is specified by the 
 * <a href="http://java.sun.com/j2se/1.4.2/docs/guide/serialization/spec/serialTOC.html">
 * Java Object Serialization Specification</a> with one exception: the
 * class descriptor is replaced by a handle; an association between handles and
 * class descriptors is maintained in a separate metadata tree.  This avoids
 * redundantly storing class descriptors for multiple instance of the same
 * class. 
 * </p>
 * @since 1.1
 * @version 1.1
 */
public final class SerialValueCoder
implements ValueCoder
{
    private ObjectStreamClass _classDescriptor;
    
    public SerialValueCoder(Class clazz)
    {
        _classDescriptor = ObjectStreamClass.lookup(clazz);
        if (_classDescriptor == null)
        {
            throw new ConversionException(
                "Not Serializable: " + clazz.getName());
        }
    }

    /**
     * <p>
     * Creates an instance of the supplied class, populates its state by
     * decoding the supplied <tt>Value</tt> using standard Java serialization, 
     * and returns it. This method will be called only if this 
     * <tt>ValueCoder</tt> has been registered with the current 
     * {@link CoderManager} to encode objects having supplied 
     * <tt>Class</tt> value. Persistit will never call this method to 
     * decode a value that was <tt>null</tt> when written
     * because null values are handled by built-in encoding logic. 
     * </p>
     * 
     * @param value     The <tt>Value</tt> from which interior fields of the
     *                  object are to be retrieved
     *                  
     * @param clazz     The class of the object to be returned.
     * 
     * @param context   An arbitrary object that can optionally be supplied by
     *                  the application to convey an application-specific 
     *                  context for the operation. (See {@link CoderContext}.)
     *                  The default value is <tt>null</tt>. For 
     *                  <tt>SerialValueCoder</tt> this parameter is always
     *                  ignored.
     *                  
     * @return          An <tt>Object</tt> having the same class as the suppled
     *                  <tt>clazz</tt> parameter.
     *                  
     * @throws          ConversionException
     */
    public Object get(Value value, Class clazz, CoderContext context)
    {
        try
        {
            ObjectInputStream stream =
                new Value.OldValueInputStream(value, _classDescriptor);
            Object object = stream.readObject();
            stream.close();
            return object;
        }
        catch (Exception e)
        {
            throw new ConversionException(e);
        }
    }
    
    /**
     * <p>
     * Encodes the supplied <tt>Object</tt> into the supplied <tt>Value</tt>
     * using standard Java serialization. This method will be called only 
     * if this <tt>ValueCoder</tt> has been registered 
     * with the current {@link CoderManager} to encode objects
     * having the class of the supplied object.
     * </p>
     * <p>
     * Upon completion of this method, the backing byte array of the 
     * <tt>Value</tt> and its size should be updated to reflect the appended
     * key segment.  Use the methods {@link Value#getEncodedBytes},
     * {@link Value#getEncodedSize} and {@link Value#setEncodedSize} to 
     * manipulate the byte array directly.  More commonly, the implementation
     * of this method will simply call the appropriate <tt>put</tt> methods to
     * write the interior field values into the <tt>Value</tt> object.
     * </p>
     * @param value     The <tt>Value</tt> to which the interior data of the
     *                  supplied <tt>Object</tt> should be encoded
     *                  
     * @param object    The object value to encode.  This parameter will never
     *                  be <tt>null</tt> because Persistit encodes nulls with
     *                  a built-in encoding.
     *                  
     * @param context   An arbitrary object that can optionally be supplied by
     *                  the application to convey an application-specific 
     *                  context for the operation. (See {@link CoderContext}.)
     *                  The default value is <tt>null</tt>. For 
     *                  <tt>SerialValueCoder</tt> this parameter is always 
     *                  ignored.
     */
    public void put(Value value, Object object, CoderContext context)
    {
        try
        {
            ObjectOutputStream stream =
                new Value.OldValueOutputStream(value, _classDescriptor);
            stream.writeObject(object);
            stream.close();
        }
        catch (Exception e)
        {
            throw new ConversionException(e);
        }
    }
}
