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
 * Created on Jul 1, 2004
 */
package com.persistit.encoding;

import com.persistit.Key;
import com.persistit.exception.ConversionException;

/**
 * <p>
 * An extended {@link KeyCoder} that can populate a supplied <tt>Object</tt>
 * with data that was formerly written to a {@link Key} by the <tt>KeyCoder</tt>
 * .
 * </p>
 * <p>
 * A <tt>KeyRenderer</tt> implements an additional method called
 * {@link #renderKeySegment} that populates a supplied target object rather than
 * creating a new object instance. For example, a {@link KeyStringCoder}, which
 * extends this interface, accepts a <tt>StringBuilder</tt> as a target. Instead
 * of decoding and creating a new <tt>String</tt> object,
 * <tt>renderKeySegment</tt> modifies the <tt>StringBuilder</tt> target to
 * contain the decoded String.
 * </p>
 * 
 * 
 * @version 1.0
 */
public interface KeyRenderer extends KeyCoder {

    /**
     * <p>
     * Populates the state of the supplied target <tt>Object</tt> by decoding
     * the next key segment of the supplied <tt>Key</tt>. This method will be
     * called only if this <tt>KeyRenderer</tt> has been registered with the
     * current {@link CoderManager} to encode objects having the supplied
     * <tt>Class</tt> value. In addition, Persistit will never call this method
     * to decode a value that was <tt>null</tt> when written because null values
     * are handled by built-in encoding logic.
     * </p>
     * 
     * @param key
     *            The <tt>Key</tt> from which interior fields of the object are
     *            to be retrieved
     * 
     * @param target
     *            An object into which the key segment is to be written
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
    public void renderKeySegment(Key key, Object target, Class clazz,
            CoderContext context) throws ConversionException;

}
