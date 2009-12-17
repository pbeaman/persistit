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
import java.io.Serializable;

import com.persistit.Key;
import com.persistit.Value;
/**
 * A marker interface for an application-specific object that may be passed
 * to a {@link KeyCoder} or {@link ValueCoder}. This object may be used to
 * pass application context information from the application to the coder
 * instance. The following methods accept a <tt>CoderContext</tt>:
 * <ul>
 * <li>{@link Key#append(Object, CoderContext)}</li> 
 * <li>{@link Key#decode(Object, CoderContext)}</li> 
 * <li>{@link Key#decodeString(CoderContext)}</li> 
 * <li>{@link Key#decodeString(StringBuffer, CoderContext)}</li> 
 * <li>{@link Key#decodeDisplayable(boolean, StringBuffer, CoderContext)}</li> 
 * <li>{@link Value#put(Object, CoderContext)}</li>
 * <li>{@link Value#get(Object, CoderContext)}</li>
 * </ul>
 * This interface has no behavior; it simply marks classes
 * that are intended for this purpose to enhance type safety. Note that
 * <tt>CoderContext</tt> extends <tt>java.io.Serializable</tt>, meaning that
 * any object you provide as a CoderContext must behave correctly when
 * serialized and deserialized.
 *
 * @version 1.0
 */
public interface CoderContext
extends Serializable
{

}
