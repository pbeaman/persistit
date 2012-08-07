/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit.encoding;

import java.io.Serializable;

import com.persistit.Key;
import com.persistit.Value;

/**
 * A marker interface for an application-specific object that may be passed to a
 * {@link KeyCoder} or {@link ValueCoder}. This object may be used to pass
 * application context information from the application to the coder instance.
 * The following methods accept a <code>CoderContext</code>:
 * <ul>
 * <li>{@link Key#append(Object, CoderContext)}</li>
 * <li>{@link Key#decode(Object, CoderContext)}</li>
 * <li>{@link Key#decodeDisplayable(boolean, Appendable, CoderContext)}</li>
 * <li>{@link Value#put(Object, CoderContext)}</li>
 * <li>{@link Value#get(Object, CoderContext)}</li>
 * </ul>
 * This interface has no behavior; it simply marks classes that are intended for
 * this purpose to enhance type safety. Note that <code>CoderContext</code>
 * extends <code>java.io.Serializable</code>, meaning that any object you
 * provide as a CoderContext must behave correctly when serialized and
 * deserialized.
 * 
 * @version 1.0
 */
public interface CoderContext extends Serializable {

}
