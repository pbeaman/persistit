/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
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
