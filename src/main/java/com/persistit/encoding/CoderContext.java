/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
