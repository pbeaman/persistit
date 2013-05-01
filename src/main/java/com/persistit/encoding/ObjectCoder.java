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

/**
 * <p>
 * Combines the {@link KeyCoder}, {@link KeyRenderer}, {@link ValueCoder} and
 * {@link ValueRenderer} into a single interface that allows Persistit to store
 * and retrieve arbitrary objects - even non-Serializable objects - without
 * byte-code enhancement, without incurring the space or time overhead of Java
 * serialization, or the need to modify the class to perform custom
 * serialization. During initialization, an application typically associates an
 * <code>ObjectCoder</code> with each the <code>Class</code> of each object that
 * will be stored in or fetched from the Persistit database. The
 * <code>ObjectCoder</code> implements all of the logic necessary to encode and
 * decode the state of objects of that class to and from Persistit storage
 * structures. Although Persistit is not designed to provide transparent
 * persistence, the <code>ObjectCoder</code> interface simplifies object
 * persistence code.
 * </p>
 * 
 * 
 * @version 1.0
 */
public interface ObjectCoder extends KeyRenderer, ValueRenderer {
}
