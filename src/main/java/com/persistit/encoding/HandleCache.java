/**
 * Copyright 2012 Akiban Technologies, Inc.
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
 * Interface for an object that can receive and supply an int-valued handle. The
 * implementation should return 0 if no handle has been set, a non-zero value if
 * the handle has been set, and should throw an IllegalStateException if an
 * attempt is made to change the handle from one non-zero value to another.
 * </p>
 * 
 * @version 1.0
 */
public interface HandleCache {

    /**
     * Store a supplied handle.
     * 
     * @param handle
     * @throws IllegalStateException
     *             if there already is a non-matching non-zero handle stored
     */
    void setHandle(final int handle);

    /**
     * @return the stored handle value
     */
    int getHandle();
}
