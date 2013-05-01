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

package com.persistit.exception;

import com.persistit.VolumeSpecification;

/**
 * Thrown when the Persistit attempts to open a Volume with an incomplete
 * {@link VolumeSpecification}, for example, with a missing page size.
 * 
 * @version 1.0
 */
public class UnderSpecifiedVolumeException extends VolumeNotFoundException {
    private static final long serialVersionUID = -5547869858193325359L;

    public UnderSpecifiedVolumeException() {
        super();
    }

    public UnderSpecifiedVolumeException(final String msg) {
        super(msg);
    }
}
