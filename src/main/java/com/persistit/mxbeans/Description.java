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

package com.persistit.mxbeans;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.management.DescriptorKey;

/**
 * 
 * This annotation is intended to work somewhat like the Description annotation
 * specified in JMX 2 (JSR 255) which is currently inactive. The annotation lets
 * you add a description to the Descriptor attribute of an MBeanFeatureInfo.
 * Unfortunately that does not directly affect the result returned by
 * {@link javax.management.MBeanFeatureInfo#getDescription()} but that can be
 * accomplished through the MXBeanWrapper class which marshals the value from
 * the Descriptor to the description attribute.
 * 
 * @see <a href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4853303">bug
 *      4853303</a>
 */
@Target({ ElementType.METHOD, ElementType.PARAMETER })
@Retention(RetentionPolicy.RUNTIME)
public @interface Description {
    String DESCRIPTION = "Description";

    @DescriptorKey(DESCRIPTION)
    String value();
}
