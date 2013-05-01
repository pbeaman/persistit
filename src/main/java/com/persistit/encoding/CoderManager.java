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

import java.util.Map;

import com.persistit.DefaultCoderManager;
import com.persistit.Exchange;
import com.persistit.Key;

/**
 * <p>
 * Manages the collections of {@link KeyCoder}s and {@link ValueCoder}s that
 * encode and decode object values in Persistit&trade;. An application should
 * register all <code>KeyCoder</code> and <code>ValueCoder</code>
 * implementations before storing or accessing objects of the associated classes
 * in Persistit.
 * </p>
 * <p>
 * When Persistit is initialized, a instance of {@link DefaultCoderManager} is
 * installed as the current <code>CoderManager</code>. Applications can refer to
 * it through the method {@link com.persistit.Persistit#getCoderManager}.
 * Applications should register <code>KeyCoder</code> and
 * <code>ValueCoder</code> instances as follows: <blockquote>
 * 
 * <pre>
 * ValueCoder vc1 = new MyClassValueCoder();
 * KeyCoder kc1 = new MyClassKeyCoder();
 * Persistit.getInstance().getCoderManager().registerValueCoder(MyClass.class, kc1);
 * Persistit.getInstance().getCoderManager().registerKeyCoder(MyClass.class, kc1);
 * </pre>
 * 
 * </blockquote> Subsequently, any time the application appends an instance of a
 * <code>MyClass</code> to a <code>Key</code>, the <code>MyClassKeyCoder</code>
 * will be used to convert the interior state of the <code>MyClass</code> into a
 * representation within the <code>Key</code>. Likewise, whenever the
 * application puts an object of class <code>MyClass</code> into a
 * <code>Value</code>, the registered <code>MyClassValueCoder</code> will be
 * used to encode it. Persistit uses the {@link #lookupKeyCoder} and
 * {@link #lookupValueCoder} methods to acquire the appropriate coder.
 * </p>
 * <p>
 * An application may replace the default implementation provided by
 * <code>DefaultCoderManager</code>. Such a custom implementation might, for
 * example, handle the relationship between certain classes and their associated
 * coders in a special way.
 * </p>
 * <p>
 * An application can create and install a customized <code>CoderManager</code>
 * as follows: <blockquote>
 * 
 * <pre>
 * CoderManager myCoderManager = new MyCoderManager(Persistit.getInstance().getCoderManager());
 * Persistit.getInstance().setCoderManager(myCoderManager);
 * </pre>
 * 
 * </blockquote> Note that the <code>MyCoderManager</code> instance receives the
 * previously installed <code>CoderManager</code> as an argument of its
 * constructor. The new <code>CoderManager</code> should return the previously
 * installed one as the value of its {@link #getParent} method, and the
 * {@link #lookupKeyCoder} and {@link #lookupValueCoder} should delegate any
 * lookups not handled by the customized <code>CoderManager</code> back to the
 * parent.
 * </p>
 * <h3>Key Ordering</h3>
 * <p>
 * The encoding of key values determines the ordering of traversal for
 * {@link Exchange#traverse} and associated methods. See {@link Key} for
 * definition of the ordering among the types for which built-in encoding
 * exists. The ordering of key values based on objects that are encoded by
 * custom <code>KeyCoder</code>s is determined by the implementation of each
 * </code>KeyCoder</code>.
 * </p>
 * This leaves the question of how keys that may contain encoded objects of
 * different classes are ordered. For example, consider the code fragment shown
 * here: <blockquote>
 * 
 * <pre>
 *  Exchange exchange = new Exchange(...);
 *  Object a = new MyClass1();  // where MyClass1 and MyClass2
 *  Object b = new MyClass2();  // have registered KeyCoders
 *  exchange.to(a).store();     // store something with a's value as key
 *  exchange.to(b).store();     // store something with b's value as key
 *  exchange.to(Key.BEFORE)
 *  while (exchange.next())
 *  {
 *      System.out.println(exchange.indexTo(-1).getTypeName());
 *  }
 * </pre>
 * 
 * </blockquote> This code will insert two records incorporating the values
 * <code>a</code> and <code>b</code> into Persistit, and will then traverse the
 * resulting tree and print the names of the classes of the key values. The
 * question is whether "MyClass2" will be printed before or after "MyClass2".
 * </p>
 * <p>
 * The answer depends on the order in which MyClass1 and MyClass2 were first
 * registered within Persistit. At the time a new class is first registered,
 * Persistit assigns a permanent <code>int</code>-valued class handle to the
 * class. Whenever an instance of that class is encoded within either a
 * <code>Key</code> or a <code>Value</code>, that handle value, rather than the
 * class name, is used. This reduces disk, memory and CPU consumption whenever
 * Persistit stores or retrieves objects. Handles are assigned in increasing
 * numeric order as classes are registered. Thus if <code>MyClass1</code> was
 * registered <i>before</i> <code>MyClass2</code>, it will precede
 * <code>MyClass2</code> in traversal order. Otherwise it will follow
 * <code>MyClass2</code>.
 * </p>
 * 
 * @version 1.0
 */
public interface CoderManager {
    /**
     * Create a <code>Map</code> of <code>Class</code>es to registered
     * <code>ValueCoder</code>s. The map is an unmodifiable, is sorted by class
     * name, and does not change with subsequent registrations or
     * unregistrations of <code>ValueCoder</code>s.
     * 
     * @return <code>Map</code> of <code>Class</code>es to
     *         <code>ValueCoder</code>s.
     */
    public Map<Class<?>, ? extends ValueCoder> getRegisteredValueCoders();

    /**
     * Create a <code>Map</code> of <code>Class</code>es to registered
     * <code>KeyCoder</code> s. The map is an unmodifiable, and is sorted by
     * class name, and does not change with subsequent registrations or
     * unregistrations of <code>KeyCoder</code>s.
     * 
     * @return <code>Map</code> of <code>Class</code>es to <code>KeyCoder</code>
     *         s.
     */
    public Map<Class<?>, ? extends KeyCoder> getRegisteredKeyCoders();

    /**
     * Register the provided <code>KeyCoder</code> to encode and decode
     * instances of supplied class.
     * 
     * @param clazz
     *            The Class for which this <code>KeyCoder</code> will provide
     *            encoding and decoding services
     * @param coder
     *            The KeyCoder to register
     * @return The <code>KeyCoder</code> formerly registered for this
     *         <code>Class</code> , or <code>null</code> if there was none.
     */
    public KeyCoder registerKeyCoder(Class<?> clazz, KeyCoder coder);

    /**
     * Remove the registered <code>KeyCoder</code> for the supplied class.
     * 
     * @param clazz
     *            The <code>Class</code> from which to remove a
     *            <code>KeyCoder</code>, if present
     * @return The <code>KeyCoder</code> that was removed, or <code>null</code>
     *         if there was none.
     */
    public KeyCoder unregisterKeyCoder(Class<?> clazz);

    /**
     * Remove the supplied <code>KeyCoder</code> from all classes to which it
     * was previously registered.
     * 
     * @param coder
     *            The <code>KeyCoder</code> to remove.
     * @return The count of classes this <code>KeyCoder</code> was registered
     *         for.
     */
    public int unregisterKeyCoder(KeyCoder coder);

    /**
     * Returns the <code>KeyCoder</code> registered for the supplied
     * <code>Class</code>, or <code>null</code> if no <code>KeyCoder</code> is
     * registered.
     * 
     * @param clazz
     *            The <code>Class</code>
     * @return The <code>KeyCoder</code> or <code>null</code> if there is none.
     */
    public KeyCoder lookupKeyCoder(Class<?> clazz);

    /**
     * Register the provided <code>ValueCoder</code> to encode and decode
     * instances of supplied class.
     * 
     * @param clazz
     *            The Class for which this <code>ValueCoder</code> will provide
     *            encoding and decoding services
     * @param coder
     *            The ValueCoder to register
     * @return The <code>ValueCoder</code> that was removed, or
     *         <code>null</code> if there was none.
     */
    public ValueCoder registerValueCoder(Class<?> clazz, ValueCoder coder);

    /**
     * Remove the registered <code>ValueCoder</code> for the supplied class.
     * 
     * @param clazz
     *            The <code>Class</code> from which to remove a
     *            <code>ValueCoder</code>, if present
     * @return The <code>ValueCoder</code> that was removed, or
     *         <code>null</code> if there was none.
     */
    public ValueCoder unregisterValueCoder(Class<?> clazz);

    /**
     * Remove the supplied <code>ValueCoder</code> from all classes to which it
     * was previously registered.
     * 
     * @param coder
     *            The <code>ValueCoder</code> to remove.
     * @return The count of classes this <code>ValueCoder</code> was registered
     *         for.
     */
    public int unregisterValueCoder(ValueCoder coder);

    /**
     * Returns the <code>ValueCoder</code> registered for the supplied
     * <code>Class</code>, or <code>null</code> if no <code>ValueCoder</code> is
     * registered.
     * 
     * @param clazz
     *            The <code>Class</code>
     * @return The <code>ValueCoder</code> or <code>null</code> if there is
     *         none.
     */
    public ValueCoder lookupValueCoder(Class<?> clazz);

    /**
     * Return a <code>ValueCoder</code> for the supplied <code>Class</code>. If
     * there is none registered, implicitly create one and register it.
     * 
     * @param clazz
     * @return The ValueCoder
     */
    public ValueCoder getValueCoder(Class<?> clazz);

    /**
     * Return the parent <code>CoderManager</code> implementation, or
     * <code>null</code> if there is none.
     * 
     * @return The parent <code>CoderManager</code>
     */
    public CoderManager getParent();

}
