/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
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
 * register all <tt>KeyCoder</tt> and <tt>ValueCoder</tt> implementations before
 * storing or accessing objects of the associated classes in Persistit.
 * </p>
 * <p>
 * When Persistit is initialized, a instance of {@link DefaultCoderManager} is
 * installed as the current <tt>CoderManager</tt>. Applications can refer to it
 * through the method {@link com.persistit.Persistit#getCoderManager}.
 * Applications should register <tt>KeyCoder</tt> and <tt>ValueCoder</tt>
 * instances as follows: <blockquote>
 * 
 * <pre>
 * ValueCoder vc1 = new MyClassValueCoder();
 * KeyCoder kc1 = new MyClassKeyCoder();
 * Persistit.getInstance().getCoderManager()
 *         .registerValueCoder(MyClass.class, kc1);
 * Persistit.getInstance().getCoderManager().registerKeyCoder(MyClass.class, kc1);
 * </pre>
 * 
 * </blockquote> Subsequently, any time the application appends an instance of a
 * <tt>MyClass</tt> to a <tt>Key</tt>, the <tt>MyClassKeyCoder</tt> will be used
 * to convert the interior state of the <tt>MyClass</tt> into a representation
 * within the <tt>Key</tt>. Likewise, whenever the application puts an object of
 * class <tt>MyClass</tt> into a <tt>Value</tt>, the registered
 * <tt>MyClassValueCoder</tt> will be used to encode it. Persistit uses the
 * {@link #lookupKeyCoder} and {@link #lookupValueCoder} methods to acquire the
 * appropriate coder.
 * </p>
 * <p>
 * An application may replace the default implementation provided by
 * <tt>DefaultCoderManager</tt>. Such a custom implementation might, for
 * example, handle the relationship between certain classes and their associated
 * coders in a special way.
 * </p>
 * <p>
 * An application can create and install a customized <tt>CoderManager</tt> as
 * follows: <blockquote>
 * 
 * <pre>
 * CoderManager myCoderManager = new MyCoderManager(Persistit.getInstance()
 *         .getCoderManager());
 * Persistit.getInstance().setCoderManager(myCoderManager);
 * </pre>
 * 
 * </blockquote> Note that the <tt>MyCoderManager</tt> instance receives the
 * previously installed <tt>CoderManager</tt> as an argument of its constructor.
 * The new <tt>CoderManager</tt> should return the previously installed one as
 * the value of its {@link #getParent} method, and the {@link #lookupKeyCoder}
 * and {@link #lookupValueCoder} should delegate any lookups not handled by the
 * customized <tt>CoderManager</tt> back to the parent.
 * </p>
 * <h3>Key Ordering</h3>
 * <p>
 * The encoding of key values determines the ordering of traversal for
 * {@link Exchange#traverse} and associated methods. See {@link Key} for
 * definition of the ordering among the types for which built-in encoding
 * exists. The ordering of key values based on objects that are encoded by
 * custom <tt>KeyCoder</tt>s is determined by the implementation of each
 * </tt>KeyCoder</tt>.
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
 * <tt>a</tt> and <tt>b</tt> into Persistit, and will then traverse the
 * resulting tree and print the names of the classes of the key values. The
 * question is whether "MyClass2" will be printed before or after "MyClass2".
 * </p>
 * <p>
 * The answer depends on the order in which MyClass1 and MyClass2 were first
 * registered within Persistit. At the time a new class is first registered,
 * Persistit assigns a permanent <tt>int</tt>-valued class handle to the class.
 * Whenever an instance of that class is encoded within either a <tt>Key</tt> or
 * a <tt>Value</tt>, that handle value, rather than the class name, is used.
 * This reduces disk, memory and CPU consumption whenever Persistit stores or
 * retrieves objects. Handles are assigned in increasing numeric order as
 * classes are registered. Thus if <tt>MyClass1</tt> was registered
 * <i>before</i> <tt>MyClass2</tt>, it will precede <tt>MyClass2</tt> in
 * traversal order. Otherwise it will follow <tt>MyClass2</tt>.
 * </p>
 * 
 * @version 1.0
 */
public interface CoderManager {
    /**
     * Create a <tt>Map</tt> of <tt>Class</tt>es to registered
     * <tt>ValueCoder</tt>s. The map is an unmodifiable, is sorted by class
     * name, and does not change with subsequent registrations or
     * unregistrations of <tt>ValueCoder</tt>s.
     * 
     * @return <tt>Map</tt> of <tt>Class</tt>es to <tt>ValueCoder</tt>s.
     */
    public Map getRegisteredValueCoders();

    /**
     * Create a <tt>Map</tt> of <tt>Class</tt>es to registered <tt>KeyCoder</tt>
     * s. The map is an unmodifiable, and is sorted by class name, and does not
     * change with subsequent registrations or unregistrations of
     * <tt>KeyCoder</tt>s.
     * 
     * @return <tt>Map</tt> of <tt>Class</tt>es to <tt>KeyCoder</tt>s.
     */
    public Map getRegisteredKeyCoders();

    /**
     * Register the provided <tt>KeyCoder</tt> to encode and decode instances of
     * supplied class.
     * 
     * @param clazz
     *            The Class for which this <tt>KeyCoder</tt> will provide
     *            encoding and decoding services
     * @param coder
     *            The KeyCoder to register
     * @return The <tt>KeyCoder</tt> formerly registered for this <tt>Class</tt>
     *         , or <tt>null</tt> if there was none.
     */
    public KeyCoder registerKeyCoder(Class clazz, KeyCoder coder);

    /**
     * Remove the registered <tt>KeyCoder</tt> for the supplied class.
     * 
     * @param clazz
     *            The <tt>Class</tt> from which to remove a <tt>KeyCoder</tt>,
     *            if present
     * @return The <tt>KeyCoder</tt> that was removed, or <tt>null</tt> if there
     *         was none.
     */
    public KeyCoder unregisterKeyCoder(Class clazz);

    /**
     * Remove the supplied <tt>KeyCoder</tt> from all classes to which it was
     * previously registered.
     * 
     * @param coder
     *            The <tt>KeyCoder</tt> to remove.
     * @return The count of classes this <tt>KeyCoder</tt> was registered for.
     */
    public int unregisterKeyCoder(KeyCoder coder);

    /**
     * Returns the <tt>KeyCoder</tt> registered for the supplied <tt>Class</tt>,
     * or <tt>null</tt> if no <tt>KeyCoder</tt> is registered.
     * 
     * @param clazz
     *            The <tt>Class</tt>
     * @return The <tt>KeyCoder</tt> or <tt>null</tt> if there is none.
     */
    public KeyCoder lookupKeyCoder(Class clazz);

    /**
     * Register the provided <tt>ValueCoder</tt> to encode and decode instances
     * of supplied class.
     * 
     * @param clazz
     *            The Class for which this <tt>ValueCoder</tt> will provide
     *            encoding and decoding services
     * @param coder
     *            The ValueCoder to register
     * @return The <tt>ValueCoder</tt> that was removed, or <tt>null</tt> if
     *         there was none.
     */
    public ValueCoder registerValueCoder(Class clazz, ValueCoder coder);

    /**
     * Remove the registered <tt>ValueCoder</tt> for the supplied class.
     * 
     * @param clazz
     *            The <tt>Class</tt> from which to remove a <tt>ValueCoder</tt>,
     *            if present
     * @return The <tt>ValueCoder</tt> that was removed, or <tt>null</tt> if
     *         there was none.
     */
    public ValueCoder unregisterValueCoder(Class clazz);

    /**
     * Remove the supplied <tt>ValueCoder</tt> from all classes to which it was
     * previously registered.
     * 
     * @param coder
     *            The <tt>ValueCoder</tt> to remove.
     * @return The count of classes this <tt>ValueCoder</tt> was registered for.
     */
    public int unregisterValueCoder(ValueCoder coder);

    /**
     * Returns the <tt>ValueCoder</tt> registered for the supplied
     * <tt>Class</tt>, or <tt>null</tt> if no <tt>ValueCoder</tt> is registered.
     * 
     * @param clazz
     *            The <tt>Class</tt>
     * @return The <tt>ValueCoder</tt> or <tt>null</tt> if there is none.
     */
    public ValueCoder lookupValueCoder(Class clazz);

    /**
     * Return a <tt>ValueCoder</tt> for the supplied <tt>Class</tt>. If there is
     * none registered, implicitly create one and register it.
     * 
     * @param clazz
     * @return The ValueCoder
     */
    public ValueCoder getValueCoder(Class clazz);

    /**
     * Return the parent <tt>CoderManager</tt> implementation, or <tt>null</tt>
     * if there is none.
     * 
     * @return The parent <tt>CoderManager</tt>
     */
    public CoderManager getParent();

}
