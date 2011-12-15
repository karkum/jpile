package com.opower.persistence.jpile.reflection;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test caching to make sure it works correctly
 *
 * @author amir.raminfar
 */
@RunWith(MockitoJUnitRunner.class)
public class CachedProxyTest {
    @Mock
    private MyInterface theImplementation;

    @Test
    public void testWithVarArray() {
        when(theImplementation.doSomethingWithVarParams(1, true, "test")).thenReturn("results!");
        MyInterface cached = CachedProxy.create(MyInterface.class, theImplementation);
        assertEquals("results!", cached.doSomethingWithVarParams(1, true, "test"));
        cached.doSomethingWithVarParams(1, true, "test"); // Call it again
        verify(theImplementation, times(1)).doSomethingWithVarParams(1, true, "test");
    }

    @Test
    public void testWithSingleParam() {
        when(theImplementation.doSomethingWithObject("foo")).thenReturn("bar");
        MyInterface cached = CachedProxy.create(MyInterface.class, theImplementation);
        assertEquals("bar", cached.doSomethingWithObject("foo"));
        cached.doSomethingWithObject("foo");
        verify(theImplementation, times(1)).doSomethingWithObject("foo");
    }

    @Test
    public void testReturningNull() {
        when(theImplementation.doSomethingWithObject("foo")).thenReturn(null);
        MyInterface cached = CachedProxy.create(MyInterface.class, theImplementation);
        assertNull(cached.doSomethingWithObject("foo"));
        cached.doSomethingWithObject("foo");
        verify(theImplementation, times(1)).doSomethingWithObject("foo");
    }

    private interface MyInterface {
        Object doSomethingWithVarParams(Object... args);

        Object doSomethingWithObject(Object o);
    }
}
