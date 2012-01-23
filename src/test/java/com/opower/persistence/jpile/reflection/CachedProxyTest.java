package com.opower.persistence.jpile.reflection;


import org.junit.Before;
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
    private static final String FOO = "foo";
    private static final String BAR = "bar";

    @Mock
    private MyInterface theImplementation;

    private MyInterface cached;

    @Before
    public void setUp() throws Exception {
        cached = CachedProxy.create(theImplementation);
    }

    @Test
    public void testWithVarArray() {
        when(theImplementation.doSomethingWithVarParams(1, true, "test")).thenReturn("results!");
        assertEquals("results!", cached.doSomethingWithVarParams(1, true, "test"));
        cached.doSomethingWithVarParams(1, true, "test"); // Call it again
        verify(theImplementation, times(1)).doSomethingWithVarParams(1, true, "test");
    }

    @Test
    public void testWithSingleParam() {
        when(theImplementation.doSomethingWithObject(FOO)).thenReturn(BAR);
        assertEquals(BAR, cached.doSomethingWithObject(FOO));
        cached.doSomethingWithObject(FOO);
        verify(theImplementation, times(1)).doSomethingWithObject(FOO);
    }

    @Test
    public void testReturningNull() {
        when(theImplementation.doSomethingWithObject(FOO)).thenReturn(null);
        assertNull(cached.doSomethingWithObject(FOO));
        cached.doSomethingWithObject(FOO);
        verify(theImplementation, times(1)).doSomethingWithObject(FOO);
    }

    private interface MyInterface {
        Object doSomethingWithVarParams(Object... args);

        Object doSomethingWithObject(Object o);
    }
}
