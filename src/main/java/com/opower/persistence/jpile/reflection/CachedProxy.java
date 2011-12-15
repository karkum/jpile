package com.opower.persistence.jpile.reflection;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;

/**
 * Creates an intermediate proxy object for a given interface. All calls are cached. See
 * <a href="https://github.com/martinus/java-playground/blob/master/src/java/com/ankerl/proxy/CachedProxy.java">CachedProxy</a>
 * for the original idea.
 *
 * @author Martin Ankerl (martin.ankerl@gmail.at)
 * @author amir.raminfar
 */
public final class CachedProxy {
    private CachedProxy() {
    }

    /**
     * Private class with all the required parameters for a method
     */
    private static final class Args {
        private final Method method;
        private final Object[] args;
        private final int hash;

        public Args(final Method method, final Object[] args) {
            this.method = method;
            this.args = args;
            this.hash = Objects.hashCode(method, args);
        }

        @Override
        public boolean equals(final Object obj) {
            if(obj instanceof Args) {
                Args other = (Args) obj;
                return Objects.equal(this.method, other.method) && Objects.equal(this.args, other.args);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    /**
     * Creates an intermediate proxy object that uses cached results if
     * available, otherwise calls the given code.
     *
     * @param <T>  Type of the class.
     * @param cl   The interface for which the proxy should be created.
     * @param code The actual calculation code that should be cached.
     * @return The proxy.
     */
    @SuppressWarnings("unchecked")
    public static <T> T create(final Class<T> cl, final T code) {
        return (T) Proxy.newProxyInstance(cl.getClassLoader(), new Class[]{cl}, new InvocationHandler() {
            final Cache<Args, Optional> cache = CacheBuilder
                    .newBuilder()
                    .softValues()
                    .build(new CacheLoader<Args, Optional>() {
                        @Override
                        public Optional load(Args key) throws Exception {
                            return Optional.fromNullable(key.method.invoke(code, key.args));
                        }
                    });

            @Override
            public Object invoke(final Object proxy, final Method method, final Object[] params) throws Throwable {
                return this.cache.getUnchecked(new Args(method, params)).orNull();
            }
        });
    }
}
