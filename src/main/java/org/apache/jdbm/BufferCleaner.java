package org.apache.jdbm;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * Due to <a href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038">a known
 * bug</a> in the Oracle Java implementation, OS memory blocks behind direct memory
 * buffers are not immediately released. This may block the handle of the underlying OS
 * file until the GC finally releases the buffer. As a result, operations like deleting
 * and recreating memory mapped files may fail non-deterministically.
 * <p>
 * This class provides explicit cleanup functionality that workarounds this behavior but
 * it requires access to internal Java API to do so. It supports both the new Java 9 way
 * of cleaning (see <code>sun.misc.Unsafe.invokeCleaner()</code>) and the old Java 7/8
 * way of doing this (see <code>sun.misc.Cleaner</code>).
 *
 * @author schaloms@gmx.de
 */
public final class BufferCleaner {
    private static final MethodHandle CLEANER;
    private static final Throwable LOOKUP_ERROR;

    static {
        MethodHandle cleaner = null;
        Throwable err = null;
        try {
            cleaner = lookupCleaner();
        } catch (ReflectiveOperationException | RuntimeException e) {
            err = e;
        }
        CLEANER = cleaner;
        LOOKUP_ERROR = err;
    }

    /**
     * Construction not permitted.
     */
    private BufferCleaner() {
    }

    /**
     * Checks if buffer cleanup is supported in the currently running JVM.
     *
     * @return <code>true</code> if cleanup is supported, <code>false</code> otherwise
     */
    public static boolean isSupported() {
        return CLEANER != null;
    }

    /**
     * Runs the buffer cleanup strategy that was prevously determined for the current JVM.
     *
     * @param buffer  the buffer to cleanup
     *
     * @throws IOException  in case of an error during cleanup or if cleanup is not
     *                      supported for the current JVM
     */
    public static void clean( MappedByteBuffer buffer ) throws IOException {
        if(buffer == null || !buffer.isDirect()) {
            throw new IllegalArgumentException("Not a valid direct buffer.");
        }
        if (CLEANER == null) {
            throw new IOException("Buffer cleaning not supported.", LOOKUP_ERROR);
        }
        try {
            CLEANER.invoke(buffer);
        } catch (Throwable t) {
            throw new IOException("Failed to unmap buffer.", t);
        }
    }

    /**
     * Finds the right buffer cleanup strategy for the currently running JVM and creates
     * a dynamic method that executes the strategy.
     *
     * @return the cleanup method
     *
     * @throws ReflectiveOperationException  in case of an error
     */
    private static MethodHandle lookupCleaner() throws ReflectiveOperationException {
        MethodHandle cleaner = tryLookupJava9Cleaner();
        if (cleaner != null) {
            return cleaner;
        }
        return lookupLegacyCleaner();
    }


    /**
     * Checks if <code>sun.misc.Unsafe</code> supports the new Java 9 buffer cleanup API
     * and if that's the case, creates dynamic method that, when called, performs cleanup
     * this way:
     * <p>
     * <blockquote><pre>
     * sun.misc.Unsafe unsafe = sun.misc.Unsafe.theUnsafe;
     * unsafe.invokeClean(&lt;buffer&gt;);
     * </pre></blockquote>
     *
     * @return a handle for cleanup method or <code>null</code> if this kind of cleanup
     *         strategy is not available in the current JVM
     *
     * @throws ReflectiveOperationException when reflective access to <code>Unsafe</code> is not possible
     */
    private static MethodHandle tryLookupJava9Cleaner() throws ReflectiveOperationException {
        Lookup lookup = MethodHandles.lookup();
        Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
        MethodHandle invokeCleanHandle;
        try {
            invokeCleanHandle = lookup.findVirtual(unsafeClass, "invokeCleaner", MethodType.methodType(void.class, ByteBuffer.class));
        } catch (NoSuchMethodException e) {
            // we're probably running under an older Java version
            return null;
        }
        // Unsafe.getUnsafe() is filtered from reflection since Java 8, we need to lookup the corresponding field
        Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
        boolean accessible = unsafeField.isAccessible();
        unsafeField.setAccessible(true);
        MethodHandle unsafeFieldHandle = lookup.unreflectGetter(unsafeField);
        unsafeField.setAccessible(accessible);
        return MethodHandles.foldArguments(invokeCleanHandle, unsafeFieldHandle);
    }

    /**
     * Creates dynamic method that, when called, performs buffer cleanup this way:
     * <p>
     * <blockquote><pre>
     * sun.misc.Cleaner cleaner = ((java.nio.DirectByteBuffer) &lt;buffer&gt;).cleaner();
     * if(BufferCleaner.isValidCleaner(cleaner)) {
     *     cleaner.clean();
     * } else {
     *     BufferCleaner.onInvalidCleaner(cleaner);
     * }
     * </pre></blockquote>
     * <p>
     * NOTE: this method as well as the created cleanup method will only work under
     * Java 7 or 8.
     *
     * @return a handle to the cleanup method
     *
     * @throws ReflectiveOperationException in case of an error
     */
    private static MethodHandle lookupLegacyCleaner() throws ReflectiveOperationException {
        Lookup lookup = MethodHandles.lookup();
        Class<?> bufferClass = Class.forName("java.nio.DirectByteBuffer");
        Method cleanerMethod = bufferClass.getMethod("cleaner");
        boolean accessible = cleanerMethod.isAccessible();
        cleanerMethod.setAccessible(true);
        MethodHandle cleanerHandle = lookup.unreflect(cleanerMethod);
        cleanerMethod.setAccessible(accessible);
        Class<?> cleanerClass = cleanerHandle.type().returnType();
        MethodHandle cleanHandle = lookup.findVirtual(cleanerClass, "clean", MethodType.methodType(void.class));
        MethodHandle isValidCleanerHandle = lookup.findStatic(BufferCleaner.class, "isValidCleaner", MethodType.methodType(boolean.class, Object.class, Class.class))
                                                  .asType(MethodType.methodType(boolean.class, cleanerClass, Class.class));
        MethodHandle configuredIsValidCleanerHandle = MethodHandles.insertArguments(isValidCleanerHandle, 1, cleanerClass);
        MethodHandle onInvalidCleanerHandle = lookup.findStatic(BufferCleaner.class, "onInvalidCleaner", MethodType.methodType(void.class, Object.class))
                                                    .asType(MethodType.methodType(void.class, cleanerClass));
        // we need a guard block here as some buffers may not contain a valid cleaner object
        return MethodHandles.filterReturnValue(cleanerHandle, MethodHandles.guardWithTest(configuredIsValidCleanerHandle, cleanHandle, onInvalidCleanerHandle));
    }

    /**
     * Checks if the given object is an instance of the given cleaner class.
     *
     * @param obj           the object to check
     * @param cleanerClass  the expected cleaner class
     *
     * @return <code>true</code> if the object is not <code>null</code> and an instance
     *         of the given class, <code>false</code> otherwise
     */
    @SuppressWarnings("unused")
    private static boolean isValidCleaner(Object obj, Class<?> cleanerClass) {
        return cleanerClass.isInstance( obj );
    }

    /**
     * Called when a cleaner object was detected that doesn't pass the test(s) of
     * the {@link #isValidCleaner(Object, Class)}. Currently only required to be able to
     * construct a dynamic guard block using <code>MethodHandles.guardWithTest</code>.
     *
     * @param obj  the invalid cleaner instance or <code>null</code>
     */
    @SuppressWarnings("unused")
    private static void onInvalidCleaner(Object obj) {
        // do nothing
    }
}
