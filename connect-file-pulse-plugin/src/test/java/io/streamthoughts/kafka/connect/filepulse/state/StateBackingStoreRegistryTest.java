package io.streamthoughts.kafka.connect.filepulse.state;

import io.streamthoughts.kafka.connect.filepulse.storage.StateBackingStore;
import io.streamthoughts.kafka.connect.filepulse.storage.StateSnapshot;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

public class StateBackingStoreRegistryTest {

    private static final String TEST_STORE = "testGivenDefaultProperties-store";

    @Test
    public void test() {

        final StateBackingStoreRegistry registry = StateBackingStoreRegistry.instance();
        registry.register(TEST_STORE, MockStateBackingStore::new);

        StateBackingStore store = registry.get(TEST_STORE);
        registry.get(TEST_STORE);

        assertTrue(registry.has(TEST_STORE));
        registry.release(TEST_STORE);
        assertTrue(registry.has(TEST_STORE));
        assertFalse(((MockStateBackingStore)store).isStopped);

        registry.release(TEST_STORE);
        assertFalse(registry.has(TEST_STORE));
        assertTrue(((MockStateBackingStore)store).isStopped);
    }

    private static class MockStateBackingStore<T> implements StateBackingStore<T> {

        public boolean isStopped = false;

        @Override
        public void start() {

        }

        @Override
        public void stop() {
            isStopped = true;
        }

        @Override
        public StateSnapshot<T> snapshot() {
            return null;
        }

        @Override
        public boolean contains(String name) {
            return false;
        }

        @Override
        public void putAsync(String name, T state) {

        }

        @Override
        public void put(String name, T state) {

        }

        @Override
        public void remove(String name) {

        }

        @Override
        public void removeAsync(String name) {

        }

        @Override
        public void refresh(long timeout, TimeUnit unit) throws TimeoutException {

        }

        @Override
        public void setUpdateListener(UpdateListener listener) {

        }
    }

}