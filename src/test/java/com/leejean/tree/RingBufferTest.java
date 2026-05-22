package com.leejean.tree;

import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RingBuffer 单元测试 / RingBuffer unit tests.
 *
 * 1. 未满：size 正确，snapshot 含所有元素
 * 2. 刚满：size=capacity，isFull
 * 3. 溢出：size 不增长，最早元素被覆盖
 * 4. 容量 0 → IllegalArgumentException
 * 5. Java 序列化往返
 */
class RingBufferTest {

    @Test
    void underCapacityShouldTrackSize() {
        RingBuffer<Integer> rb = new RingBuffer<>(5);
        rb.add(10);
        rb.add(20);
        rb.add(30);

        assertEquals(3, rb.size());
        assertFalse(rb.isFull());
        assertEquals(5, rb.capacity());

        List<Integer> snap = rb.snapshot();
        assertEquals(3, snap.size());
        assertTrue(snap.contains(10));
        assertTrue(snap.contains(20));
        assertTrue(snap.contains(30));
    }

    @Test
    void exactCapacityShouldBeFull() {
        RingBuffer<Integer> rb = new RingBuffer<>(5);
        for (int i = 1; i <= 5; i++) {
            rb.add(i);
        }

        assertEquals(5, rb.size());
        assertTrue(rb.isFull());

        List<Integer> snap = rb.snapshot();
        assertEquals(5, snap.size());
        for (int i = 1; i <= 5; i++) {
            assertTrue(snap.contains(i));
        }
    }

    @Test
    void overflowShouldOverwriteOldest() {
        RingBuffer<Integer> rb = new RingBuffer<>(5);
        for (int i = 1; i <= 7; i++) {
            rb.add(i);
        }

        assertEquals(5, rb.size());
        assertTrue(rb.isFull());

        List<Integer> snap = rb.snapshot();
        assertEquals(5, snap.size());
        // 最早的 1、2 被覆盖 / oldest 1, 2 overwritten
        assertFalse(snap.contains(1));
        assertFalse(snap.contains(2));
        // 3~7 保留 / 3~7 retained
        for (int i = 3; i <= 7; i++) {
            assertTrue(snap.contains(i), "Should contain " + i);
        }
    }

    @Test
    void zeroCapacityShouldThrow() {
        assertThrows(IllegalArgumentException.class, () -> new RingBuffer<>(0));
    }

    @Test
    void negativeCapacityShouldThrow() {
        assertThrows(IllegalArgumentException.class, () -> new RingBuffer<>(-1));
    }

    @Test
    void peekOldestEmptyShouldThrow() {
        RingBuffer<Integer> rb = new RingBuffer<>(5);
        assertThrows(IllegalStateException.class, rb::peekOldest);
    }

    @Test
    void peekOldestPartiallyFilled() {
        RingBuffer<Integer> rb = new RingBuffer<>(5);
        rb.add(10);
        rb.add(20);
        rb.add(30);
        // 未满时最老的是 index=0 → 最先 add 的元素
        assertEquals(10, rb.peekOldest());
    }

    @Test
    void peekOldestWhenFull() {
        RingBuffer<Integer> rb = new RingBuffer<>(3);
        rb.add(1);
        rb.add(2);
        rb.add(3);
        // 满时 head=0，最老是 buffer[0]=1
        assertEquals(1, rb.peekOldest());

        // add 4 → 覆盖 1，head=1，最老变成 buffer[1]=2
        rb.add(4);
        assertEquals(2, rb.peekOldest());

        // add 5 → 覆盖 2，head=2，最老变成 buffer[2]=3
        rb.add(5);
        assertEquals(3, rb.peekOldest());
    }

    @SuppressWarnings("unchecked")
    @Test
    void serializationRoundTrip() throws Exception {
        RingBuffer<String> rb = new RingBuffer<>(4);
        rb.add("a");
        rb.add("b");
        rb.add("c");

        // 序列化 / serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(rb);
        }

        // 反序列化 / deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        RingBuffer<String> restored;
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            restored = (RingBuffer<String>) ois.readObject();
        }

        assertEquals(rb.size(), restored.size());
        assertEquals(rb.capacity(), restored.capacity());
        assertEquals(rb.isFull(), restored.isFull());

        List<String> origSnap = rb.snapshot();
        List<String> restoredSnap = restored.snapshot();
        assertEquals(origSnap, restoredSnap);
    }
}
