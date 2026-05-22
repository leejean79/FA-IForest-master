package com.leejean.tree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 固定大小的环形缓冲区，FIFO 覆盖语义 / Fixed-size ring buffer with FIFO overwrite.
 *
 * <p>填充阶段（size < capacity）：每次 add 直接追加。
 * 满后：每次 add 覆盖最老的元素，size 不再增长。
 *
 * <p>用途 / Use case: 维护一个"最近 N 条"的滑动窗口，用于 iForest 子样本采集。
 *
 * @param <T> 元素类型
 */
public class RingBuffer<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int capacity;
    private final Object[] buffer;
    private int head;        // 下一个写入位置
    private int size;        // 当前已存元素数（≤ capacity）

    public RingBuffer(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0, got " + capacity);
        }
        this.capacity = capacity;
        this.buffer = new Object[capacity];
        this.head = 0;
        this.size = 0;
    }

    /** 添加元素（必要时覆盖最老的）/ Add element, overwriting oldest if full. */
    public void add(T element) {
        buffer[head] = element;
        head = (head + 1) % capacity;
        if (size < capacity) {
            size++;
        }
    }

    public int size() {
        return size;
    }

    public int capacity() {
        return capacity;
    }

    public boolean isFull() {
        return size == capacity;
    }

    /**
     * 返回当前最老的元素（即下一次 add 满时会被覆盖的那个）。
     * Returns the oldest element currently in the buffer (the one to be overwritten next).
     * @throws IllegalStateException if buffer is empty
     */
    @SuppressWarnings("unchecked")
    public T peekOldest() {
        if (size == 0) {
            throw new IllegalStateException("buffer is empty");
        }
        if (size < capacity) {
            // 缓冲未满：最老的是 index=0 的元素
            return (T) buffer[0];
        }
        // 缓冲已满：head 指向下一个写入位置，也就是最老元素的位置
        return (T) buffer[head];
    }

    /** 当前所有元素的快照（按未指定顺序）/ Snapshot of all current elements. */
    @SuppressWarnings("unchecked")
    public List<T> snapshot() {
        List<T> out = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            out.add((T) buffer[i]);
        }
        return out;
    }
}
