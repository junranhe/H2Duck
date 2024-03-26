package hdserver;

import java.util.HashMap;
import java.util.Map;

//用于单线程session中,不依赖额外线程的缓存
public class LruCache<K, V> {
    /**
     * 双向链表节点
     */
    static class LinkNode<K, V> {
        K key;
        V value;
        LinkNode<K, V> next;
        LinkNode<K, V> pre;

        public LinkNode() {
        }

        public LinkNode(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    /**
     * 缓存存储
     */
    Map<K, LinkNode<K, V>> cache;
    /**
     * 虚拟头结点
     */
    LinkNode<K, V> head;
    /**
     * 虚拟尾结点
     */
    LinkNode<K, V> tail;
    /**
     * 初始缓存的容量
     */
    int capacity;

    public LruCache(int capacity) {
        this.capacity = capacity;
        this.cache = new HashMap<K, LinkNode<K, V>>(capacity);
        this.head = new LinkNode<K, V>();
        this.tail = new LinkNode<K, V>();
        // 将虚拟头结点和尾结点相连
        this.head.next = this.tail;
        this.tail.pre = this.head;
    }

    public V get(K key) {
        LinkNode<K, V> node = cache.get(key);
        if (node != null) {
            // 把查询到的节点拿出来
            node.pre.next = node.next;
            node.next.pre = node.pre;
            node.pre = null;
            node.next = null;
            // 把查询到的节点放在第一个位置
            node.next = head.next;
            head.next.pre = node;
            head.next = node;
            node.pre = head;

            return node.value;
        }
        return null;
    }

    public V remove(K key) {
        LinkNode<K, V> node = cache.get(key);
        if (node != null) {
            // 把查询到的节点拿出来
            node.pre.next = node.next;
            node.next.pre = node.pre;
            node.pre = null;
            node.next = null;

            cache.remove(key);
            return node.value;
        }
        return null;
    }

    public void put(K key, V value) {
        LinkNode<K, V> node = cache.get(key);
        if (node != null) {
            // key存在更新对应的值
            node.value = value;
            // 把查询到的节点拿出来
            node.pre.next = node.next;
            node.next.pre = node.pre;
            node.pre = null;
            node.next = null;
            // 把查询到的节点放在第一个位置
            node.next = head.next;
            node.next.pre = node;
            head.next = node;
            node.pre = head;
        } else {
            // 缓存满，需要移出最近最少使用的节点
            // 即虚拟尾结点指向的节点
            if (cache.size() >= capacity) {
                // 找出最后一个节点
                LinkNode<K, V> tailPre = tail.pre;
                tailPre.pre.next = tail;
                tail.pre = tailPre.pre;
                // 释放最后一个几点
                tailPre.pre = null;
                tailPre.next = null;
                // 在缓存中删除这个节点
                cache.remove(tailPre.key);
            }
            // 新建节点放入缓存
            LinkNode<K, V> tmpNode = new LinkNode<K, V>(key, value);
            cache.put(key, tmpNode);
            // 把新建节点放入第一个位置
            tmpNode.next = head.next;
            head.next.pre = tmpNode;
            head.next = tmpNode;
            tmpNode.pre = head;
        }
    }
}
