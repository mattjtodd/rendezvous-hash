package com.mattjtodd.rendezvoushash;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

import com.google.common.base.Charsets;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RendezvousHash<K, N extends Comparable<? super N>> {

  private final HashFunction hasher;

  private final Funnel<? super K> keyFunnel;

  private final Funnel<? super N> nodeFunnel;

  private final ConcurrentSkipListSet<N> ordered;

  private static class HashNode<N> implements Comparable<HashNode<? super N>> {
    private final N node;
    private final long hash;

    public HashNode(N node, long hash) {
      this.node = checkNotNull(node);
      this.hash = hash;
    }

    public long getHash() {
      return hash;
    }

    public N getNode() {
      return node;
    }

    @Override
    public int compareTo(HashNode<? super N> o) {
      return Long.compare(hash, o.getHash());
    }
  }

  /**
   * Creates a new RendezvousHash with a starting set of nodes provided by init. The funnels will be used when generating the hash that combines the nodes and
   * keys. The hasher specifies the hashing algorithm to use.
   */
  public RendezvousHash(HashFunction hasher, Funnel<? super K> keyFunnel, Funnel<? super N> nodeFunnel, Collection<N> init) {
    this.hasher = checkNotNull(hasher);
    this.keyFunnel = checkNotNull(keyFunnel);
    this.nodeFunnel = checkNotNull(nodeFunnel);
    this.ordered = new ConcurrentSkipListSet<>(init);
  }

  public boolean add(N node) {
    return ordered.add(node);
  }

  public boolean remove(N node) {
    return ordered.remove(node);
  }

  public Optional<N> get(K key) {
    return ordered
        .stream()
        .map(node -> new HashNode<>(node, hash(key, node)))
        .max(HashNode::compareTo)
        .map(HashNode::getNode);
  }

  private long hash(K key, N node) {
    return hasher
        .newHasher()
        .putObject(key, keyFunnel)
        .putObject(node, nodeFunnel)
        .hash()
        .asLong();
  }
}
