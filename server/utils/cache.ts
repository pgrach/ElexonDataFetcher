/**
 * In-memory cache management for Bitcoin Mining Analytics platform
 * 
 * This module provides a configurable caching system to improve performance
 * by reducing database queries for frequently accessed data.
 */

import { logger } from './logger';

/**
 * Base entry for all cache items
 */
interface CacheEntry<T> {
  value: T;
  timestamp: number;
  ttl: number;
}

/**
 * Generic in-memory cache implementation
 */
export class Cache<T> {
  private cache: Map<string, CacheEntry<T>> = new Map();
  private name: string;
  private defaultTtl: number;
  private maxSize: number;
  private cleanupInterval: NodeJS.Timeout | null = null;
  
  /**
   * Create a new cache instance
   * 
   * @param name - Cache identifier for logging
   * @param defaultTtl - Default time to live in seconds (0 = no expiration)
   * @param maxSize - Maximum number of entries before eviction (0 = unlimited)
   * @param cleanupIntervalSec - How often to run cleanup (0 = manual only)
   */
  constructor(
    name: string,
    defaultTtl: number = 300,
    maxSize: number = 1000,
    cleanupIntervalSec: number = 60
  ) {
    this.name = name;
    this.defaultTtl = defaultTtl * 1000;   // Convert to milliseconds
    this.maxSize = maxSize;
    
    // Setup automatic cleanup if interval > 0
    if (cleanupIntervalSec > 0) {
      this.cleanupInterval = setInterval(
        () => this.cleanup(),
        cleanupIntervalSec * 1000
      );
    }
  }
  
  /**
   * Set a value in the cache
   */
  set(key: string, value: T, ttl: number = this.defaultTtl): void {
    // Check if cache is at max capacity
    if (this.maxSize > 0 && this.cache.size >= this.maxSize && !this.cache.has(key)) {
      this.evictOldest();
    }
    
    this.cache.set(key, {
      value,
      timestamp: Date.now(),
      ttl
    });
  }
  
  /**
   * Get a value from the cache
   * Returns undefined if not found or expired
   */
  get(key: string): T | undefined {
    const entry = this.cache.get(key);
    
    if (!entry) {
      return undefined;
    }
    
    // Check if entry is expired
    if (entry.ttl > 0 && Date.now() - entry.timestamp > entry.ttl) {
      this.cache.delete(key);
      return undefined;
    }
    
    return entry.value;
  }
  
  /**
   * Check if key exists in cache and is valid
   */
  has(key: string): boolean {
    return this.get(key) !== undefined;
  }
  
  /**
   * Delete a value from the cache
   */
  delete(key: string): boolean {
    return this.cache.delete(key);
  }
  
  /**
   * Clear all values from the cache
   */
  clear(): void {
    this.cache.clear();
  }
  
  /**
   * Get all valid keys in the cache
   */
  keys(): string[] {
    // Return only non-expired keys
    const validKeys: string[] = [];
    const now = Date.now();
    
    this.cache.forEach((entry, key) => {
      if (entry.ttl === 0 || now - entry.timestamp <= entry.ttl) {
        validKeys.push(key);
      }
    });
    
    return validKeys;
  }
  
  /**
   * Get the number of items in the cache
   */
  size(): number {
    return this.cache.size;
  }
  
  /**
   * Get or set cache value with callback
   * This is useful for implementing memoization patterns
   */
  async getOrSet(
    key: string,
    fetchCallback: () => Promise<T>,
    ttl: number = this.defaultTtl
  ): Promise<T> {
    // Check if value is already cached
    const cachedValue = this.get(key);
    if (cachedValue !== undefined) {
      return cachedValue;
    }
    
    try {
      // Fetch value using callback
      const value = await fetchCallback();
      
      // Store in cache
      this.set(key, value, ttl);
      
      return value;
    } catch (error) {
      logger.error(`Cache fetch failed for key '${key}'`, {
        module: 'cache',
        context: { cacheName: this.name, key },
        error: error as Error
      });
      throw error;
    }
  }
  
  /**
   * Remove expired items from cache
   */
  cleanup(): number {
    const now = Date.now();
    let removedCount = 0;
    
    // Use forEach to iterate through cache entries
    this.cache.forEach((entry, key) => {
      if (entry.ttl > 0 && now - entry.timestamp > entry.ttl) {
        this.cache.delete(key);
        removedCount++;
      }
    });
    
    if (removedCount > 0) {
      logger.debug(`Cleaned up ${removedCount} expired items from ${this.name} cache`, {
        module: 'cache',
        context: {
          cacheName: this.name,
          remainingItems: this.cache.size
        }
      });
    }
    
    return removedCount;
  }
  
  /**
   * Destroy the cache and cleanup resources
   */
  destroy(): void {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null;
    }
    
    this.clear();
  }
  
  /**
   * Evict the oldest item from the cache
   */
  private evictOldest(): void {
    let oldestKey: string | null = null;
    let oldestTime = Infinity;
    
    // Use forEach to find the oldest item
    this.cache.forEach((entry, key) => {
      if (entry.timestamp < oldestTime) {
        oldestTime = entry.timestamp;
        oldestKey = key;
      }
    });
    
    if (oldestKey) {
      this.cache.delete(oldestKey);
      logger.debug(`Evicted oldest item from ${this.name} cache: ${oldestKey}`, {
        module: 'cache'
      });
    }
  }
}

// Create and export commonly used caches
export const difficultyCache = new Cache<number>('difficulty', 3600);  // 1 hour TTL
export const priceCache = new Cache<number>('price', 300);            // 5 minutes TTL
export const farmDataCache = new Cache<any>('farmData', 1800);        // 30 minutes TTL
export const calculationCache = new Cache<any>('calculations', 600);  // 10 minutes TTL