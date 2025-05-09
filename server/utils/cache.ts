/**
 * Cache Utilities
 * 
 * This module provides in-memory caching mechanisms to reduce database and API calls.
 */

interface CacheOptions {
  ttl?: number; // Time-to-live in milliseconds
}

/**
 * Simple in-memory cache implementation with TTL
 */
export class SimpleCache<T> {
  private cache: Map<string, { value: T; expiry: number }>;
  private defaultTtl: number; // Time-to-live in milliseconds
  
  /**
   * Create a new cache instance
   * 
   * @param defaultTtl Default time-to-live in milliseconds (default: 1 hour)
   */
  constructor(defaultTtl: number = 60 * 60 * 1000) {
    this.cache = new Map();
    this.defaultTtl = defaultTtl;
  }

  /**
   * Set a value in the cache
   * 
   * @param key Cache key
   * @param value Value to store
   * @param options Cache options (e.g., ttl)
   */
  set(key: string, value: T, options: CacheOptions = {}): void {
    const ttl = options.ttl || this.defaultTtl;
    const expiry = Date.now() + ttl;
    
    this.cache.set(key, { value, expiry });
  }

  /**
   * Get a value from the cache
   * 
   * @param key Cache key
   * @returns The cached value, or undefined if not found or expired
   */
  get(key: string): T | undefined {
    const item = this.cache.get(key);
    
    if (!item) {
      return undefined;
    }
    
    // Check if the item has expired
    if (item.expiry < Date.now()) {
      this.cache.delete(key);
      return undefined;
    }
    
    return item.value;
  }

  /**
   * Delete a value from the cache
   * 
   * @param key Cache key
   */
  delete(key: string): void {
    this.cache.delete(key);
  }

  /**
   * Clear all values from the cache
   */
  clear(): void {
    this.cache.clear();
  }

  /**
   * Get all valid (non-expired) keys in the cache
   * 
   * @returns Array of cache keys
   */
  keys(): string[] {
    const now = Date.now();
    const keys: string[] = [];
    
    for (const [key, item] of this.cache.entries()) {
      if (item.expiry >= now) {
        keys.push(key);
      } else {
        // Clean up expired items as we iterate
        this.cache.delete(key);
      }
    }
    
    return keys;
  }
  
  /**
   * Get the number of valid items in the cache
   * 
   * @returns Number of valid items
   */
  size(): number {
    return this.keys().length;
  }
}

// Create cache instances with different TTLs

// 1 hour TTL for price data
export const priceCache = new SimpleCache<number>(60 * 60 * 1000);

// 6 hour TTL for difficulty data (changes less frequently)
export const difficultyCache = new SimpleCache<number>(6 * 60 * 60 * 1000);

// 24 hour TTL for complex calculation results
export const calculationCache = new SimpleCache<any>(24 * 60 * 60 * 1000);

// 5 minute TTL for frequently updated data
export const frequentCache = new SimpleCache<any>(5 * 60 * 1000);