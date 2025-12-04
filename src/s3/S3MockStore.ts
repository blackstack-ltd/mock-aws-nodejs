import type { ChecksumAlgorithm, ObjectCannedACL, StorageClass } from '@aws-sdk/client-s3';
import { createHash } from 'crypto';
import type { MockStore } from '../core/types';

/**
 * Represents a stored S3 object with all metadata
 */
export interface StoredObject {
  /** The object key */
  key: string;
  /** The object body as a Buffer */
  body: Buffer;
  /** Content-Type header */
  contentType?: string;
  /** Content-Encoding header */
  contentEncoding?: string;
  /** Content-Language header */
  contentLanguage?: string;
  /** Content-Disposition header */
  contentDisposition?: string;
  /** Cache-Control header */
  cacheControl?: string;
  /** Expires header */
  expires?: Date;
  /** User-defined metadata */
  metadata?: Record<string, string>;
  /** ETag (MD5 hash of the object) */
  etag: string;
  /** Last modified timestamp */
  lastModified: Date;
  /** Content length in bytes */
  contentLength: number;
  /** Storage class */
  storageClass?: StorageClass;
  /** Server-side encryption */
  serverSideEncryption?: string;
  /** SSE-KMS key ID */
  sseKmsKeyId?: string;
  /** Bucket key enabled */
  bucketKeyEnabled?: boolean;
  /** Version ID (if versioning enabled) */
  versionId?: string;
  /** ACL */
  acl?: ObjectCannedACL;
  /** Checksum algorithm */
  checksumAlgorithm?: ChecksumAlgorithm;
  /** Checksum CRC32 */
  checksumCRC32?: string;
  /** Checksum CRC32C */
  checksumCRC32C?: string;
  /** Checksum SHA1 */
  checksumSHA1?: string;
  /** Checksum SHA256 */
  checksumSHA256?: string;
  /** Tags */
  tagging?: string;
}

/**
 * Represents a stored S3 bucket
 */
export interface StoredBucket {
  /** Bucket name */
  name: string;
  /** Creation date */
  creationDate: Date;
  /** Region */
  region?: string;
  /** Versioning enabled */
  versioningEnabled?: boolean;
}

/**
 * In-memory store for S3 mock data.
 * Manages buckets and objects for the mock S3 service.
 */
export class S3MockStore implements MockStore {
  private buckets: Map<string, StoredBucket> = new Map();
  private objects: Map<string, Map<string, StoredObject>> = new Map();

  /**
   * Create a bucket
   */
  createBucket(name: string, region?: string): StoredBucket {
    if (this.buckets.has(name)) {
      throw new Error(`Bucket already exists: ${name}`);
    }

    const bucket: StoredBucket = {
      name,
      creationDate: new Date(),
      region,
    };

    this.buckets.set(name, bucket);
    this.objects.set(name, new Map());
    return bucket;
  }

  /**
   * Get a bucket by name
   */
  getBucket(name: string): StoredBucket | undefined {
    return this.buckets.get(name);
  }

  /**
   * Check if a bucket exists
   */
  bucketExists(name: string): boolean {
    return this.buckets.has(name);
  }

  /**
   * Delete a bucket
   */
  deleteBucket(name: string): boolean {
    const objects = this.objects.get(name);
    if (objects && objects.size > 0) {
      throw new Error(`Bucket is not empty: ${name}`);
    }

    this.objects.delete(name);
    return this.buckets.delete(name);
  }

  /**
   * List all buckets
   */
  listBuckets(): StoredBucket[] {
    return Array.from(this.buckets.values());
  }

  /**
   * Ensure a bucket exists, creating it if it doesn't
   * This is useful for tests that don't want to explicitly create buckets
   */
  ensureBucket(name: string): StoredBucket {
    if (!this.buckets.has(name)) {
      return this.createBucket(name);
    }
    return this.buckets.get(name)!;
  }

  /**
   * Put an object into a bucket
   */
  putObject(
    bucket: string,
    key: string,
    body: Buffer | string | Uint8Array,
    options: Partial<Omit<StoredObject, 'key' | 'body' | 'etag' | 'lastModified' | 'contentLength'>> = {}
  ): StoredObject {
    // Auto-create bucket if it doesn't exist (for ease of testing)
    this.ensureBucket(bucket);

    const bucketObjects = this.objects.get(bucket);
    if (!bucketObjects) {
      throw new Error(`Bucket does not exist: ${bucket}`);
    }

    // Convert body to Buffer
    let bodyBuffer: Buffer;
    if (Buffer.isBuffer(body)) {
      bodyBuffer = body;
    } else if (typeof body === 'string') {
      bodyBuffer = Buffer.from(body, 'utf-8');
    } else if (body instanceof Uint8Array) {
      bodyBuffer = Buffer.from(body);
    } else {
      bodyBuffer = Buffer.from(String(body), 'utf-8');
    }

    // Calculate ETag (MD5 hash)
    const etag = `"${createHash('md5').update(bodyBuffer).digest('hex')}"`;

    const object: StoredObject = {
      key,
      body: bodyBuffer,
      etag,
      lastModified: new Date(),
      contentLength: bodyBuffer.length,
      contentType: options.contentType,
      contentEncoding: options.contentEncoding,
      contentLanguage: options.contentLanguage,
      contentDisposition: options.contentDisposition,
      cacheControl: options.cacheControl,
      expires: options.expires,
      metadata: options.metadata,
      storageClass: options.storageClass || 'STANDARD',
      serverSideEncryption: options.serverSideEncryption,
      sseKmsKeyId: options.sseKmsKeyId,
      bucketKeyEnabled: options.bucketKeyEnabled,
      versionId: options.versionId,
      acl: options.acl,
      checksumAlgorithm: options.checksumAlgorithm,
      checksumCRC32: options.checksumCRC32,
      checksumCRC32C: options.checksumCRC32C,
      checksumSHA1: options.checksumSHA1,
      checksumSHA256: options.checksumSHA256,
      tagging: options.tagging,
    };

    bucketObjects.set(key, object);
    return object;
  }

  /**
   * Get an object from a bucket
   */
  getObject(bucket: string, key: string): StoredObject | undefined {
    const bucketObjects = this.objects.get(bucket);
    if (!bucketObjects) {
      return undefined;
    }
    return bucketObjects.get(key);
  }

  /**
   * Check if an object exists
   */
  objectExists(bucket: string, key: string): boolean {
    const bucketObjects = this.objects.get(bucket);
    if (!bucketObjects) {
      return false;
    }
    return bucketObjects.has(key);
  }

  /**
   * Delete an object from a bucket
   */
  deleteObject(bucket: string, key: string): boolean {
    const bucketObjects = this.objects.get(bucket);
    if (!bucketObjects) {
      return false;
    }
    return bucketObjects.delete(key);
  }

  /**
   * List objects in a bucket with optional prefix filtering
   */
  listObjects(
    bucket: string,
    options: {
      prefix?: string;
      delimiter?: string;
      startAfter?: string;
      maxKeys?: number;
      continuationToken?: string;
    } = {}
  ): {
    objects: StoredObject[];
    commonPrefixes: string[];
    isTruncated: boolean;
    nextContinuationToken?: string;
  } {
    const bucketObjects = this.objects.get(bucket);
    if (!bucketObjects) {
      throw new Error(`Bucket does not exist: ${bucket}`);
    }

    const { prefix = '', delimiter, startAfter, maxKeys = 1000, continuationToken } = options;

    // Get all matching objects
    let allObjects = Array.from(bucketObjects.values())
      .filter((obj) => obj.key.startsWith(prefix))
      .sort((a, b) => a.key.localeCompare(b.key));

    // Apply startAfter or continuationToken
    const startKey = continuationToken || startAfter;
    if (startKey) {
      const startIndex = allObjects.findIndex((obj) => obj.key > startKey);
      if (startIndex === -1) {
        allObjects = [];
      } else {
        allObjects = allObjects.slice(startIndex);
      }
    }

    // Handle delimiter (for directory-like listing)
    const commonPrefixes: Set<string> = new Set();
    let filteredObjects: StoredObject[] = [];

    if (delimiter) {
      for (const obj of allObjects) {
        const keyAfterPrefix = obj.key.slice(prefix.length);
        const delimiterIndex = keyAfterPrefix.indexOf(delimiter);

        if (delimiterIndex !== -1) {
          // This is a "directory" - add to common prefixes
          const commonPrefix = prefix + keyAfterPrefix.slice(0, delimiterIndex + 1);
          commonPrefixes.add(commonPrefix);
        } else {
          // This is a direct object
          filteredObjects.push(obj);
        }
      }
    } else {
      filteredObjects = allObjects;
    }

    // Apply maxKeys limit
    const isTruncated = filteredObjects.length > maxKeys;
    const resultObjects = filteredObjects.slice(0, maxKeys);
    const nextContinuationToken = isTruncated
      ? resultObjects[resultObjects.length - 1]?.key
      : undefined;

    return {
      objects: resultObjects,
      commonPrefixes: Array.from(commonPrefixes).sort(),
      isTruncated,
      nextContinuationToken,
    };
  }

  /**
   * Reset the store to empty state
   */
  reset(): void {
    this.buckets.clear();
    this.objects.clear();
  }

  /**
   * Get a snapshot of all data for debugging
   */
  snapshot(): {
    buckets: StoredBucket[];
    objects: Record<string, StoredObject[]>;
  } {
    const objects: Record<string, StoredObject[]> = {};
    this.objects.forEach((bucketObjects, bucketName) => {
      objects[bucketName] = Array.from(bucketObjects.values());
    });

    return {
      buckets: this.listBuckets(),
      objects,
    };
  }
}

// Default global store instance
let defaultStore: S3MockStore | null = null;

/**
 * Get the default S3 mock store instance
 */
export function getDefaultS3Store(): S3MockStore {
  if (!defaultStore) {
    defaultStore = new S3MockStore();
  }
  return defaultStore;
}

/**
 * Reset the default S3 mock store
 */
export function resetDefaultS3Store(): void {
  if (defaultStore) {
    defaultStore.reset();
  }
}
