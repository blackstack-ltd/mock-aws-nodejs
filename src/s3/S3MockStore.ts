import type {
  ChecksumAlgorithm,
  ObjectCannedACL,
  StorageClass,
  ServerSideEncryption,
  BucketCannedACL,
  BucketVersioningStatus,
  Tag,
  CORSRule,
  LifecycleRule,
  NotificationConfiguration,
  ReplicationConfiguration,
  AccelerateConfiguration,
  BucketLoggingStatus,
  ObjectLockConfiguration,
  PublicAccessBlockConfiguration,
  AnalyticsConfiguration,
  InventoryConfiguration,
  MetricsConfiguration,
  RequestPaymentConfiguration,
} from '@aws-sdk/client-s3';
import { createHash, randomUUID } from 'crypto';
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
  tags?: Tag[];
  /** Object Lock Mode */
  objectLockMode?: string;
  /** Object Lock Retain Until Date */
  objectLockRetainUntilDate?: Date;
  /** Object Lock Legal Hold Status */
  objectLockLegalHoldStatus?: string;
  /** Restore status */
  restoreStatus?: {
    isRestoreInProgress?: boolean;
    restoreExpiryDate?: Date;
  };
}

/**
 * Represents bucket configuration
 */
export interface BucketConfiguration {
  /** Versioning configuration */
  versioning?: {
    Status?: BucketVersioningStatus;
    MFADelete?: 'Enabled' | 'Disabled';
  };
  /** CORS configuration */
  cors?: CORSRule[];
  /** Bucket policy */
  policy?: string;
  /** Bucket ACL */
  acl?: {
    Owner?: { DisplayName?: string; ID?: string };
    Grants?: Array<{
      Grantee?: {
        Type: string;
        DisplayName?: string;
        ID?: string;
        URI?: string;
        EmailAddress?: string;
      };
      Permission?: string;
    }>;
  };
  /** Server-side encryption configuration */
  encryption?: {
    Rules: Array<{
      ApplyServerSideEncryptionByDefault?: {
        SSEAlgorithm: ServerSideEncryption;
        KMSMasterKeyID?: string;
      };
      BucketKeyEnabled?: boolean;
    }>;
  };
  /** Lifecycle configuration */
  lifecycle?: LifecycleRule[];
  /** Website configuration */
  website?: {
    IndexDocument?: { Suffix: string };
    ErrorDocument?: { Key: string };
    RedirectAllRequestsTo?: { HostName: string; Protocol?: string };
    RoutingRules?: Array<{
      Condition?: { HttpErrorCodeReturnedEquals?: string; KeyPrefixEquals?: string };
      Redirect: {
        HostName?: string;
        HttpRedirectCode?: string;
        Protocol?: string;
        ReplaceKeyPrefixWith?: string;
        ReplaceKeyWith?: string;
      };
    }>;
  };
  /** Bucket tags */
  tags?: Tag[];
  /** Notification configuration */
  notification?: NotificationConfiguration;
  /** Replication configuration */
  replication?: ReplicationConfiguration;
  /** Accelerate configuration */
  accelerate?: AccelerateConfiguration;
  /** Logging configuration */
  logging?: BucketLoggingStatus;
  /** Object Lock configuration */
  objectLock?: ObjectLockConfiguration;
  /** Public Access Block configuration */
  publicAccessBlock?: PublicAccessBlockConfiguration;
  /** Analytics configurations */
  analytics?: Map<string, AnalyticsConfiguration>;
  /** Inventory configurations */
  inventory?: Map<string, InventoryConfiguration>;
  /** Metrics configurations */
  metrics?: Map<string, MetricsConfiguration>;
  /** Request payment configuration */
  requestPayment?: RequestPaymentConfiguration;
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
  /** Bucket configuration */
  configuration: BucketConfiguration;
}

/**
 * Represents a multipart upload
 */
export interface MultipartUpload {
  /** Upload ID */
  uploadId: string;
  /** Bucket name */
  bucket: string;
  /** Object key */
  key: string;
  /** Initiated timestamp */
  initiated: Date;
  /** Storage class */
  storageClass?: StorageClass;
  /** Uploaded parts */
  parts: Map<number, UploadedPart>;
  /** Metadata */
  metadata?: Record<string, string>;
  /** Content-Type */
  contentType?: string;
  /** Server-side encryption */
  serverSideEncryption?: string;
  /** SSE-KMS key ID */
  sseKmsKeyId?: string;
  /** ACL */
  acl?: ObjectCannedACL;
  /** Tags */
  tags?: Tag[];
}

/**
 * Represents an uploaded part in a multipart upload
 */
export interface UploadedPart {
  /** Part number */
  partNumber: number;
  /** ETag */
  etag: string;
  /** Part size */
  size: number;
  /** Part data */
  data: Buffer;
  /** Last modified */
  lastModified: Date;
}

/**
 * In-memory store for S3 mock data.
 * Manages buckets and objects for the mock S3 service.
 */
export class S3MockStore implements MockStore {
  private buckets: Map<string, StoredBucket> = new Map();
  private objects: Map<string, Map<string, StoredObject>> = new Map();
  private multipartUploads: Map<string, MultipartUpload> = new Map();

  /**
   * Create a bucket
   */
  createBucket(name: string, region?: string, acl?: BucketCannedACL): StoredBucket {
    if (this.buckets.has(name)) {
      throw new Error(`Bucket already exists: ${name}`);
    }

    const bucket: StoredBucket = {
      name,
      creationDate: new Date(),
      region,
      configuration: {
        analytics: new Map(),
        inventory: new Map(),
        metrics: new Map(),
      },
    };

    // Set default ACL if provided
    if (acl) {
      bucket.configuration.acl = this.getDefaultBucketAcl(acl);
    }

    this.buckets.set(name, bucket);
    this.objects.set(name, new Map());
    return bucket;
  }

  /**
   * Get default bucket ACL based on canned ACL
   */
  private getDefaultBucketAcl(cannedAcl: BucketCannedACL) {
    const owner = { ID: 'default-owner-id', DisplayName: 'default-owner' };

    switch (cannedAcl) {
      case 'private':
        return {
          Owner: owner,
          Grants: [
            {
              Grantee: { Type: 'CanonicalUser', ID: owner.ID },
              Permission: 'FULL_CONTROL',
            },
          ],
        };
      case 'public-read':
        return {
          Owner: owner,
          Grants: [
            {
              Grantee: { Type: 'CanonicalUser', ID: owner.ID },
              Permission: 'FULL_CONTROL',
            },
            {
              Grantee: { Type: 'Group', URI: 'http://acs.amazonaws.com/groups/global/AllUsers' },
              Permission: 'READ',
            },
          ],
        };
      case 'public-read-write':
        return {
          Owner: owner,
          Grants: [
            {
              Grantee: { Type: 'CanonicalUser', ID: owner.ID },
              Permission: 'FULL_CONTROL',
            },
            {
              Grantee: { Type: 'Group', URI: 'http://acs.amazonaws.com/groups/global/AllUsers' },
              Permission: 'READ',
            },
            {
              Grantee: { Type: 'Group', URI: 'http://acs.amazonaws.com/groups/global/AllUsers' },
              Permission: 'WRITE',
            },
          ],
        };
      case 'authenticated-read':
        return {
          Owner: owner,
          Grants: [
            {
              Grantee: { Type: 'CanonicalUser', ID: owner.ID },
              Permission: 'FULL_CONTROL',
            },
            {
              Grantee: { Type: 'Group', URI: 'http://acs.amazonaws.com/groups/global/AuthenticatedUsers' },
              Permission: 'READ',
            },
          ],
        };
      default:
        return {
          Owner: owner,
          Grants: [
            {
              Grantee: { Type: 'CanonicalUser', ID: owner.ID },
              Permission: 'FULL_CONTROL',
            },
          ],
        };
    }
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
      tags: options.tags,
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
   * Copy an object from one location to another
   */
  copyObject(
    sourceBucket: string,
    sourceKey: string,
    destBucket: string,
    destKey: string,
    options: Partial<Omit<StoredObject, 'key' | 'body' | 'etag' | 'lastModified' | 'contentLength'>> = {}
  ): StoredObject {
    const sourceObject = this.getObject(sourceBucket, sourceKey);
    if (!sourceObject) {
      throw new Error(`Source object does not exist: ${sourceBucket}/${sourceKey}`);
    }

    // Copy the object with new metadata
    return this.putObject(destBucket, destKey, sourceObject.body, {
      ...sourceObject,
      ...options,
    });
  }

  // ==================== Multipart Upload Methods ====================

  /**
   * Create a multipart upload
   */
  createMultipartUpload(
    bucket: string,
    key: string,
    options: {
      metadata?: Record<string, string>;
      contentType?: string;
      storageClass?: StorageClass;
      serverSideEncryption?: string;
      sseKmsKeyId?: string;
      acl?: ObjectCannedACL;
      tags?: Tag[];
    } = {}
  ): MultipartUpload {
    this.ensureBucket(bucket);

    const uploadId = randomUUID();
    const upload: MultipartUpload = {
      uploadId,
      bucket,
      key,
      initiated: new Date(),
      parts: new Map(),
      ...options,
    };

    this.multipartUploads.set(uploadId, upload);
    return upload;
  }

  /**
   * Upload a part for a multipart upload
   */
  uploadPart(
    uploadId: string,
    partNumber: number,
    data: Buffer
  ): UploadedPart {
    const upload = this.multipartUploads.get(uploadId);
    if (!upload) {
      throw new Error(`Multipart upload not found: ${uploadId}`);
    }

    const etag = `"${createHash('md5').update(data).digest('hex')}"`;
    const part: UploadedPart = {
      partNumber,
      etag,
      size: data.length,
      data,
      lastModified: new Date(),
    };

    upload.parts.set(partNumber, part);
    return part;
  }

  /**
   * Complete a multipart upload
   */
  completeMultipartUpload(
    uploadId: string,
    parts: Array<{ PartNumber: number; ETag: string }>
  ): StoredObject {
    const upload = this.multipartUploads.get(uploadId);
    if (!upload) {
      throw new Error(`Multipart upload not found: ${uploadId}`);
    }

    // Verify all parts exist and match ETags
    const orderedParts: UploadedPart[] = [];
    for (const partInfo of parts) {
      const part = upload.parts.get(partInfo.PartNumber);
      if (!part) {
        throw new Error(`Part ${partInfo.PartNumber} not found`);
      }
      if (part.etag !== partInfo.ETag) {
        throw new Error(`ETag mismatch for part ${partInfo.PartNumber}`);
      }
      orderedParts.push(part);
    }

    // Concatenate all parts
    const body = Buffer.concat(orderedParts.map((p) => p.data));

    // Create the final object
    const object = this.putObject(upload.bucket, upload.key, body, {
      metadata: upload.metadata,
      contentType: upload.contentType,
      storageClass: upload.storageClass,
      serverSideEncryption: upload.serverSideEncryption,
      sseKmsKeyId: upload.sseKmsKeyId,
      acl: upload.acl,
      tags: upload.tags,
    });

    // Clean up the multipart upload
    this.multipartUploads.delete(uploadId);

    return object;
  }

  /**
   * Abort a multipart upload
   */
  abortMultipartUpload(uploadId: string): boolean {
    return this.multipartUploads.delete(uploadId);
  }

  /**
   * List multipart uploads for a bucket
   */
  listMultipartUploads(bucket: string, options: { prefix?: string; maxUploads?: number } = {}): MultipartUpload[] {
    const uploads = Array.from(this.multipartUploads.values()).filter(
      (upload) =>
        upload.bucket === bucket && (!options.prefix || upload.key.startsWith(options.prefix))
    );

    return uploads.slice(0, options.maxUploads || 1000);
  }

  /**
   * Get multipart upload
   */
  getMultipartUpload(uploadId: string): MultipartUpload | undefined {
    return this.multipartUploads.get(uploadId);
  }

  /**
   * List parts for a multipart upload
   */
  listParts(uploadId: string, options: { maxParts?: number; partNumberMarker?: number } = {}): UploadedPart[] {
    const upload = this.multipartUploads.get(uploadId);
    if (!upload) {
      throw new Error(`Multipart upload not found: ${uploadId}`);
    }

    let parts = Array.from(upload.parts.values()).sort((a, b) => a.partNumber - b.partNumber);

    if (options.partNumberMarker) {
      parts = parts.filter((p) => p.partNumber > options.partNumberMarker!);
    }

    return parts.slice(0, options.maxParts || 1000);
  }

  // ==================== Bucket Configuration Methods ====================

  /**
   * Get bucket configuration
   */
  getBucketConfiguration(bucket: string): BucketConfiguration {
    const storedBucket = this.buckets.get(bucket);
    if (!storedBucket) {
      throw new Error(`Bucket does not exist: ${bucket}`);
    }
    return storedBucket.configuration;
  }

  /**
   * Update bucket configuration
   */
  updateBucketConfiguration(bucket: string, config: Partial<BucketConfiguration>): void {
    const storedBucket = this.buckets.get(bucket);
    if (!storedBucket) {
      throw new Error(`Bucket does not exist: ${bucket}`);
    }
    storedBucket.configuration = { ...storedBucket.configuration, ...config };
  }

  // ==================== Object Tagging Methods ====================

  /**
   * Get object tags
   */
  getObjectTags(bucket: string, key: string): Tag[] | undefined {
    const object = this.getObject(bucket, key);
    return object?.tags;
  }

  /**
   * Set object tags
   */
  setObjectTags(bucket: string, key: string, tags: Tag[]): void {
    const bucketObjects = this.objects.get(bucket);
    if (!bucketObjects) {
      throw new Error(`Bucket does not exist: ${bucket}`);
    }

    const object = bucketObjects.get(key);
    if (!object) {
      throw new Error(`Object does not exist: ${key}`);
    }

    object.tags = tags;
  }

  /**
   * Delete object tags
   */
  deleteObjectTags(bucket: string, key: string): void {
    const bucketObjects = this.objects.get(bucket);
    if (!bucketObjects) {
      throw new Error(`Bucket does not exist: ${bucket}`);
    }

    const object = bucketObjects.get(key);
    if (object) {
      object.tags = undefined;
    }
  }

  /**
   * Reset the store to empty state
   */
  reset(): void {
    this.buckets.clear();
    this.objects.clear();
    this.multipartUploads.clear();
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
