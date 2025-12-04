import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
  CreateBucketCommand,
  DeleteBucketCommand,
  ListBucketsCommand,
  HeadBucketCommand,
  GetBucketLocationCommand,
  CopyObjectCommand,
  DeleteObjectsCommand,
  ListObjectsCommand,
  GetObjectAttributesCommand,
  GetObjectTaggingCommand,
  PutObjectTaggingCommand,
  DeleteObjectTaggingCommand,
  GetObjectAclCommand,
  PutObjectAclCommand,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  UploadPartCopyCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
  ListMultipartUploadsCommand,
  ListPartsCommand,
  GetBucketVersioningCommand,
  PutBucketVersioningCommand,
  GetBucketCorsCommand,
  PutBucketCorsCommand,
  DeleteBucketCorsCommand,
  GetBucketPolicyCommand,
  PutBucketPolicyCommand,
  DeleteBucketPolicyCommand,
  GetBucketAclCommand,
  PutBucketAclCommand,
  GetBucketEncryptionCommand,
  PutBucketEncryptionCommand,
  DeleteBucketEncryptionCommand,
  GetBucketLifecycleConfigurationCommand,
  PutBucketLifecycleConfigurationCommand,
  DeleteBucketLifecycleCommand,
  GetBucketWebsiteCommand,
  PutBucketWebsiteCommand,
  DeleteBucketWebsiteCommand,
  GetBucketTaggingCommand,
  PutBucketTaggingCommand,
  DeleteBucketTaggingCommand,
  GetBucketLoggingCommand,
  PutBucketLoggingCommand,
} from '@aws-sdk/client-s3';
import { MockClient } from '../core/MockClient';
import { S3MockStore, getDefaultS3Store } from './S3MockStore';
import {
  createPutObjectHandler,
  createGetObjectHandler,
  createHeadObjectHandler,
  createDeleteObjectHandler,
  createListObjectsV2Handler,
  createCreateBucketHandler,
  createDeleteBucketHandler,
  createListBucketsHandler,
  createHeadBucketHandler,
  createGetBucketLocationHandler,
  createCopyObjectHandler,
  createDeleteObjectsHandler,
  createListObjectsHandler,
  createGetObjectAttributesHandler,
  createGetObjectTaggingHandler,
  createPutObjectTaggingHandler,
  createDeleteObjectTaggingHandler,
  createGetObjectAclHandler,
  createPutObjectAclHandler,
  createCreateMultipartUploadHandler,
  createUploadPartHandler,
  createUploadPartCopyHandler,
  createCompleteMultipartUploadHandler,
  createAbortMultipartUploadHandler,
  createListMultipartUploadsHandler,
  createListPartsHandler,
  createGetBucketVersioningHandler,
  createPutBucketVersioningHandler,
  createGetBucketCorsHandler,
  createPutBucketCorsHandler,
  createDeleteBucketCorsHandler,
  createGetBucketPolicyHandler,
  createPutBucketPolicyHandler,
  createDeleteBucketPolicyHandler,
  createGetBucketAclHandler,
  createPutBucketAclHandler,
  createGetBucketEncryptionHandler,
  createPutBucketEncryptionHandler,
  createDeleteBucketEncryptionHandler,
  createGetBucketLifecycleConfigurationHandler,
  createPutBucketLifecycleConfigurationHandler,
  createDeleteBucketLifecycleHandler,
  createGetBucketWebsiteHandler,
  createPutBucketWebsiteHandler,
  createDeleteBucketWebsiteHandler,
  createGetBucketTaggingHandler,
  createPutBucketTaggingHandler,
  createDeleteBucketTaggingHandler,
  createGetBucketLoggingHandler,
  createPutBucketLoggingHandler,
} from './handlers';

/**
 * Mock client for AWS S3 SDK V3.
 *
 * Provides a seamless way to mock S3 operations in tests by intercepting
 * the `send` method of S3Client instances.
 *
 * @example
 * ```typescript
 * import { S3Client, PutObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
 * import { S3MockClient } from 'mock-aws-nodejs';
 *
 * describe('S3 operations', () => {
 *   let s3Client: S3Client;
 *   let mockClient: S3MockClient;
 *
 *   beforeEach(() => {
 *     s3Client = new S3Client({ region: 'us-east-1' });
 *     mockClient = new S3MockClient();
 *     mockClient.install(s3Client);
 *   });
 *
 *   afterEach(() => {
 *     mockClient.restore();
 *     mockClient.reset();
 *   });
 *
 *   it('should store and retrieve objects', async () => {
 *     // Put an object
 *     await s3Client.send(new PutObjectCommand({
 *       Bucket: 'my-bucket',
 *       Key: 'test.txt',
 *       Body: 'Hello, World!',
 *     }));
 *
 *     // Get the object
 *     const response = await s3Client.send(new GetObjectCommand({
 *       Bucket: 'my-bucket',
 *       Key: 'test.txt',
 *     }));
 *
 *     const body = await response.Body?.transformToString();
 *     expect(body).toBe('Hello, World!');
 *   });
 * });
 * ```
 */
export class S3MockClient extends MockClient<S3Client> {
  private readonly store: S3MockStore;

  /**
   * Create a new S3MockClient
   *
   * @param store - Optional custom S3MockStore. If not provided, uses the default global store.
   */
  constructor(store?: S3MockStore) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    super(S3Client as any);
    this.store = store || getDefaultS3Store();
    this.registerDefaultHandlers();
  }

  /**
   * Get the underlying mock store for direct manipulation
   */
  getStore(): S3MockStore {
    return this.store;
  }

  /**
   * Reset both the mock client behaviors and the underlying store
   */
  override reset(): this {
    super.reset();
    this.store.reset();
    return this;
  }

  /**
   * Seed the mock store with test data
   *
   * @example
   * ```typescript
   * mockClient.seed({
   *   'my-bucket': {
   *     'file1.txt': 'content1',
   *     'file2.txt': Buffer.from('content2'),
   *     'folder/file3.txt': { body: 'content3', contentType: 'text/plain' },
   *   },
   * });
   * ```
   */
  seed(
    data: Record<
      string,
      Record<string, string | Buffer | { body: string | Buffer; contentType?: string; metadata?: Record<string, string> }>
    >
  ): this {
    for (const [bucket, objects] of Object.entries(data)) {
      this.store.ensureBucket(bucket);
      for (const [key, value] of Object.entries(objects)) {
        if (typeof value === 'string' || Buffer.isBuffer(value)) {
          this.store.putObject(bucket, key, value);
        } else {
          this.store.putObject(bucket, key, value.body, {
            contentType: value.contentType,
            metadata: value.metadata,
          });
        }
      }
    }
    return this;
  }

  /**
   * Register default handlers for all supported S3 commands
   */
  private registerDefaultHandlers(): void {
    // ==================== Bucket Management Commands ====================
    this.registerDefaultHandler(CreateBucketCommand, createCreateBucketHandler(this.store));
    this.registerDefaultHandler(DeleteBucketCommand, createDeleteBucketHandler(this.store));
    this.registerDefaultHandler(ListBucketsCommand, createListBucketsHandler(this.store));
    this.registerDefaultHandler(HeadBucketCommand, createHeadBucketHandler(this.store));
    this.registerDefaultHandler(GetBucketLocationCommand, createGetBucketLocationHandler(this.store));

    // ==================== Object Operations ====================
    this.registerDefaultHandler(PutObjectCommand, createPutObjectHandler(this.store));
    this.registerDefaultHandler(GetObjectCommand, createGetObjectHandler(this.store));
    this.registerDefaultHandler(HeadObjectCommand, createHeadObjectHandler(this.store));
    this.registerDefaultHandler(DeleteObjectCommand, createDeleteObjectHandler(this.store));
    this.registerDefaultHandler(CopyObjectCommand, createCopyObjectHandler(this.store));
    this.registerDefaultHandler(DeleteObjectsCommand, createDeleteObjectsHandler(this.store));
    this.registerDefaultHandler(ListObjectsCommand, createListObjectsHandler(this.store));
    this.registerDefaultHandler(ListObjectsV2Command, createListObjectsV2Handler(this.store));
    this.registerDefaultHandler(GetObjectAttributesCommand, createGetObjectAttributesHandler(this.store));

    // ==================== Object Tagging ====================
    this.registerDefaultHandler(GetObjectTaggingCommand, createGetObjectTaggingHandler(this.store));
    this.registerDefaultHandler(PutObjectTaggingCommand, createPutObjectTaggingHandler(this.store));
    this.registerDefaultHandler(DeleteObjectTaggingCommand, createDeleteObjectTaggingHandler(this.store));

    // ==================== Object ACL ====================
    this.registerDefaultHandler(GetObjectAclCommand, createGetObjectAclHandler(this.store));
    this.registerDefaultHandler(PutObjectAclCommand, createPutObjectAclHandler(this.store));

    // ==================== Multipart Upload ====================
    this.registerDefaultHandler(CreateMultipartUploadCommand, createCreateMultipartUploadHandler(this.store));
    this.registerDefaultHandler(UploadPartCommand, createUploadPartHandler(this.store));
    this.registerDefaultHandler(UploadPartCopyCommand, createUploadPartCopyHandler(this.store));
    this.registerDefaultHandler(CompleteMultipartUploadCommand, createCompleteMultipartUploadHandler(this.store));
    this.registerDefaultHandler(AbortMultipartUploadCommand, createAbortMultipartUploadHandler(this.store));
    this.registerDefaultHandler(ListMultipartUploadsCommand, createListMultipartUploadsHandler(this.store));
    this.registerDefaultHandler(ListPartsCommand, createListPartsHandler(this.store));

    // ==================== Bucket Versioning ====================
    this.registerDefaultHandler(GetBucketVersioningCommand, createGetBucketVersioningHandler(this.store));
    this.registerDefaultHandler(PutBucketVersioningCommand, createPutBucketVersioningHandler(this.store));

    // ==================== Bucket CORS ====================
    this.registerDefaultHandler(GetBucketCorsCommand, createGetBucketCorsHandler(this.store));
    this.registerDefaultHandler(PutBucketCorsCommand, createPutBucketCorsHandler(this.store));
    this.registerDefaultHandler(DeleteBucketCorsCommand, createDeleteBucketCorsHandler(this.store));

    // ==================== Bucket Policy ====================
    this.registerDefaultHandler(GetBucketPolicyCommand, createGetBucketPolicyHandler(this.store));
    this.registerDefaultHandler(PutBucketPolicyCommand, createPutBucketPolicyHandler(this.store));
    this.registerDefaultHandler(DeleteBucketPolicyCommand, createDeleteBucketPolicyHandler(this.store));

    // ==================== Bucket ACL ====================
    this.registerDefaultHandler(GetBucketAclCommand, createGetBucketAclHandler(this.store));
    this.registerDefaultHandler(PutBucketAclCommand, createPutBucketAclHandler(this.store));

    // ==================== Bucket Encryption ====================
    this.registerDefaultHandler(GetBucketEncryptionCommand, createGetBucketEncryptionHandler(this.store));
    this.registerDefaultHandler(PutBucketEncryptionCommand, createPutBucketEncryptionHandler(this.store));
    this.registerDefaultHandler(DeleteBucketEncryptionCommand, createDeleteBucketEncryptionHandler(this.store));

    // ==================== Bucket Lifecycle ====================
    this.registerDefaultHandler(GetBucketLifecycleConfigurationCommand, createGetBucketLifecycleConfigurationHandler(this.store));
    this.registerDefaultHandler(PutBucketLifecycleConfigurationCommand, createPutBucketLifecycleConfigurationHandler(this.store));
    this.registerDefaultHandler(DeleteBucketLifecycleCommand, createDeleteBucketLifecycleHandler(this.store));

    // ==================== Bucket Website ====================
    this.registerDefaultHandler(GetBucketWebsiteCommand, createGetBucketWebsiteHandler(this.store));
    this.registerDefaultHandler(PutBucketWebsiteCommand, createPutBucketWebsiteHandler(this.store));
    this.registerDefaultHandler(DeleteBucketWebsiteCommand, createDeleteBucketWebsiteHandler(this.store));

    // ==================== Bucket Tagging ====================
    this.registerDefaultHandler(GetBucketTaggingCommand, createGetBucketTaggingHandler(this.store));
    this.registerDefaultHandler(PutBucketTaggingCommand, createPutBucketTaggingHandler(this.store));
    this.registerDefaultHandler(DeleteBucketTaggingCommand, createDeleteBucketTaggingHandler(this.store));

    // ==================== Bucket Logging ====================
    this.registerDefaultHandler(GetBucketLoggingCommand, createGetBucketLoggingHandler(this.store));
    this.registerDefaultHandler(PutBucketLoggingCommand, createPutBucketLoggingHandler(this.store));
  }
}

/**
 * Create and install an S3 mock client on the given S3Client instance.
 * This is a convenience function for quick setup.
 *
 * @example
 * ```typescript
 * const s3Client = new S3Client({ region: 'us-east-1' });
 * const mockClient = mockS3Client(s3Client);
 *
 * // Use s3Client as normal - all calls are mocked
 * await s3Client.send(new PutObjectCommand({ ... }));
 *
 * // Clean up
 * mockClient.restore();
 * ```
 */
export function mockS3Client(client: S3Client, store?: S3MockStore): S3MockClient {
  const mockClient = new S3MockClient(store);
  mockClient.install(client);
  return mockClient;
}
