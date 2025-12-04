import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
} from '@aws-sdk/client-s3';
import { MockClient } from '../core/MockClient';
import { S3MockStore, getDefaultS3Store } from './S3MockStore';
import {
  createPutObjectHandler,
  createGetObjectHandler,
  createHeadObjectHandler,
  createDeleteObjectHandler,
  createListObjectsV2Handler,
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
    // Register PutObject handler
    this.registerDefaultHandler(PutObjectCommand, createPutObjectHandler(this.store));

    // Register GetObject handler
    this.registerDefaultHandler(GetObjectCommand, createGetObjectHandler(this.store));

    // Register HeadObject handler
    this.registerDefaultHandler(HeadObjectCommand, createHeadObjectHandler(this.store));

    // Register DeleteObject handler
    this.registerDefaultHandler(DeleteObjectCommand, createDeleteObjectHandler(this.store));

    // Register ListObjectsV2 handler
    this.registerDefaultHandler(ListObjectsV2Command, createListObjectsV2Handler(this.store));
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
