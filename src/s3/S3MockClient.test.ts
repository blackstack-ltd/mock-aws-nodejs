import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
  type PutObjectCommandOutput,
  type GetObjectCommandOutput,
} from '@aws-sdk/client-s3';
import { S3MockClient, S3MockStore } from './index';

describe('S3MockClient', () => {
  let s3Client: S3Client;
  let mockClient: S3MockClient;
  let store: S3MockStore;

  beforeEach(() => {
    store = new S3MockStore();
    s3Client = new S3Client({ region: 'us-east-1' });
    mockClient = new S3MockClient(store);
    mockClient.install(s3Client);
  });

  afterEach(() => {
    mockClient.restore();
    mockClient.reset();
  });

  describe('PutObject', () => {
    it('should store an object with string body', async () => {
      const response = await s3Client.send(
        new PutObjectCommand({
          Bucket: 'test-bucket',
          Key: 'test.txt',
          Body: 'Hello, World!',
        })
      );

      expect(response.$metadata.httpStatusCode).toBe(200);
      expect(response.ETag).toBeDefined();
      expect(response.ETag).toMatch(/^"[a-f0-9]{32}"$/);
    });

    it('should store an object with Buffer body', async () => {
      const response = await s3Client.send(
        new PutObjectCommand({
          Bucket: 'test-bucket',
          Key: 'binary.bin',
          Body: Buffer.from([0x00, 0x01, 0x02, 0x03]),
        })
      );

      expect(response.$metadata.httpStatusCode).toBe(200);
      expect(response.ETag).toBeDefined();
    });

    it('should store an object with metadata', async () => {
      await s3Client.send(
        new PutObjectCommand({
          Bucket: 'test-bucket',
          Key: 'with-meta.txt',
          Body: 'content',
          ContentType: 'text/plain',
          Metadata: {
            'custom-header': 'custom-value',
          },
        })
      );

      const storedObject = store.getObject('test-bucket', 'with-meta.txt');
      expect(storedObject).toBeDefined();
      expect(storedObject?.contentType).toBe('text/plain');
      expect(storedObject?.metadata).toEqual({ 'custom-header': 'custom-value' });
    });

    it('should auto-create bucket if it does not exist', async () => {
      await s3Client.send(
        new PutObjectCommand({
          Bucket: 'auto-created-bucket',
          Key: 'test.txt',
          Body: 'content',
        })
      );

      expect(store.bucketExists('auto-created-bucket')).toBe(true);
    });

    it('should throw error when Bucket is missing', async () => {
      await expect(
        s3Client.send(
          new PutObjectCommand({
            Bucket: '',
            Key: 'test.txt',
            Body: 'content',
          })
        )
      ).rejects.toThrow('Bucket');
    });

    it('should throw error when Key is missing', async () => {
      await expect(
        s3Client.send(
          new PutObjectCommand({
            Bucket: 'test-bucket',
            Key: '',
            Body: 'content',
          })
        )
      ).rejects.toThrow('Key');
    });
  });

  describe('GetObject', () => {
    beforeEach(async () => {
      await s3Client.send(
        new PutObjectCommand({
          Bucket: 'test-bucket',
          Key: 'test.txt',
          Body: 'Hello, World!',
          ContentType: 'text/plain',
        })
      );
    });

    it('should retrieve an object', async () => {
      const response = await s3Client.send(
        new GetObjectCommand({
          Bucket: 'test-bucket',
          Key: 'test.txt',
        })
      );

      expect(response.$metadata.httpStatusCode).toBe(200);
      expect(response.ContentType).toBe('text/plain');
      expect(response.ContentLength).toBe(13);

      const body = await response.Body?.transformToString();
      expect(body).toBe('Hello, World!');
    });

    it('should support transformToByteArray', async () => {
      const response = await s3Client.send(
        new GetObjectCommand({
          Bucket: 'test-bucket',
          Key: 'test.txt',
        })
      );

      const bytes = await response.Body?.transformToByteArray();
      expect(bytes).toBeInstanceOf(Uint8Array);
      expect(Buffer.from(bytes!).toString()).toBe('Hello, World!');
    });

    it('should throw NoSuchKey for non-existent object', async () => {
      await expect(
        s3Client.send(
          new GetObjectCommand({
            Bucket: 'test-bucket',
            Key: 'nonexistent.txt',
          })
        )
      ).rejects.toMatchObject({
        name: 'NoSuchKey',
        $metadata: { httpStatusCode: 404 },
      });
    });

    it('should throw NoSuchBucket for non-existent bucket', async () => {
      await expect(
        s3Client.send(
          new GetObjectCommand({
            Bucket: 'nonexistent-bucket',
            Key: 'test.txt',
          })
        )
      ).rejects.toMatchObject({
        name: 'NoSuchBucket',
        $metadata: { httpStatusCode: 404 },
      });
    });

    it('should support range requests', async () => {
      const response = await s3Client.send(
        new GetObjectCommand({
          Bucket: 'test-bucket',
          Key: 'test.txt',
          Range: 'bytes=0-4',
        })
      );

      const body = await response.Body?.transformToString();
      expect(body).toBe('Hello');
      expect(response.ContentRange).toBe('bytes 0-4/13');
    });

    it('should handle IfMatch condition (success)', async () => {
      const headResponse = await s3Client.send(
        new HeadObjectCommand({
          Bucket: 'test-bucket',
          Key: 'test.txt',
        })
      );

      const response = await s3Client.send(
        new GetObjectCommand({
          Bucket: 'test-bucket',
          Key: 'test.txt',
          IfMatch: headResponse.ETag,
        })
      );

      const body = await response.Body?.transformToString();
      expect(body).toBe('Hello, World!');
    });

    it('should handle IfMatch condition (failure)', async () => {
      await expect(
        s3Client.send(
          new GetObjectCommand({
            Bucket: 'test-bucket',
            Key: 'test.txt',
            IfMatch: '"wrong-etag"',
          })
        )
      ).rejects.toMatchObject({
        name: 'PreconditionFailed',
      });
    });

    it('should handle IfNoneMatch condition', async () => {
      const headResponse = await s3Client.send(
        new HeadObjectCommand({
          Bucket: 'test-bucket',
          Key: 'test.txt',
        })
      );

      const response = await s3Client.send(
        new GetObjectCommand({
          Bucket: 'test-bucket',
          Key: 'test.txt',
          IfNoneMatch: headResponse.ETag,
        })
      );

      expect(response.$metadata.httpStatusCode).toBe(304);
    });
  });

  describe('HeadObject', () => {
    beforeEach(async () => {
      await s3Client.send(
        new PutObjectCommand({
          Bucket: 'test-bucket',
          Key: 'test.txt',
          Body: 'Hello, World!',
          ContentType: 'text/plain',
          Metadata: { 'x-custom': 'value' },
        })
      );
    });

    it('should return object metadata without body', async () => {
      const response = await s3Client.send(
        new HeadObjectCommand({
          Bucket: 'test-bucket',
          Key: 'test.txt',
        })
      );

      expect(response.$metadata.httpStatusCode).toBe(200);
      expect(response.ContentType).toBe('text/plain');
      expect(response.ContentLength).toBe(13);
      expect(response.ETag).toBeDefined();
      expect(response.LastModified).toBeInstanceOf(Date);
      expect(response.Metadata).toEqual({ 'x-custom': 'value' });
    });

    it('should throw NotFound for non-existent object', async () => {
      await expect(
        s3Client.send(
          new HeadObjectCommand({
            Bucket: 'test-bucket',
            Key: 'nonexistent.txt',
          })
        )
      ).rejects.toMatchObject({
        name: 'NotFound',
        $metadata: { httpStatusCode: 404 },
      });
    });

    it('should throw NoSuchBucket for non-existent bucket', async () => {
      await expect(
        s3Client.send(
          new HeadObjectCommand({
            Bucket: 'nonexistent-bucket',
            Key: 'test.txt',
          })
        )
      ).rejects.toMatchObject({
        name: 'NoSuchBucket',
        $metadata: { httpStatusCode: 404 },
      });
    });
  });

  describe('DeleteObject', () => {
    beforeEach(async () => {
      await s3Client.send(
        new PutObjectCommand({
          Bucket: 'test-bucket',
          Key: 'test.txt',
          Body: 'content',
        })
      );
    });

    it('should delete an existing object', async () => {
      const response = await s3Client.send(
        new DeleteObjectCommand({
          Bucket: 'test-bucket',
          Key: 'test.txt',
        })
      );

      expect(response.$metadata.httpStatusCode).toBe(200);

      // Verify object is deleted
      await expect(
        s3Client.send(
          new GetObjectCommand({
            Bucket: 'test-bucket',
            Key: 'test.txt',
          })
        )
      ).rejects.toMatchObject({ name: 'NoSuchKey' });
    });

    it('should succeed even if object does not exist', async () => {
      const response = await s3Client.send(
        new DeleteObjectCommand({
          Bucket: 'test-bucket',
          Key: 'nonexistent.txt',
        })
      );

      expect(response.$metadata.httpStatusCode).toBe(200);
    });

    it('should throw NoSuchBucket for non-existent bucket', async () => {
      await expect(
        s3Client.send(
          new DeleteObjectCommand({
            Bucket: 'nonexistent-bucket',
            Key: 'test.txt',
          })
        )
      ).rejects.toMatchObject({
        name: 'NoSuchBucket',
        $metadata: { httpStatusCode: 404 },
      });
    });
  });

  describe('ListObjectsV2', () => {
    beforeEach(async () => {
      // Create some test objects
      const objects = [
        { key: 'folder1/file1.txt', body: 'content1' },
        { key: 'folder1/file2.txt', body: 'content2' },
        { key: 'folder1/subfolder/file3.txt', body: 'content3' },
        { key: 'folder2/file4.txt', body: 'content4' },
        { key: 'root.txt', body: 'root content' },
      ];

      for (const obj of objects) {
        await s3Client.send(
          new PutObjectCommand({
            Bucket: 'test-bucket',
            Key: obj.key,
            Body: obj.body,
          })
        );
      }
    });

    it('should list all objects in bucket', async () => {
      const response = await s3Client.send(
        new ListObjectsV2Command({
          Bucket: 'test-bucket',
        })
      );

      expect(response.$metadata.httpStatusCode).toBe(200);
      expect(response.Contents).toHaveLength(5);
      expect(response.KeyCount).toBe(5);
      expect(response.IsTruncated).toBe(false);
    });

    it('should filter by prefix', async () => {
      const response = await s3Client.send(
        new ListObjectsV2Command({
          Bucket: 'test-bucket',
          Prefix: 'folder1/',
        })
      );

      expect(response.Contents).toHaveLength(3);
      expect(response.Contents?.every((obj) => obj.Key?.startsWith('folder1/'))).toBe(true);
    });

    it('should support delimiter for directory-like listing', async () => {
      const response = await s3Client.send(
        new ListObjectsV2Command({
          Bucket: 'test-bucket',
          Delimiter: '/',
        })
      );

      expect(response.Contents).toHaveLength(1); // Only root.txt
      expect(response.Contents?.[0].Key).toBe('root.txt');
      expect(response.CommonPrefixes).toHaveLength(2); // folder1/ and folder2/
      expect(response.CommonPrefixes?.map((p) => p.Prefix)).toEqual(['folder1/', 'folder2/']);
    });

    it('should support prefix with delimiter', async () => {
      const response = await s3Client.send(
        new ListObjectsV2Command({
          Bucket: 'test-bucket',
          Prefix: 'folder1/',
          Delimiter: '/',
        })
      );

      expect(response.Contents).toHaveLength(2); // file1.txt and file2.txt
      expect(response.CommonPrefixes).toHaveLength(1); // subfolder/
      expect(response.CommonPrefixes?.[0].Prefix).toBe('folder1/subfolder/');
    });

    it('should support MaxKeys', async () => {
      const response = await s3Client.send(
        new ListObjectsV2Command({
          Bucket: 'test-bucket',
          MaxKeys: 2,
        })
      );

      expect(response.Contents).toHaveLength(2);
      expect(response.IsTruncated).toBe(true);
      expect(response.NextContinuationToken).toBeDefined();
    });

    it('should support pagination with ContinuationToken', async () => {
      const firstPage = await s3Client.send(
        new ListObjectsV2Command({
          Bucket: 'test-bucket',
          MaxKeys: 2,
        })
      );

      const secondPage = await s3Client.send(
        new ListObjectsV2Command({
          Bucket: 'test-bucket',
          MaxKeys: 2,
          ContinuationToken: firstPage.NextContinuationToken,
        })
      );

      expect(secondPage.Contents).toBeDefined();
      expect(secondPage.Contents!.length).toBeGreaterThan(0);

      // Ensure no duplicate keys between pages
      const firstPageKeys = new Set(firstPage.Contents?.map((o) => o.Key));
      const secondPageKeys = secondPage.Contents?.map((o) => o.Key);
      expect(secondPageKeys?.every((key) => !firstPageKeys.has(key))).toBe(true);
    });

    it('should throw NoSuchBucket for non-existent bucket', async () => {
      await expect(
        s3Client.send(
          new ListObjectsV2Command({
            Bucket: 'nonexistent-bucket',
          })
        )
      ).rejects.toMatchObject({
        name: 'NoSuchBucket',
        $metadata: { httpStatusCode: 404 },
      });
    });

    it('should return object metadata in contents', async () => {
      const response = await s3Client.send(
        new ListObjectsV2Command({
          Bucket: 'test-bucket',
          Prefix: 'root.txt',
        })
      );

      expect(response.Contents).toHaveLength(1);
      const object = response.Contents?.[0];
      expect(object?.Key).toBe('root.txt');
      expect(object?.Size).toBe(12); // 'root content'
      expect(object?.ETag).toBeDefined();
      expect(object?.LastModified).toBeInstanceOf(Date);
    });
  });

  describe('Custom mock behaviors', () => {
    it('should allow overriding default handlers with custom response', async () => {
      const customETag = '"custom-etag"';

      mockClient.on(PutObjectCommand).resolves({
        $metadata: { httpStatusCode: 200 },
        ETag: customETag,
      } as PutObjectCommandOutput);

      const response = await s3Client.send(
        new PutObjectCommand({
          Bucket: 'any-bucket',
          Key: 'any-key',
          Body: 'any-content',
        })
      );

      expect(response.ETag).toBe(customETag);
    });

    it('should allow overriding with custom error', async () => {
      const customError = new Error('Custom error');
      mockClient.on(GetObjectCommand).rejects(customError);

      await expect(
        s3Client.send(
          new GetObjectCommand({
            Bucket: 'test-bucket',
            Key: 'test.txt',
          })
        )
      ).rejects.toThrow('Custom error');
    });

    it('should allow overriding with custom handler', async () => {
      mockClient.on(GetObjectCommand).callsFake((input) => {
        return {
          $metadata: { httpStatusCode: 200 },
          Body: undefined,
          ETag: `"etag-for-${input.Key}"`,
        } as GetObjectCommandOutput;
      });

      const response = await s3Client.send(
        new GetObjectCommand({
          Bucket: 'test-bucket',
          Key: 'my-file.txt',
        })
      );

      expect(response.ETag).toBe('"etag-for-my-file.txt"');
    });

    it('should track command calls', async () => {
      await s3Client.send(
        new PutObjectCommand({
          Bucket: 'bucket1',
          Key: 'key1',
          Body: 'content1',
        })
      );

      await s3Client.send(
        new PutObjectCommand({
          Bucket: 'bucket2',
          Key: 'key2',
          Body: 'content2',
        })
      );

      const putCalls = mockClient.commandCalls(PutObjectCommand);
      expect(putCalls).toHaveLength(2);
      expect(putCalls[0].input.Bucket).toBe('bucket1');
      expect(putCalls[1].input.Bucket).toBe('bucket2');
    });
  });

  describe('seed()', () => {
    it('should seed the store with test data', async () => {
      mockClient.seed({
        'seeded-bucket': {
          'file1.txt': 'content1',
          'file2.txt': Buffer.from('content2'),
          'file3.txt': { body: 'content3', contentType: 'text/plain' },
        },
      });

      // Verify seeded data can be retrieved
      const response1 = await s3Client.send(
        new GetObjectCommand({
          Bucket: 'seeded-bucket',
          Key: 'file1.txt',
        })
      );
      expect(await response1.Body?.transformToString()).toBe('content1');

      const response2 = await s3Client.send(
        new GetObjectCommand({
          Bucket: 'seeded-bucket',
          Key: 'file3.txt',
        })
      );
      expect(response2.ContentType).toBe('text/plain');
    });
  });

  describe('reset()', () => {
    it('should clear all stored objects', async () => {
      await s3Client.send(
        new PutObjectCommand({
          Bucket: 'test-bucket',
          Key: 'test.txt',
          Body: 'content',
        })
      );

      mockClient.reset();

      await expect(
        s3Client.send(
          new GetObjectCommand({
            Bucket: 'test-bucket',
            Key: 'test.txt',
          })
        )
      ).rejects.toMatchObject({ name: 'NoSuchBucket' });
    });
  });
});
