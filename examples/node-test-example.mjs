/**
 * Example test file using Node.js built-in test module
 * Run with: node --test examples/node-test-example.mjs
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert';
import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
} from '@aws-sdk/client-s3';
import { S3MockClient, S3MockStore } from '../dist/index.js';

describe('S3MockClient with node:test', () => {
  let s3Client;
  let mockClient;
  let store;

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

      assert.strictEqual(response.$metadata.httpStatusCode, 200);
      assert.ok(response.ETag);
      assert.match(response.ETag, /^"[a-f0-9]{32}"$/);
    });

    it('should store an object with Buffer body', async () => {
      const response = await s3Client.send(
        new PutObjectCommand({
          Bucket: 'test-bucket',
          Key: 'binary.bin',
          Body: Buffer.from([0x00, 0x01, 0x02, 0x03]),
        })
      );

      assert.strictEqual(response.$metadata.httpStatusCode, 200);
      assert.ok(response.ETag);
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

      assert.strictEqual(response.$metadata.httpStatusCode, 200);
      assert.strictEqual(response.ContentType, 'text/plain');
      assert.strictEqual(response.ContentLength, 13);

      const body = await response.Body?.transformToString();
      assert.strictEqual(body, 'Hello, World!');
    });

    it('should throw NoSuchKey for non-existent object', async () => {
      await assert.rejects(
        async () => {
          await s3Client.send(
            new GetObjectCommand({
              Bucket: 'test-bucket',
              Key: 'nonexistent.txt',
            })
          );
        },
        (error) => {
          assert.strictEqual(error.name, 'NoSuchKey');
          assert.strictEqual(error.$metadata?.httpStatusCode, 404);
          return true;
        }
      );
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

      assert.strictEqual(response.$metadata.httpStatusCode, 200);
      assert.strictEqual(response.ContentType, 'text/plain');
      assert.strictEqual(response.ContentLength, 13);
      assert.ok(response.ETag);
      assert.ok(response.LastModified instanceof Date);
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

      assert.strictEqual(response.$metadata.httpStatusCode, 200);

      // Verify object is deleted
      await assert.rejects(
        async () => {
          await s3Client.send(
            new GetObjectCommand({
              Bucket: 'test-bucket',
              Key: 'test.txt',
            })
          );
        },
        (error) => {
          assert.strictEqual(error.name, 'NoSuchKey');
          return true;
        }
      );
    });
  });

  describe('ListObjectsV2', () => {
    beforeEach(async () => {
      const objects = [
        { key: 'folder1/file1.txt', body: 'content1' },
        { key: 'folder1/file2.txt', body: 'content2' },
        { key: 'folder2/file3.txt', body: 'content3' },
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

      assert.strictEqual(response.$metadata.httpStatusCode, 200);
      assert.strictEqual(response.Contents?.length, 4);
      assert.strictEqual(response.KeyCount, 4);
      assert.strictEqual(response.IsTruncated, false);
    });

    it('should filter by prefix', async () => {
      const response = await s3Client.send(
        new ListObjectsV2Command({
          Bucket: 'test-bucket',
          Prefix: 'folder1/',
        })
      );

      assert.strictEqual(response.Contents?.length, 2);
      assert.ok(response.Contents?.every((obj) => obj.Key?.startsWith('folder1/')));
    });
  });
});
