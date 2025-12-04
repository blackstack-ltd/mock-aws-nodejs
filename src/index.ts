/**
 * Mock AWS SDK V3 - A testing library for AWS SDK V3
 *
 * This library provides mock implementations for AWS SDK V3 clients,
 * allowing you to easily test code that depends on AWS services
 * without making actual API calls.
 *
 * @example
 * ```typescript
 * import { S3Client, PutObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
 * import { S3MockClient } from 'mock-aws-nodejs';
 *
 * // Create a real S3 client
 * const s3Client = new S3Client({ region: 'us-east-1' });
 *
 * // Create and install the mock
 * const mockClient = new S3MockClient();
 * mockClient.install(s3Client);
 *
 * // Now all S3 operations are mocked
 * await s3Client.send(new PutObjectCommand({
 *   Bucket: 'test-bucket',
 *   Key: 'test.txt',
 *   Body: 'Hello, World!',
 * }));
 *
 * // Retrieve the object
 * const response = await s3Client.send(new GetObjectCommand({
 *   Bucket: 'test-bucket',
 *   Key: 'test.txt',
 * }));
 *
 * const body = await response.Body?.transformToString();
 * console.log(body); // 'Hello, World!'
 *
 * // Clean up
 * mockClient.restore();
 * mockClient.reset();
 * ```
 *
 * @packageDocumentation
 */

// Core exports
export { MockClient, CommandBehaviorBuilder } from './core';
export type {
  AwsCommand,
  AwsCommandConstructor,
  CommandHandler,
  CommandCall,
  MockedCommandBehavior,
  MockStore,
  AwsError,
} from './core';
export { createAwsError, createResponseMetadata } from './core';

// S3 exports
export {
  S3MockClient,
  mockS3Client,
  S3MockStore,
  getDefaultS3Store,
  resetDefaultS3Store,
  type StoredObject,
  type StoredBucket,
  createPutObjectHandler,
  createGetObjectHandler,
  createHeadObjectHandler,
  createDeleteObjectHandler,
  createListObjectsV2Handler,
} from './s3';
