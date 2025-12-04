# mock-aws-nodejs

A mock library for AWS SDK V3 to test code that depends on AWS services.

## Features

- Seamlessly mock AWS SDK V3 clients in your tests
- In-memory storage for realistic behavior
- Full compliance with AWS SDK V3 response types
- Extensible architecture for adding more AWS services
- Custom mock behaviors for edge case testing
- Call tracking for verification

## Installation

```bash
npm install mock-aws-nodejs --save-dev
```

## Supported Services

### S3 - Comprehensive Support

The library now supports **all major S3 operations** including bucket management, object operations, multipart uploads, tagging, ACLs, and bucket configurations.

#### Bucket Operations
- `CreateBucketCommand` - Create buckets
- `DeleteBucketCommand` - Delete buckets
- `ListBucketsCommand` - List all buckets
- `HeadBucketCommand` - Check if bucket exists
- `GetBucketLocationCommand` - Get bucket region

#### Object Operations
- `PutObjectCommand` - Store objects
- `GetObjectCommand` - Retrieve objects (with range request support)
- `HeadObjectCommand` - Get object metadata
- `DeleteObjectCommand` - Delete single object
- `CopyObjectCommand` - Copy objects between buckets/keys
- `DeleteObjectsCommand` - Batch delete objects
- `ListObjectsCommand` - List objects (V1)
- `ListObjectsV2Command` - List objects (V2 with pagination, prefix, delimiter support)
- `GetObjectAttributesCommand` - Get object attributes and checksums

#### Object Tagging
- `GetObjectTaggingCommand` - Get object tags
- `PutObjectTaggingCommand` - Set object tags
- `DeleteObjectTaggingCommand` - Remove object tags

#### Object ACL
- `GetObjectAclCommand` - Get object access control list
- `PutObjectAclCommand` - Set object access control list

#### Multipart Upload
- `CreateMultipartUploadCommand` - Initiate multipart upload
- `UploadPartCommand` - Upload a part
- `UploadPartCopyCommand` - Copy a part from another object
- `CompleteMultipartUploadCommand` - Complete multipart upload
- `AbortMultipartUploadCommand` - Abort multipart upload
- `ListMultipartUploadsCommand` - List in-progress uploads
- `ListPartsCommand` - List uploaded parts

#### Bucket Versioning
- `GetBucketVersioningCommand` - Get versioning configuration
- `PutBucketVersioningCommand` - Set versioning configuration

#### Bucket CORS
- `GetBucketCorsCommand` - Get CORS configuration
- `PutBucketCorsCommand` - Set CORS configuration
- `DeleteBucketCorsCommand` - Remove CORS configuration

#### Bucket Policy
- `GetBucketPolicyCommand` - Get bucket policy
- `PutBucketPolicyCommand` - Set bucket policy
- `DeleteBucketPolicyCommand` - Remove bucket policy

#### Bucket ACL
- `GetBucketAclCommand` - Get bucket access control list
- `PutBucketAclCommand` - Set bucket access control list

#### Bucket Encryption
- `GetBucketEncryptionCommand` - Get encryption configuration
- `PutBucketEncryptionCommand` - Set encryption configuration
- `DeleteBucketEncryptionCommand` - Remove encryption configuration

#### Bucket Lifecycle
- `GetBucketLifecycleConfigurationCommand` - Get lifecycle rules
- `PutBucketLifecycleConfigurationCommand` - Set lifecycle rules
- `DeleteBucketLifecycleCommand` - Remove lifecycle configuration

#### Bucket Website
- `GetBucketWebsiteCommand` - Get website configuration
- `PutBucketWebsiteCommand` - Set website configuration
- `DeleteBucketWebsiteCommand` - Remove website configuration

#### Bucket Tagging
- `GetBucketTaggingCommand` - Get bucket tags
- `PutBucketTaggingCommand` - Set bucket tags
- `DeleteBucketTaggingCommand` - Remove bucket tags

#### Bucket Logging
- `GetBucketLoggingCommand` - Get logging configuration
- `PutBucketLoggingCommand` - Set logging configuration

## Usage

### Basic Example (Jest)

```typescript
import { S3Client, PutObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
import { S3MockClient } from 'mock-aws-nodejs';

describe('My S3 tests', () => {
  let s3Client: S3Client;
  let mockClient: S3MockClient;

  beforeEach(() => {
    // Create a real S3 client
    s3Client = new S3Client({ region: 'us-east-1' });

    // Create and install the mock
    mockClient = new S3MockClient();
    mockClient.install(s3Client);
  });

  afterEach(() => {
    // Clean up
    mockClient.restore();
    mockClient.reset();
  });

  it('should store and retrieve objects', async () => {
    // Put an object
    await s3Client.send(new PutObjectCommand({
      Bucket: 'my-bucket',
      Key: 'test.txt',
      Body: 'Hello, World!',
    }));

    // Get the object
    const response = await s3Client.send(new GetObjectCommand({
      Bucket: 'my-bucket',
      Key: 'test.txt',
    }));

    const body = await response.Body?.transformToString();
    expect(body).toBe('Hello, World!');
  });
});
```

### Using with Node.js Built-in Test Module

The library fully supports Node.js's built-in `node:test` module (Node.js 18+):

```javascript
// test/s3.test.mjs - Run with: node --test test/s3.test.mjs
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert';
import { S3Client, PutObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
import { S3MockClient, S3MockStore } from 'mock-aws-nodejs';

describe('S3 operations', () => {
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

  it('should store and retrieve objects', async () => {
    await s3Client.send(new PutObjectCommand({
      Bucket: 'my-bucket',
      Key: 'test.txt',
      Body: 'Hello, World!',
    }));

    const response = await s3Client.send(new GetObjectCommand({
      Bucket: 'my-bucket',
      Key: 'test.txt',
    }));

    const body = await response.Body?.transformToString();
    assert.strictEqual(body, 'Hello, World!');
  });

  it('should throw NoSuchKey for missing objects', async () => {
    await assert.rejects(
      async () => {
        await s3Client.send(new GetObjectCommand({
          Bucket: 'my-bucket',
          Key: 'nonexistent.txt',
        }));
      },
      (error) => {
        assert.strictEqual(error.name, 'NoSuchKey');
        assert.strictEqual(error.$metadata?.httpStatusCode, 404);
        return true;
      }
    );
  });
});
```

Run with:
```bash
node --test test/s3.test.mjs
```

### Quick Setup with Helper Function

```typescript
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { mockS3Client } from 'mock-aws-nodejs';

const s3Client = new S3Client({ region: 'us-east-1' });
const mockClient = mockS3Client(s3Client);

// Use s3Client as normal - all calls are mocked
```

### Seeding Test Data

```typescript
mockClient.seed({
  'my-bucket': {
    'file1.txt': 'content1',
    'file2.txt': Buffer.from('binary content'),
    'file3.txt': {
      body: 'content with metadata',
      contentType: 'text/plain',
      metadata: { 'x-custom': 'value' }
    },
  },
});
```

### Custom Mock Behaviors

Override default handlers for specific test scenarios:

```typescript
// Return a custom response
mockClient.on(GetObjectCommand).resolves({
  $metadata: { httpStatusCode: 200 },
  Body: undefined,
  ETag: '"custom-etag"',
});

// Simulate an error
mockClient.on(GetObjectCommand).rejects(new Error('Network error'));

// Custom handler with access to input
mockClient.on(GetObjectCommand).callsFake((input) => {
  return {
    $metadata: { httpStatusCode: 200 },
    ETag: `"etag-for-${input.Key}"`,
  };
});
```

### Verifying Calls

```typescript
// Get all calls to a specific command
const putCalls = mockClient.commandCalls(PutObjectCommand);
expect(putCalls).toHaveLength(2);
expect(putCalls[0].input.Bucket).toBe('my-bucket');

// Get all recorded calls
const allCalls = mockClient.allCalls();
```

### Direct Store Access

Access the underlying store for advanced scenarios:

```typescript
const store = mockClient.getStore();

// Manually add objects
store.putObject('bucket', 'key', Buffer.from('data'));

// Check object existence
const exists = store.objectExists('bucket', 'key');

// Get a snapshot of all data
const snapshot = store.snapshot();
```

## Error Handling

The mock returns AWS-compatible errors:

```typescript
try {
  await s3Client.send(new GetObjectCommand({
    Bucket: 'bucket',
    Key: 'nonexistent',
  }));
} catch (error) {
  console.log(error.name);           // 'NoSuchKey'
  console.log(error.$fault);         // 'client'
  console.log(error.$metadata.httpStatusCode); // 404
}
```

## Architecture

The library is designed for extensibility:

```
src/
  core/
    MockClient.ts      # Base mock client class
    types.ts           # Core type definitions
  s3/
    S3MockClient.ts    # S3-specific mock client
    S3MockStore.ts     # In-memory S3 storage
    handlers.ts        # Command handlers
```

To add support for a new AWS service:
1. Create a new store class extending the pattern in `S3MockStore`
2. Create command handlers following the pattern in `handlers.ts`
3. Create a service-specific mock client extending `MockClient`

## License

MIT
