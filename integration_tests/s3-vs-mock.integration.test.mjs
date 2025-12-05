// Integration tests that compare mock-aws-nodejs S3 mock behaviour against real AWS S3
//
// These tests use the Node.js core `node:test` module and the AWS SDK v3 for JavaScript.
// They make real network calls to AWS S3. Run them only against a dedicated test account.
//
// Requirements:
// - Valid AWS credentials available in the environment (e.g. AWS_PROFILE / AWS_ACCESS_KEY_ID,...)
// - Optional: AWS_REGION (defaults to us-east-1)
// - Optional: AWS_S3_TEST_BUCKET_PREFIX (defaults to "mock-aws-nodejs-it")
// - The library must be built first so that `dist/index.js` exists (e.g. `npm run build`).

import test from 'node:test';
import assert from 'node:assert/strict';
import {
  S3Client,
  CreateBucketCommand,
  DeleteBucketCommand,
  PutObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  DeleteObjectCommand,
  ListObjectsCommand,
  CopyObjectCommand,
  DeleteObjectsCommand,
  GetObjectAttributesCommand,
  GetObjectTaggingCommand,
  PutObjectTaggingCommand,
  DeleteObjectTaggingCommand,
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
  HeadBucketCommand,
  ListBucketsCommand,
  GetBucketLocationCommand,
  PutBucketOwnershipControlsCommand,
} from '@aws-sdk/client-s3';
import { STSClient, GetCallerIdentityCommand } from '@aws-sdk/client-sts';
import lib from '../dist/index.js';

// dist/index.js is CommonJS; when imported from ESM we get its exports under the default.
const { S3MockClient } = lib;

const region = process.env.AWS_REGION || 'us-east-1';
const bucketPrefix = process.env.AWS_S3_TEST_BUCKET_PREFIX || 'mock-aws-nodejs-it';

let accountIdPromise;
async function getAccountId() {
  if (!accountIdPromise) {
    const sts = new STSClient({ region });
    accountIdPromise = sts.send(new GetCallerIdentityCommand({})).then((r) => r.Account);
  }
  return accountIdPromise;
}

function uniqueBucketName(suffix) {
  const ts = Date.now().toString(36);
  const rand = Math.random().toString(36).slice(2, 8);
  // S3 bucket names must be lowercase and follow DNS rules
  return `${bucketPrefix}-${suffix}-${ts}-${rand}`.toLowerCase();
}

function createClients() {
  const realS3 = new S3Client({ region });
  const s3ForMock = new S3Client({ region });
  const mockClient = new S3MockClient();
  mockClient.install(s3ForMock);
  return { realS3, s3ForMock, mockClient };
}

async function createBucket(client, bucket) {
  const input = { Bucket: bucket };
  // In us-east-1 the LocationConstraint must be omitted
  if (region !== 'us-east-1') {
    input.CreateBucketConfiguration = { LocationConstraint: region };
  }
  await client.send(new CreateBucketCommand(input));
}

async function deleteBucketRecursively(client, bucket) {
  // Attempt full cleanup; let errors propagate so tests fail loudly if cleanup fails.
  let continuationToken;
  do {
    const res = await client.send(
      new ListObjectsV2Command({ Bucket: bucket, ContinuationToken: continuationToken })
    );
    const contents = res.Contents ?? [];
    for (const obj of contents) {
      if (obj.Key) {
        await client.send(new DeleteObjectCommand({ Bucket: bucket, Key: obj.Key }));
      }
    }
    continuationToken = res.IsTruncated ? res.NextContinuationToken : undefined;
  } while (continuationToken);

  await client.send(new DeleteBucketCommand({ Bucket: bucket }));
}

async function withIsolatedBuckets(t, fn) {
  const { realS3, s3ForMock, mockClient } = createClients();
  const realBucket = uniqueBucketName('real');
  const mockBucket = uniqueBucketName('mock');

  await Promise.all([
    createBucket(realS3, realBucket),
    createBucket(s3ForMock, mockBucket),
  ]);

  t.diagnostic(`Using buckets real=${realBucket}, mock=${mockBucket} in region=${region}`);

  try {
    await fn({ realS3, s3ForMock, realBucket, mockBucket });
  } finally {
    await Promise.all([
      deleteBucketRecursively(realS3, realBucket),
      deleteBucketRecursively(s3ForMock, mockBucket),
    ]);
    mockClient.restore();
    mockClient.reset();
  }
}

// -----------------------------------------------------------------------------
// Basic object lifecycle: PutObject / GetObject / HeadObject
// -----------------------------------------------------------------------------

test('PutObject/GetObject/HeadObject behaviour matches real AWS S3', async (t) => {
  await withIsolatedBuckets(t, async ({ realS3, s3ForMock, realBucket, mockBucket }) => {
    const key = 'dir/example.txt';
    const body = 'Hello from integration test';

    const [realPut, mockPut] = await Promise.all([
      realS3.send(
        new PutObjectCommand({
          Bucket: realBucket,
          Key: key,
          Body: body,
          ContentType: 'text/plain',
        })
      ),
      s3ForMock.send(
        new PutObjectCommand({
          Bucket: mockBucket,
          Key: key,
          Body: body,
          ContentType: 'text/plain',
        })
      ),
    ]);

    assert.equal(realPut.$metadata.httpStatusCode, 200);
    assert.equal(mockPut.$metadata.httpStatusCode, 200);
    assert.ok(typeof realPut.ETag === 'string');
    assert.ok(typeof mockPut.ETag === 'string');
    // For non-multipart uploads, both real S3 and the mock compute an MD5-based ETag
    assert.equal(realPut.ETag, mockPut.ETag);

    const [realGet, mockGet] = await Promise.all([
      realS3.send(new GetObjectCommand({ Bucket: realBucket, Key: key })),
      s3ForMock.send(new GetObjectCommand({ Bucket: mockBucket, Key: key })),
    ]);

    const realBody = await realGet.Body?.transformToString();
    const mockBody = await mockGet.Body?.transformToString();

    assert.equal(realBody, body);
    assert.equal(mockBody, body);
    assert.equal(realGet.ContentType, 'text/plain');
    assert.equal(mockGet.ContentType, 'text/plain');

    const [realHead, mockHead] = await Promise.all([
      realS3.send(new HeadObjectCommand({ Bucket: realBucket, Key: key })),
      s3ForMock.send(new HeadObjectCommand({ Bucket: mockBucket, Key: key })),
    ]);

    assert.equal(realHead.ContentLength, mockHead.ContentLength);
    assert.equal(realHead.ContentType, mockHead.ContentType);
    assert.equal(realHead.ETag, mockHead.ETag);
  });
});

// -----------------------------------------------------------------------------
// Listing semantics: ListObjectsV2 with prefix and delimiter
// -----------------------------------------------------------------------------

test('ListObjectsV2 prefix and delimiter behaviour matches real AWS S3', async (t) => {
  await withIsolatedBuckets(t, async ({ realS3, s3ForMock, realBucket, mockBucket }) => {
    const keys = ['a/file1.txt', 'a/file2.txt', 'b/file3.txt', 'root.txt'];
    const body = 'x';

    await Promise.all([
      ...keys.map((key) =>
        realS3.send(new PutObjectCommand({ Bucket: realBucket, Key: key, Body: body }))
      ),
      ...keys.map((key) =>
        s3ForMock.send(new PutObjectCommand({ Bucket: mockBucket, Key: key, Body: body }))
      ),
    ]);

    const [realList, mockList] = await Promise.all([
      realS3.send(
        new ListObjectsV2Command({ Bucket: realBucket, Prefix: 'a/', Delimiter: '/' })
      ),
      s3ForMock.send(
        new ListObjectsV2Command({ Bucket: mockBucket, Prefix: 'a/', Delimiter: '/' })
      ),
    ]);

    const realKeys = (realList.Contents ?? [])
      .map((o) => o.Key)
      .filter((k) => typeof k === 'string')
      .sort();
    const mockKeys = (mockList.Contents ?? [])
      .map((o) => o.Key)
      .filter((k) => typeof k === 'string')
      .sort();

    assert.deepEqual(realKeys, mockKeys);

    const realPrefixes = (realList.CommonPrefixes ?? [])
      .map((p) => p.Prefix)
      .filter((p) => typeof p === 'string')
      .sort();
    const mockPrefixes = (mockList.CommonPrefixes ?? [])
      .map((p) => p.Prefix)
      .filter((p) => typeof p === 'string')
      .sort();

    assert.deepEqual(realPrefixes, mockPrefixes);
  });
});

// -----------------------------------------------------------------------------
// Error behaviour: GetObject on a missing key
// -----------------------------------------------------------------------------

test('GetObject on a missing key matches NoSuchKey error shape', async (t) => {
  await withIsolatedBuckets(t, async ({ realS3, s3ForMock, realBucket, mockBucket }) => {
    const missingKey = 'does-not-exist.txt';

    const [realResult, mockResult] = await Promise.allSettled([
      realS3.send(new GetObjectCommand({ Bucket: realBucket, Key: missingKey })),
      s3ForMock.send(new GetObjectCommand({ Bucket: mockBucket, Key: missingKey })),
    ]);

    assert.equal(realResult.status, 'rejected');
    assert.equal(mockResult.status, 'rejected');

    const realErr = /** @type {any} */ (realResult).reason;
    const mockErr = /** @type {any} */ (mockResult).reason;

    assert.equal(realErr.name, 'NoSuchKey');
    assert.equal(mockErr.name, 'NoSuchKey');
    assert.equal(realErr.$metadata?.httpStatusCode, 404);
    assert.equal(mockErr.$metadata?.httpStatusCode, 404);
  });
});

// -----------------------------------------------------------------------------
// Bucket metadata: HeadBucket, GetBucketLocation, ListBuckets presence
// -----------------------------------------------------------------------------

test('Bucket metadata operations behave consistently', async (t) => {
  await withIsolatedBuckets(t, async ({ realS3, s3ForMock, realBucket, mockBucket }) => {
    // HeadBucket
    const [realHead, mockHead] = await Promise.all([
      realS3.send(new HeadBucketCommand({ Bucket: realBucket })),
      s3ForMock.send(new HeadBucketCommand({ Bucket: mockBucket })),
    ]);
    assert.equal(realHead.$metadata.httpStatusCode, 200);
    assert.equal(mockHead.$metadata.httpStatusCode, 200);

    // GetBucketLocation
    const [realLoc, mockLoc] = await Promise.all([
      realS3.send(new GetBucketLocationCommand({ Bucket: realBucket })),
      s3ForMock.send(new GetBucketLocationCommand({ Bucket: mockBucket })),
    ]);

    // In us-east-1, LocationConstraint is null/undefined for classic region.
    const realLocation = realLoc.LocationConstraint || 'us-east-1';
    const mockLocation = mockLoc.LocationConstraint || 'us-east-1';
    assert.equal(realLocation, mockLocation);

    // ListBuckets: ensure each service sees its own bucket
    const [realList, mockList] = await Promise.all([
      realS3.send(new ListBucketsCommand({})),
      s3ForMock.send(new ListBucketsCommand({})),
    ]);

    const realNames = (realList.Buckets ?? []).map((b) => b.Name).filter(Boolean);
    const mockNames = (mockList.Buckets ?? []).map((b) => b.Name).filter(Boolean);

    assert.ok(realNames.includes(realBucket));
    assert.deepEqual(mockNames.sort(), [mockBucket].sort());
  });
});

// -----------------------------------------------------------------------------
// Object operations: CopyObject, DeleteObjects, ListObjects (v1), GetObjectAttributes
// -----------------------------------------------------------------------------

test('Additional object operations behave consistently', async (t) => {
  await withIsolatedBuckets(t, async ({ realS3, s3ForMock, realBucket, mockBucket }) => {
    const key1 = 'folder/file1.txt';
    const key2 = 'folder/file2.txt';
    const copyKey = 'folder/copied.txt';

    await Promise.all([
      realS3.send(new PutObjectCommand({ Bucket: realBucket, Key: key1, Body: 'one' })),
      realS3.send(new PutObjectCommand({ Bucket: realBucket, Key: key2, Body: 'two' })),
      s3ForMock.send(new PutObjectCommand({ Bucket: mockBucket, Key: key1, Body: 'one' })),
      s3ForMock.send(new PutObjectCommand({ Bucket: mockBucket, Key: key2, Body: 'two' })),
    ]);

    // CopyObject
    const copySourceReal = `/${realBucket}/${encodeURIComponent(key1)}`;
    const copySourceMock = `/${mockBucket}/${encodeURIComponent(key1)}`;

    const [realCopy, mockCopy] = await Promise.all([
      realS3.send(
        new CopyObjectCommand({ Bucket: realBucket, Key: copyKey, CopySource: copySourceReal })
      ),
      s3ForMock.send(
        new CopyObjectCommand({ Bucket: mockBucket, Key: copyKey, CopySource: copySourceMock })
      ),
    ]);

    assert.equal(realCopy.$metadata.httpStatusCode, 200);
    assert.equal(mockCopy.$metadata.httpStatusCode, 200);

    // ListObjects (v1)
    const [realList, mockList] = await Promise.all([
      realS3.send(new ListObjectsCommand({ Bucket: realBucket, Prefix: 'folder/' })),
      s3ForMock.send(new ListObjectsCommand({ Bucket: mockBucket, Prefix: 'folder/' })),
    ]);

    const realKeys = (realList.Contents ?? []).map((o) => o.Key).filter(Boolean).sort();
    const mockKeys = (mockList.Contents ?? []).map((o) => o.Key).filter(Boolean).sort();

    assert.deepEqual(realKeys, mockKeys);

    // GetObjectAttributes on the copied object
    const [realAttr, mockAttr] = await Promise.all([
      realS3.send(
        new GetObjectAttributesCommand({
          Bucket: realBucket,
          Key: copyKey,
          ObjectAttributes: ['ETag', 'ObjectSize'],
        })
      ),
      s3ForMock.send(
        new GetObjectAttributesCommand({
          Bucket: mockBucket,
          Key: copyKey,
          ObjectAttributes: ['ETag', 'ObjectSize'],
        })
      ),
    ]);

    // Normalize ETags because some APIs return quoted values and others do not.
    const normalizeEtag = (etag) =>
      typeof etag === 'string' ? etag.replace(/^"|"$/g, '') : etag;

    assert.equal(normalizeEtag(realAttr.ETag), normalizeEtag(mockAttr.ETag));
    assert.equal(realAttr.ObjectSize, mockAttr.ObjectSize);

    // Exercise DeleteObjects (multi-object delete)
    const deleteInputReal = {
      Bucket: realBucket,
      Delete: { Objects: [{ Key: key1 }, { Key: key2 }, { Key: copyKey }] },
    };
    const deleteInputMock = {
      Bucket: mockBucket,
      Delete: { Objects: [{ Key: key1 }, { Key: key2 }, { Key: copyKey }] },
    };

    const [realDel, mockDel] = await Promise.all([
      realS3.send(new DeleteObjectsCommand(deleteInputReal)),
      s3ForMock.send(new DeleteObjectsCommand(deleteInputMock)),
    ]);

    assert.equal((realDel.Deleted ?? []).length, (mockDel.Deleted ?? []).length);
  });
});

// -----------------------------------------------------------------------------
// Object tagging: Get/Put/DeleteObjectTagging
// -----------------------------------------------------------------------------

test('Object tagging behaves consistently', async (t) => {
  await withIsolatedBuckets(t, async ({ realS3, s3ForMock, realBucket, mockBucket }) => {
    const key = 'tagged.txt';
    await Promise.all([
      realS3.send(new PutObjectCommand({ Bucket: realBucket, Key: key, Body: 'x' })),
      s3ForMock.send(new PutObjectCommand({ Bucket: mockBucket, Key: key, Body: 'x' })),
    ]);

    const tagging = { TagSet: [{ Key: 'env', Value: 'test' }] };

    await Promise.all([
      realS3.send(
        new PutObjectTaggingCommand({ Bucket: realBucket, Key: key, Tagging: tagging })
      ),
      s3ForMock.send(
        new PutObjectTaggingCommand({ Bucket: mockBucket, Key: key, Tagging: tagging })
      ),
    ]);

    const [realTags, mockTags] = await Promise.all([
      realS3.send(new GetObjectTaggingCommand({ Bucket: realBucket, Key: key })),
      s3ForMock.send(new GetObjectTaggingCommand({ Bucket: mockBucket, Key: key })),
    ]);

    assert.deepEqual(realTags.TagSet ?? [], mockTags.TagSet ?? []);

    await Promise.all([
      realS3.send(new DeleteObjectTaggingCommand({ Bucket: realBucket, Key: key })),
      s3ForMock.send(new DeleteObjectTaggingCommand({ Bucket: mockBucket, Key: key })),
    ]);

    const [realAfter, mockAfter] = await Promise.allSettled([
      realS3.send(new GetObjectTaggingCommand({ Bucket: realBucket, Key: key })),
      s3ForMock.send(new GetObjectTaggingCommand({ Bucket: mockBucket, Key: key })),
    ]);

    // AWS returns an empty TagSet after delete; mock should behave the same.
    assert.equal(realAfter.status, 'fulfilled');
    assert.equal(mockAfter.status, 'fulfilled');
    assert.deepEqual(realAfter.value.TagSet ?? [], []);
    assert.deepEqual(mockAfter.value.TagSet ?? [], []);
  });
});

// -----------------------------------------------------------------------------
// Multipart uploads: create, upload parts, complete, list uploads/parts, abort
// -----------------------------------------------------------------------------

test('Multipart upload lifecycle behaves consistently', async (t) => {
  await withIsolatedBuckets(t, async ({ realS3, s3ForMock, realBucket, mockBucket }) => {
    const key = 'multipart.bin';
    // Each part must be at least 5 MB for real S3 multipart uploads.
    const part1 = Buffer.alloc(5 * 1024 * 1024, 'a');
    const part2 = Buffer.alloc(5 * 1024 * 1024, 'b');

    // Create multipart uploads
    const [realCreate, mockCreate] = await Promise.all([
      realS3.send(new CreateMultipartUploadCommand({ Bucket: realBucket, Key: key })),
      s3ForMock.send(new CreateMultipartUploadCommand({ Bucket: mockBucket, Key: key })),
    ]);

    assert.ok(realCreate.UploadId);
    assert.ok(mockCreate.UploadId);

    // Upload parts
    const [realPart1, mockPart1] = await Promise.all([
      realS3.send(
        new UploadPartCommand({
          Bucket: realBucket,
          Key: key,
          UploadId: realCreate.UploadId,
          PartNumber: 1,
          Body: part1,
        })
      ),
      s3ForMock.send(
        new UploadPartCommand({
          Bucket: mockBucket,
          Key: key,
          UploadId: mockCreate.UploadId,
          PartNumber: 1,
          Body: part1,
        })
      ),
    ]);

    const [realPart2, mockPart2] = await Promise.all([
      realS3.send(
        new UploadPartCommand({
          Bucket: realBucket,
          Key: key,
          UploadId: realCreate.UploadId,
          PartNumber: 2,
          Body: part2,
        })
      ),
      s3ForMock.send(
        new UploadPartCommand({
          Bucket: mockBucket,
          Key: key,
          UploadId: mockCreate.UploadId,
          PartNumber: 2,
          Body: part2,
        })
      ),
    ]);

    assert.ok(realPart1.ETag);
    assert.ok(realPart2.ETag);
    assert.ok(mockPart1.ETag);
    assert.ok(mockPart2.ETag);

    // List parts
    const [realParts, mockParts] = await Promise.all([
      realS3.send(
        new ListPartsCommand({ Bucket: realBucket, Key: key, UploadId: realCreate.UploadId })
      ),
      s3ForMock.send(
        new ListPartsCommand({ Bucket: mockBucket, Key: key, UploadId: mockCreate.UploadId })
      ),
    ]);

    assert.equal((realParts.Parts ?? []).length, (mockParts.Parts ?? []).length);

    // Complete uploads
    const [realComplete, mockComplete] = await Promise.all([
      realS3.send(
        new CompleteMultipartUploadCommand({
          Bucket: realBucket,
          Key: key,
          UploadId: realCreate.UploadId,
          MultipartUpload: {
            Parts: [
              { PartNumber: 1, ETag: realPart1.ETag },
              { PartNumber: 2, ETag: realPart2.ETag },
            ],
          },
        })
      ),
      s3ForMock.send(
        new CompleteMultipartUploadCommand({
          Bucket: mockBucket,
          Key: key,
          UploadId: mockCreate.UploadId,
          MultipartUpload: {
            Parts: [
              { PartNumber: 1, ETag: mockPart1.ETag },
              { PartNumber: 2, ETag: mockPart2.ETag },
            ],
          },
        })
      ),
    ]);

    assert.equal(realComplete.Bucket, realBucket);
    assert.equal(mockComplete.Bucket, mockBucket);

    const totalSize = part1.length + part2.length;
    const [realHead, mockHead] = await Promise.all([
      realS3.send(new HeadObjectCommand({ Bucket: realBucket, Key: key })),
      s3ForMock.send(new HeadObjectCommand({ Bucket: mockBucket, Key: key })),
    ]);

    assert.equal(realHead.ContentLength, totalSize);
    assert.equal(mockHead.ContentLength, totalSize);

    // There should no longer be active multipart uploads for this key
    const [realListUploads, mockListUploads] = await Promise.all([
      realS3.send(new ListMultipartUploadsCommand({ Bucket: realBucket })),
      s3ForMock.send(new ListMultipartUploadsCommand({ Bucket: mockBucket })),
    ]);

    assert.equal((realListUploads.Uploads ?? []).length, 0);
    assert.equal((mockListUploads.Uploads ?? []).length, 0);
  });
});

// -----------------------------------------------------------------------------
// Bucket versioning
// -----------------------------------------------------------------------------

test('Bucket versioning configuration behaves consistently', async (t) => {
  await withIsolatedBuckets(t, async ({ realS3, s3ForMock, realBucket, mockBucket }) => {
    const cfg = { Status: 'Enabled' };

    const [realPut, mockPut] = await Promise.allSettled([
      realS3.send(
        new PutBucketVersioningCommand({
          Bucket: realBucket,
          VersioningConfiguration: cfg,
        })
      ),
      s3ForMock.send(
        new PutBucketVersioningCommand({
          Bucket: mockBucket,
          VersioningConfiguration: cfg,
        })
      ),
    ]);

    if (realPut.status === 'rejected') {
      const err = realPut.reason;
      if (err?.name === 'AccessDenied' || err?.$metadata?.httpStatusCode === 403) {
        t.skip('AccessDenied for PutBucketVersioning on real AWS; skipping comparison');
      }
      throw err;
    }

    if (mockPut.status === 'rejected') {
      throw mockPut.reason;
    }

    const [realGet, mockGet] = await Promise.all([
      realS3.send(new GetBucketVersioningCommand({ Bucket: realBucket })),
      s3ForMock.send(new GetBucketVersioningCommand({ Bucket: mockBucket })),
    ]);

    assert.equal(realGet.Status ?? undefined, mockGet.Status ?? undefined);
  });
});

// -----------------------------------------------------------------------------
// Bucket CORS
// -----------------------------------------------------------------------------

test('Bucket CORS configuration behaves consistently', async (t) => {
  await withIsolatedBuckets(t, async ({ realS3, s3ForMock, realBucket, mockBucket }) => {
    const corsCfg = {
      CORSRules: [
        {
          AllowedMethods: ['GET'],
          AllowedOrigins: ['*'],
        },
      ],
    };

    const [realPut, mockPut] = await Promise.allSettled([
      realS3.send(
        new PutBucketCorsCommand({ Bucket: realBucket, CORSConfiguration: corsCfg })
      ),
      s3ForMock.send(
        new PutBucketCorsCommand({ Bucket: mockBucket, CORSConfiguration: corsCfg })
      ),
    ]);

    if (realPut.status === 'rejected') {
      const err = realPut.reason;
      if (err?.name === 'AccessDenied' || err?.$metadata?.httpStatusCode === 403) {
        t.skip('AccessDenied for PutBucketCors on real AWS; skipping comparison');
      }
      throw err;
    }
    if (mockPut.status === 'rejected') {
      throw mockPut.reason;
    }

    const [realGet, mockGet] = await Promise.all([
      realS3.send(new GetBucketCorsCommand({ Bucket: realBucket })),
      s3ForMock.send(new GetBucketCorsCommand({ Bucket: mockBucket })),
    ]);

    assert.deepEqual(realGet.CORSRules ?? [], mockGet.CORSRules ?? []);

    await Promise.all([
      realS3.send(new DeleteBucketCorsCommand({ Bucket: realBucket })),
      s3ForMock.send(new DeleteBucketCorsCommand({ Bucket: mockBucket })),
    ]);
  });
});

// -----------------------------------------------------------------------------
// Bucket policy
// -----------------------------------------------------------------------------

test('Bucket policy behaves consistently (where permitted)', async (t) => {
  await withIsolatedBuckets(t, async ({ realS3, s3ForMock, realBucket, mockBucket }) => {
    const accountId = await getAccountId();
    const principalArn = `arn:aws:iam::${accountId}:root`;

    const policyObject = {
      Version: '2012-10-17',
      Statement: [
        {
          Sid: 'TestStatement',
          Effect: 'Allow',
          Principal: { AWS: principalArn },
          Action: ['s3:GetObject'],
          Resource: [`arn:aws:s3:::${realBucket}/*`],
        },
      ],
    };

    const policyString = JSON.stringify(policyObject);

    const [realPut, mockPut] = await Promise.allSettled([
      realS3.send(new PutBucketPolicyCommand({ Bucket: realBucket, Policy: policyString })),
      s3ForMock.send(new PutBucketPolicyCommand({ Bucket: mockBucket, Policy: policyString })),
    ]);

    if (realPut.status === 'rejected') {
      const err = realPut.reason;
      if (err?.name === 'AccessDenied' || err?.$metadata?.httpStatusCode === 403) {
        t.skip('AccessDenied for PutBucketPolicy on real AWS; skipping comparison');
      }
      throw err;
    }
    if (mockPut.status === 'rejected') {
      throw mockPut.reason;
    }

    const [realGet, mockGet] = await Promise.all([
      realS3.send(new GetBucketPolicyCommand({ Bucket: realBucket })),
      s3ForMock.send(new GetBucketPolicyCommand({ Bucket: mockBucket })),
    ]);

    const realPolicyObj = JSON.parse(realGet.Policy ?? '{}');
    const mockPolicyObj = JSON.parse(mockGet.Policy ?? '{}');
    assert.deepEqual(realPolicyObj, mockPolicyObj);

    await Promise.all([
      realS3.send(new DeleteBucketPolicyCommand({ Bucket: realBucket })),
      s3ForMock.send(new DeleteBucketPolicyCommand({ Bucket: mockBucket })),
    ]);
  });
});

// -----------------------------------------------------------------------------
// Bucket ACL
// -----------------------------------------------------------------------------

async function enableBucketAclsOnRealBucket(realS3, bucket) {
  await realS3.send(
    new PutBucketOwnershipControlsCommand({
      Bucket: bucket,
      OwnershipControls: {
        Rules: [
          {
            ObjectOwnership: 'BucketOwnerPreferred',
          },
        ],
      },
    }),
  );
}

test('Bucket ACL behaves consistently (basic round-trip)', async (t) => {
  await withIsolatedBuckets(t, async ({ realS3, s3ForMock, realBucket, mockBucket }) => {
    const list = await realS3.send(new ListBucketsCommand({}));
    const canonicalId = list.Owner?.ID;
    if (!canonicalId) {
      t.skip('Unable to determine canonical user ID from ListBuckets owner');
    }

    try {
      await enableBucketAclsOnRealBucket(realS3, realBucket);
    } catch (err) {
      if (err?.name === 'AccessDenied' || err?.$metadata?.httpStatusCode === 403) {
        t.skip('AccessDenied for PutBucketOwnershipControls on real AWS; skipping ACL comparison');
      }
      throw err;
    }

    const acl = {
      Owner: { ID: canonicalId, DisplayName: 'owner' },
      Grants: [
        {
          Grantee: {
            Type: 'CanonicalUser',
            ID: canonicalId,
            DisplayName: 'owner',
          },
          Permission: 'FULL_CONTROL',
        },
      ],
    };

    const [realPut, mockPut] = await Promise.allSettled([
      realS3.send(new PutBucketAclCommand({ Bucket: realBucket, AccessControlPolicy: acl })),
      s3ForMock.send(new PutBucketAclCommand({ Bucket: mockBucket, AccessControlPolicy: acl })),
    ]);

    if (realPut.status === 'rejected') {
      const err = realPut.reason;
      if (err?.name === 'AccessDenied' || err?.$metadata?.httpStatusCode === 403) {
        t.skip('AccessDenied for PutBucketAcl on real AWS; skipping comparison');
      }
      throw err;
    }
    if (mockPut.status === 'rejected') {
      throw mockPut.reason;
    }

    const [realGet, mockGet] = await Promise.all([
      realS3.send(new GetBucketAclCommand({ Bucket: realBucket })),
      s3ForMock.send(new GetBucketAclCommand({ Bucket: mockBucket })),
    ]);

    assert.ok((realGet.Grants ?? []).length > 0);
    assert.ok((mockGet.Grants ?? []).length > 0);
  });
});

// -----------------------------------------------------------------------------
// Bucket encryption (SSE-S3 only)
// -----------------------------------------------------------------------------

test('Bucket encryption configuration behaves consistently for SSE-S3', async (t) => {
  await withIsolatedBuckets(t, async ({ realS3, s3ForMock, realBucket, mockBucket }) => {
    const cfg = {
      Rules: [
        {
          ApplyServerSideEncryptionByDefault: {
            SSEAlgorithm: 'AES256',
          },
        },
      ],
    };

    const [realPut, mockPut] = await Promise.allSettled([
      realS3.send(
        new PutBucketEncryptionCommand({
          Bucket: realBucket,
          ServerSideEncryptionConfiguration: cfg,
        })
      ),
      s3ForMock.send(
        new PutBucketEncryptionCommand({
          Bucket: mockBucket,
          ServerSideEncryptionConfiguration: cfg,
        })
      ),
    ]);

    if (realPut.status === 'rejected') {
      const err = realPut.reason;
      if (err?.name === 'AccessDenied' || err?.$metadata?.httpStatusCode === 403) {
        t.skip('AccessDenied for PutBucketEncryption on real AWS; skipping comparison');
      }
      throw err;
    }
    if (mockPut.status === 'rejected') {
      throw mockPut.reason;
    }

    const [realGet, mockGet] = await Promise.all([
      realS3.send(new GetBucketEncryptionCommand({ Bucket: realBucket })),
      s3ForMock.send(new GetBucketEncryptionCommand({ Bucket: mockBucket })),
    ]);

    const normalizeRules = (rules) =>
      (rules ?? []).map((r) => ({
        SSEAlgorithm: r.ApplyServerSideEncryptionByDefault?.SSEAlgorithm,
      }));

    assert.deepEqual(
      normalizeRules(realGet.ServerSideEncryptionConfiguration?.Rules),
      normalizeRules(mockGet.ServerSideEncryptionConfiguration?.Rules),
    );

    await Promise.all([
      realS3.send(new DeleteBucketEncryptionCommand({ Bucket: realBucket })),
      s3ForMock.send(new DeleteBucketEncryptionCommand({ Bucket: mockBucket })),
    ]);
  });
});

// -----------------------------------------------------------------------------
// Bucket lifecycle configuration
// -----------------------------------------------------------------------------

test('Bucket lifecycle configuration behaves consistently', async (t) => {
  await withIsolatedBuckets(t, async ({ realS3, s3ForMock, realBucket, mockBucket }) => {
    const lifecycleCfg = {
      Rules: [
        {
          ID: 'expire-temp',
          Status: 'Enabled',
          Filter: { Prefix: 'temp/' },
          Expiration: { Days: 30 },
        },
      ],
    };

    const [realPut, mockPut] = await Promise.allSettled([
      realS3.send(
        new PutBucketLifecycleConfigurationCommand({
          Bucket: realBucket,
          LifecycleConfiguration: lifecycleCfg,
        })
      ),
      s3ForMock.send(
        new PutBucketLifecycleConfigurationCommand({
          Bucket: mockBucket,
          LifecycleConfiguration: lifecycleCfg,
        })
      ),
    ]);

    if (realPut.status === 'rejected') {
      const err = realPut.reason;
      if (err?.name === 'AccessDenied' || err?.$metadata?.httpStatusCode === 403) {
        t.skip('AccessDenied for PutBucketLifecycleConfiguration on real AWS; skipping comparison');
      }
      throw err;
    }
    if (mockPut.status === 'rejected') {
      throw mockPut.reason;
    }

    const [realGet, mockGet] = await Promise.all([
      realS3.send(new GetBucketLifecycleConfigurationCommand({ Bucket: realBucket })),
      s3ForMock.send(new GetBucketLifecycleConfigurationCommand({ Bucket: mockBucket })),
    ]);

    assert.deepEqual(realGet.Rules ?? [], mockGet.Rules ?? []);

    await Promise.all([
      realS3.send(new DeleteBucketLifecycleCommand({ Bucket: realBucket })),
      s3ForMock.send(new DeleteBucketLifecycleCommand({ Bucket: mockBucket })),
    ]);
  });
});

// -----------------------------------------------------------------------------
// Bucket website configuration
// -----------------------------------------------------------------------------

test('Bucket website configuration behaves consistently', async (t) => {
  await withIsolatedBuckets(t, async ({ realS3, s3ForMock, realBucket, mockBucket }) => {
    const websiteCfg = {
      IndexDocument: { Suffix: 'index.html' },
      ErrorDocument: { Key: 'error.html' },
    };

    const [realPut, mockPut] = await Promise.allSettled([
      realS3.send(
        new PutBucketWebsiteCommand({ Bucket: realBucket, WebsiteConfiguration: websiteCfg })
      ),
      s3ForMock.send(
        new PutBucketWebsiteCommand({ Bucket: mockBucket, WebsiteConfiguration: websiteCfg })
      ),
    ]);

    if (realPut.status === 'rejected') {
      const err = realPut.reason;
      if (err?.name === 'AccessDenied' || err?.$metadata?.httpStatusCode === 403) {
        t.skip('AccessDenied for PutBucketWebsite on real AWS; skipping comparison');
      }
      throw err;
    }
    if (mockPut.status === 'rejected') {
      throw mockPut.reason;
    }

    const [realGet, mockGet] = await Promise.all([
      realS3.send(new GetBucketWebsiteCommand({ Bucket: realBucket })),
      s3ForMock.send(new GetBucketWebsiteCommand({ Bucket: mockBucket })),
    ]);

    // Compare selected fields
    assert.equal(realGet.IndexDocument?.Suffix, mockGet.IndexDocument?.Suffix);
    assert.equal(realGet.ErrorDocument?.Key, mockGet.ErrorDocument?.Key);

    await Promise.all([
      realS3.send(new DeleteBucketWebsiteCommand({ Bucket: realBucket })),
      s3ForMock.send(new DeleteBucketWebsiteCommand({ Bucket: mockBucket })),
    ]);
  });
});

// -----------------------------------------------------------------------------
// Bucket tagging
// -----------------------------------------------------------------------------

test('Bucket tagging behaves consistently', async (t) => {
  await withIsolatedBuckets(t, async ({ realS3, s3ForMock, realBucket, mockBucket }) => {
    const tagging = {
      TagSet: [
        { Key: 'env', Value: 'test' },
        { Key: 'team', Value: 'mock-aws-nodejs' },
      ],
    };

    const [realPut, mockPut] = await Promise.allSettled([
      realS3.send(new PutBucketTaggingCommand({ Bucket: realBucket, Tagging: tagging })),
      s3ForMock.send(new PutBucketTaggingCommand({ Bucket: mockBucket, Tagging: tagging })),
    ]);

    if (realPut.status === 'rejected') {
      const err = realPut.reason;
      if (err?.name === 'AccessDenied' || err?.$metadata?.httpStatusCode === 403) {
        t.skip('AccessDenied for PutBucketTagging on real AWS; skipping comparison');
      }
      throw err;
    }
    if (mockPut.status === 'rejected') {
      throw mockPut.reason;
    }

    const [realGet, mockGet] = await Promise.all([
      realS3.send(new GetBucketTaggingCommand({ Bucket: realBucket })),
      s3ForMock.send(new GetBucketTaggingCommand({ Bucket: mockBucket })),
    ]);

    assert.deepEqual(realGet.TagSet ?? [], mockGet.TagSet ?? []);

    await Promise.all([
      realS3.send(new DeleteBucketTaggingCommand({ Bucket: realBucket })),
      s3ForMock.send(new DeleteBucketTaggingCommand({ Bucket: mockBucket })),
    ]);
  });
});

// -----------------------------------------------------------------------------
// Bucket logging
// -----------------------------------------------------------------------------

test('Bucket logging behaves consistently (where permitted)', async (t) => {
  await withIsolatedBuckets(t, async ({ realS3, s3ForMock, realBucket, mockBucket }) => {
    // Use a separate logs bucket per side to avoid interfering with other tests.
    const realLogBucket = uniqueBucketName('real-logs');
    const mockLogBucket = uniqueBucketName('mock-logs');

    await Promise.all([
      createBucket(realS3, realLogBucket),
      createBucket(s3ForMock, mockLogBucket),
    ]);

    try {
      const loggingStatus = {
        LoggingEnabled: {
          TargetBucket: realLogBucket,
          TargetPrefix: 'logs/',
        },
      };

      const [realPut, mockPut] = await Promise.allSettled([
        realS3.send(
          new PutBucketLoggingCommand({
            Bucket: realBucket,
            BucketLoggingStatus: loggingStatus,
          })
        ),
        s3ForMock.send(
          new PutBucketLoggingCommand({
            Bucket: mockBucket,
            BucketLoggingStatus: {
              LoggingEnabled: {
                TargetBucket: mockLogBucket,
                TargetPrefix: 'logs/',
              },
            },
          })
        ),
      ]);

      if (realPut.status === 'rejected') {
        const err = realPut.reason;
        if (err?.name === 'AccessDenied' || err?.$metadata?.httpStatusCode === 403) {
          t.skip('AccessDenied for PutBucketLogging on real AWS; skipping comparison');
        }
        // Some environments disallow certain logging targets; treat as fatal otherwise.
        throw err;
      }
      if (mockPut.status === 'rejected') {
        throw mockPut.reason;
      }

      const [realGet, mockGet] = await Promise.all([
        realS3.send(new GetBucketLoggingCommand({ Bucket: realBucket })),
        s3ForMock.send(new GetBucketLoggingCommand({ Bucket: mockBucket })),
      ]);

      const realEnabled = realGet.LoggingEnabled;
      const mockEnabled = mockGet.LoggingEnabled;

      // Compare that both either have logging enabled or disabled; if enabled, compare prefixes.
      assert.equal(!!realEnabled, !!mockEnabled);
      if (realEnabled && mockEnabled) {
        assert.equal(realEnabled.TargetPrefix, mockEnabled.TargetPrefix);
      }
    } finally {
      await Promise.all([
        deleteBucketRecursively(realS3, realLogBucket),
        deleteBucketRecursively(s3ForMock, mockLogBucket),
      ]);
    }
  });
});
