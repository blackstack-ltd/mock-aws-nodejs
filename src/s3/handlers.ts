import { Readable } from 'stream';
import type {
  PutObjectCommandInput,
  PutObjectCommandOutput,
  GetObjectCommandInput,
  GetObjectCommandOutput,
  HeadObjectCommandInput,
  HeadObjectCommandOutput,
  DeleteObjectCommandInput,
  DeleteObjectCommandOutput,
  ListObjectsV2CommandInput,
  ListObjectsV2CommandOutput,
  CreateBucketCommandInput,
  CreateBucketCommandOutput,
  DeleteBucketCommandInput,
  DeleteBucketCommandOutput,
  ListBucketsCommandInput,
  ListBucketsCommandOutput,
  HeadBucketCommandInput,
  HeadBucketCommandOutput,
  CopyObjectCommandInput,
  CopyObjectCommandOutput,
  DeleteObjectsCommandInput,
  DeleteObjectsCommandOutput,
  ListObjectsCommandInput,
  ListObjectsCommandOutput,
  GetObjectTaggingCommandInput,
  GetObjectTaggingCommandOutput,
  PutObjectTaggingCommandInput,
  PutObjectTaggingCommandOutput,
  DeleteObjectTaggingCommandInput,
  DeleteObjectTaggingCommandOutput,
  GetObjectAclCommandInput,
  GetObjectAclCommandOutput,
  PutObjectAclCommandInput,
  PutObjectAclCommandOutput,
  CreateMultipartUploadCommandInput,
  CreateMultipartUploadCommandOutput,
  UploadPartCommandInput,
  UploadPartCommandOutput,
  CompleteMultipartUploadCommandInput,
  CompleteMultipartUploadCommandOutput,
  AbortMultipartUploadCommandInput,
  AbortMultipartUploadCommandOutput,
  ListMultipartUploadsCommandInput,
  ListMultipartUploadsCommandOutput,
  ListPartsCommandInput,
  ListPartsCommandOutput,
  GetBucketVersioningCommandInput,
  GetBucketVersioningCommandOutput,
  PutBucketVersioningCommandInput,
  PutBucketVersioningCommandOutput,
  GetBucketCorsCommandInput,
  GetBucketCorsCommandOutput,
  PutBucketCorsCommandInput,
  PutBucketCorsCommandOutput,
  DeleteBucketCorsCommandInput,
  DeleteBucketCorsCommandOutput,
  GetBucketPolicyCommandInput,
  GetBucketPolicyCommandOutput,
  PutBucketPolicyCommandInput,
  PutBucketPolicyCommandOutput,
  DeleteBucketPolicyCommandInput,
  DeleteBucketPolicyCommandOutput,
  GetBucketAclCommandInput,
  GetBucketAclCommandOutput,
  PutBucketAclCommandInput,
  PutBucketAclCommandOutput,
  GetBucketEncryptionCommandInput,
  GetBucketEncryptionCommandOutput,
  PutBucketEncryptionCommandInput,
  PutBucketEncryptionCommandOutput,
  DeleteBucketEncryptionCommandInput,
  DeleteBucketEncryptionCommandOutput,
  GetBucketLifecycleConfigurationCommandInput,
  GetBucketLifecycleConfigurationCommandOutput,
  PutBucketLifecycleConfigurationCommandInput,
  PutBucketLifecycleConfigurationCommandOutput,
  DeleteBucketLifecycleCommandInput,
  DeleteBucketLifecycleCommandOutput,
  GetBucketWebsiteCommandInput,
  GetBucketWebsiteCommandOutput,
  PutBucketWebsiteCommandInput,
  PutBucketWebsiteCommandOutput,
  DeleteBucketWebsiteCommandInput,
  DeleteBucketWebsiteCommandOutput,
  GetBucketTaggingCommandInput,
  GetBucketTaggingCommandOutput,
  PutBucketTaggingCommandInput,
  PutBucketTaggingCommandOutput,
  DeleteBucketTaggingCommandInput,
  DeleteBucketTaggingCommandOutput,
  GetBucketLoggingCommandInput,
  GetBucketLoggingCommandOutput,
  PutBucketLoggingCommandInput,
  PutBucketLoggingCommandOutput,
  GetBucketLocationCommandInput,
  GetBucketLocationCommandOutput,
  GetObjectAttributesCommandInput,
  GetObjectAttributesCommandOutput,
  UploadPartCopyCommandInput,
  UploadPartCopyCommandOutput,
} from '@aws-sdk/client-s3';
import type { SdkStream } from '@aws-sdk/types';
import { createAwsError, createResponseMetadata } from '../core/types';
import type { S3MockStore } from './S3MockStore';

/**
 * Convert a Buffer to a readable stream that matches AWS SDK's SdkStream type
 */
function bufferToSdkStream(buffer: Buffer): SdkStream<Readable> {
  const stream = Readable.from(buffer) as SdkStream<Readable>;

  // Add the transformToByteArray method that AWS SDK expects
  stream.transformToByteArray = async () => new Uint8Array(buffer);

  // Add the transformToString method
  stream.transformToString = async (encoding?: string) =>
    buffer.toString((encoding as BufferEncoding) || 'utf-8');

  // Add the transformToWebStream method
  stream.transformToWebStream = () => {
    throw new Error('transformToWebStream is not supported in mock');
  };

  return stream;
}

/**
 * Create a handler for PutObject command
 */
export function createPutObjectHandler(store: S3MockStore) {
  return async (input: PutObjectCommandInput): Promise<PutObjectCommandOutput> => {
    const { Bucket, Key, Body } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!Key) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Key\' in params', 400);
    }

    // Convert Body to Buffer
    let bodyBuffer: Buffer;
    if (Body === undefined || Body === null) {
      bodyBuffer = Buffer.alloc(0);
    } else if (Buffer.isBuffer(Body)) {
      bodyBuffer = Body;
    } else if (typeof Body === 'string') {
      bodyBuffer = Buffer.from(Body, 'utf-8');
    } else if (Body instanceof Uint8Array) {
      bodyBuffer = Buffer.from(Body);
    } else if (Body instanceof Readable || (typeof Body === 'object' && Body !== null && typeof (Body as unknown as Record<string, unknown>).read === 'function')) {
      // Handle Node.js streams
      const chunks: Buffer[] = [];
      for await (const chunk of Body as AsyncIterable<Buffer | string>) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as string));
      }
      bodyBuffer = Buffer.concat(chunks);
    } else if (typeof Blob !== 'undefined' && Body instanceof Blob) {
      bodyBuffer = Buffer.from(await Body.arrayBuffer());
    } else {
      bodyBuffer = Buffer.from(String(Body), 'utf-8');
    }

    const storedObject = store.putObject(Bucket, Key, bodyBuffer, {
      contentType: input.ContentType,
      contentEncoding: input.ContentEncoding,
      contentLanguage: input.ContentLanguage,
      contentDisposition: input.ContentDisposition,
      cacheControl: input.CacheControl,
      expires: input.Expires,
      metadata: input.Metadata,
      storageClass: input.StorageClass,
      serverSideEncryption: input.ServerSideEncryption,
      sseKmsKeyId: input.SSEKMSKeyId,
      bucketKeyEnabled: input.BucketKeyEnabled,
      acl: input.ACL,
      checksumAlgorithm: input.ChecksumAlgorithm,
      checksumCRC32: input.ChecksumCRC32,
      checksumCRC32C: input.ChecksumCRC32C,
      checksumSHA1: input.ChecksumSHA1,
      checksumSHA256: input.ChecksumSHA256,
    });

    const response: PutObjectCommandOutput = {
      $metadata: createResponseMetadata(),
      ETag: storedObject.etag,
      VersionId: storedObject.versionId,
      ServerSideEncryption: storedObject.serverSideEncryption as PutObjectCommandOutput['ServerSideEncryption'],
      SSEKMSKeyId: storedObject.sseKmsKeyId,
      BucketKeyEnabled: storedObject.bucketKeyEnabled,
      ChecksumCRC32: storedObject.checksumCRC32,
      ChecksumCRC32C: storedObject.checksumCRC32C,
      ChecksumSHA1: storedObject.checksumSHA1,
      ChecksumSHA256: storedObject.checksumSHA256,
    };

    return response;
  };
}

/**
 * Create a handler for GetObject command
 */
export function createGetObjectHandler(store: S3MockStore) {
  return async (input: GetObjectCommandInput): Promise<GetObjectCommandOutput> => {
    const { Bucket, Key } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!Key) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Key\' in params', 400);
    }

    // Check if bucket exists
    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    const storedObject = store.getObject(Bucket, Key);

    if (!storedObject) {
      throw createAwsError('NoSuchKey', `The specified key does not exist: ${Key}`, 404);
    }

    // Handle conditional gets
    if (input.IfMatch && input.IfMatch !== storedObject.etag) {
      throw createAwsError('PreconditionFailed', 'At least one of the pre-conditions you specified did not hold', 412);
    }

    if (input.IfNoneMatch && input.IfNoneMatch === storedObject.etag) {
      const response: GetObjectCommandOutput = {
        $metadata: {
          ...createResponseMetadata(),
          httpStatusCode: 304,
        },
      };
      return response;
    }

    if (input.IfModifiedSince && storedObject.lastModified <= input.IfModifiedSince) {
      const response: GetObjectCommandOutput = {
        $metadata: {
          ...createResponseMetadata(),
          httpStatusCode: 304,
        },
      };
      return response;
    }

    if (input.IfUnmodifiedSince && storedObject.lastModified > input.IfUnmodifiedSince) {
      throw createAwsError('PreconditionFailed', 'At least one of the pre-conditions you specified did not hold', 412);
    }

    // Handle range requests
    let body = storedObject.body;
    let contentLength = storedObject.contentLength;
    let contentRange: string | undefined;

    if (input.Range) {
      const rangeMatch = input.Range.match(/^bytes=(\d*)-(\d*)$/);
      if (rangeMatch) {
        const start = rangeMatch[1] ? parseInt(rangeMatch[1], 10) : 0;
        const end = rangeMatch[2]
          ? Math.min(parseInt(rangeMatch[2], 10), storedObject.contentLength - 1)
          : storedObject.contentLength - 1;

        if (start <= end && start < storedObject.contentLength) {
          body = storedObject.body.slice(start, end + 1);
          contentLength = body.length;
          contentRange = `bytes ${start}-${end}/${storedObject.contentLength}`;
        }
      }
    }

    const response: GetObjectCommandOutput = {
      $metadata: createResponseMetadata(),
      Body: bufferToSdkStream(body),
      ContentLength: contentLength,
      ContentType: storedObject.contentType,
      ContentEncoding: storedObject.contentEncoding,
      ContentLanguage: storedObject.contentLanguage,
      ContentDisposition: storedObject.contentDisposition,
      CacheControl: storedObject.cacheControl,
      Expires: storedObject.expires,
      ETag: storedObject.etag,
      LastModified: storedObject.lastModified,
      Metadata: storedObject.metadata,
      StorageClass: storedObject.storageClass,
      ServerSideEncryption: storedObject.serverSideEncryption as GetObjectCommandOutput['ServerSideEncryption'],
      SSEKMSKeyId: storedObject.sseKmsKeyId,
      BucketKeyEnabled: storedObject.bucketKeyEnabled,
      VersionId: storedObject.versionId,
      ContentRange: contentRange,
      ChecksumCRC32: storedObject.checksumCRC32,
      ChecksumCRC32C: storedObject.checksumCRC32C,
      ChecksumSHA1: storedObject.checksumSHA1,
      ChecksumSHA256: storedObject.checksumSHA256,
    };

    return response;
  };
}

/**
 * Create a handler for HeadObject command
 */
export function createHeadObjectHandler(store: S3MockStore) {
  return async (input: HeadObjectCommandInput): Promise<HeadObjectCommandOutput> => {
    const { Bucket, Key } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!Key) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Key\' in params', 400);
    }

    // Check if bucket exists
    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    const storedObject = store.getObject(Bucket, Key);

    if (!storedObject) {
      throw createAwsError('NotFound', `The specified key does not exist: ${Key}`, 404);
    }

    // Handle conditional gets
    if (input.IfMatch && input.IfMatch !== storedObject.etag) {
      throw createAwsError('PreconditionFailed', 'At least one of the pre-conditions you specified did not hold', 412);
    }

    if (input.IfNoneMatch && input.IfNoneMatch === storedObject.etag) {
      const response: HeadObjectCommandOutput = {
        $metadata: {
          ...createResponseMetadata(),
          httpStatusCode: 304,
        },
      };
      return response;
    }

    if (input.IfModifiedSince && storedObject.lastModified <= input.IfModifiedSince) {
      const response: HeadObjectCommandOutput = {
        $metadata: {
          ...createResponseMetadata(),
          httpStatusCode: 304,
        },
      };
      return response;
    }

    if (input.IfUnmodifiedSince && storedObject.lastModified > input.IfUnmodifiedSince) {
      throw createAwsError('PreconditionFailed', 'At least one of the pre-conditions you specified did not hold', 412);
    }

    const response: HeadObjectCommandOutput = {
      $metadata: createResponseMetadata(),
      ContentLength: storedObject.contentLength,
      ContentType: storedObject.contentType,
      ContentEncoding: storedObject.contentEncoding,
      ContentLanguage: storedObject.contentLanguage,
      ContentDisposition: storedObject.contentDisposition,
      CacheControl: storedObject.cacheControl,
      Expires: storedObject.expires,
      ETag: storedObject.etag,
      LastModified: storedObject.lastModified,
      Metadata: storedObject.metadata,
      StorageClass: storedObject.storageClass,
      ServerSideEncryption: storedObject.serverSideEncryption as HeadObjectCommandOutput['ServerSideEncryption'],
      SSEKMSKeyId: storedObject.sseKmsKeyId,
      BucketKeyEnabled: storedObject.bucketKeyEnabled,
      VersionId: storedObject.versionId,
      ChecksumCRC32: storedObject.checksumCRC32,
      ChecksumCRC32C: storedObject.checksumCRC32C,
      ChecksumSHA1: storedObject.checksumSHA1,
      ChecksumSHA256: storedObject.checksumSHA256,
    };

    return response;
  };
}

/**
 * Create a handler for DeleteObject command
 */
export function createDeleteObjectHandler(store: S3MockStore) {
  return async (input: DeleteObjectCommandInput): Promise<DeleteObjectCommandOutput> => {
    const { Bucket, Key } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!Key) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Key\' in params', 400);
    }

    // Check if bucket exists
    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    // Get version ID before deletion if exists
    const existingObject = store.getObject(Bucket, Key);
    const versionId = existingObject?.versionId;

    // S3 DeleteObject succeeds even if object doesn't exist
    store.deleteObject(Bucket, Key);

    const response: DeleteObjectCommandOutput = {
      $metadata: createResponseMetadata(),
      DeleteMarker: false,
      VersionId: versionId,
    };

    return response;
  };
}

/**
 * Create a handler for ListObjectsV2 command
 */
export function createListObjectsV2Handler(store: S3MockStore) {
  return async (input: ListObjectsV2CommandInput): Promise<ListObjectsV2CommandOutput> => {
    const { Bucket } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    // Check if bucket exists
    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    const result = store.listObjects(Bucket, {
      prefix: input.Prefix,
      delimiter: input.Delimiter,
      startAfter: input.StartAfter,
      maxKeys: input.MaxKeys,
      continuationToken: input.ContinuationToken,
    });

    const response: ListObjectsV2CommandOutput = {
      $metadata: createResponseMetadata(),
      Name: Bucket,
      Prefix: input.Prefix || '',
      Delimiter: input.Delimiter,
      MaxKeys: input.MaxKeys || 1000,
      IsTruncated: result.isTruncated,
      Contents: result.objects.map((obj) => ({
        Key: obj.key,
        LastModified: obj.lastModified,
        ETag: obj.etag,
        Size: obj.contentLength,
        StorageClass: obj.storageClass,
        ChecksumAlgorithm: obj.checksumAlgorithm ? [obj.checksumAlgorithm] : undefined,
      })),
      CommonPrefixes: result.commonPrefixes.map((prefix) => ({ Prefix: prefix })),
      KeyCount: result.objects.length,
      ContinuationToken: input.ContinuationToken,
      NextContinuationToken: result.nextContinuationToken,
      StartAfter: input.StartAfter,
      EncodingType: input.EncodingType,
    };

    return response;
  };
}

// ==================== Bucket Management Handlers ====================

/**
 * Create a handler for CreateBucket command
 */
export function createCreateBucketHandler(store: S3MockStore) {
  return async (input: CreateBucketCommandInput): Promise<CreateBucketCommandOutput> => {
    const { Bucket } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (store.bucketExists(Bucket)) {
      throw createAwsError('BucketAlreadyExists', `The requested bucket name is not available: ${Bucket}`, 409);
    }

    const region = input.CreateBucketConfiguration?.LocationConstraint || 'us-east-1';
    store.createBucket(Bucket, region, input.ACL);

    const response: CreateBucketCommandOutput = {
      $metadata: createResponseMetadata(),
      Location: `/${Bucket}`,
    };

    return response;
  };
}

/**
 * Create a handler for DeleteBucket command
 */
export function createDeleteBucketHandler(store: S3MockStore) {
  return async (input: DeleteBucketCommandInput): Promise<DeleteBucketCommandOutput> => {
    const { Bucket } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    try {
      store.deleteBucket(Bucket);
    } catch (error) {
      throw createAwsError('BucketNotEmpty', `The bucket you tried to delete is not empty: ${Bucket}`, 409);
    }

    const response: DeleteBucketCommandOutput = {
      $metadata: createResponseMetadata(),
    };

    return response;
  };
}

/**
 * Create a handler for ListBuckets command
 */
export function createListBucketsHandler(store: S3MockStore) {
  return async (_input: ListBucketsCommandInput): Promise<ListBucketsCommandOutput> => {
    const buckets = store.listBuckets();

    const response: ListBucketsCommandOutput = {
      $metadata: createResponseMetadata(),
      Buckets: buckets.map((bucket) => ({
        Name: bucket.name,
        CreationDate: bucket.creationDate,
      })),
      Owner: {
        ID: 'default-owner-id',
        DisplayName: 'default-owner',
      },
    };

    return response;
  };
}

/**
 * Create a handler for HeadBucket command
 */
export function createHeadBucketHandler(store: S3MockStore) {
  return async (input: HeadBucketCommandInput): Promise<HeadBucketCommandOutput> => {
    const { Bucket } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NotFound', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    const response: HeadBucketCommandOutput = {
      $metadata: createResponseMetadata(),
    };

    return response;
  };
}

/**
 * Create a handler for GetBucketLocation command
 */
export function createGetBucketLocationHandler(store: S3MockStore) {
  return async (input: GetBucketLocationCommandInput): Promise<GetBucketLocationCommandOutput> => {
    const { Bucket } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    const bucket = store.getBucket(Bucket);
    if (!bucket) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    const response: GetBucketLocationCommandOutput = {
      $metadata: createResponseMetadata(),
      LocationConstraint: (bucket.region as GetBucketLocationCommandOutput['LocationConstraint']) || undefined,
    };

    return response;
  };
}

// ==================== Additional Object Operation Handlers ====================

/**
 * Create a handler for CopyObject command
 */
export function createCopyObjectHandler(store: S3MockStore) {
  return async (input: CopyObjectCommandInput): Promise<CopyObjectCommandOutput> => {
    const { Bucket, Key, CopySource } = input;

    if (!Bucket || !Key || !CopySource) {
      throw createAwsError('MissingRequiredParameter', 'Missing required parameters', 400);
    }

    // Parse CopySource (format: /bucket/key or bucket/key)
    const sourceMatch = CopySource.match(/^\/?(.*?)\/(.*)/);
    if (!sourceMatch) {
      throw createAwsError('InvalidArgument', 'Invalid CopySource format', 400);
    }

    const sourceBucket = sourceMatch[1];
    const sourceKey = decodeURIComponent(sourceMatch[2]);

    if (!store.bucketExists(sourceBucket)) {
      throw createAwsError('NoSuchBucket', `The specified source bucket does not exist: ${sourceBucket}`, 404);
    }

    if (!store.objectExists(sourceBucket, sourceKey)) {
      throw createAwsError('NoSuchKey', `The specified source key does not exist: ${sourceKey}`, 404);
    }

    const copiedObject = store.copyObject(sourceBucket, sourceKey, Bucket, Key, {
      contentType: input.ContentType,
      metadata: input.Metadata,
      cacheControl: input.CacheControl,
      contentDisposition: input.ContentDisposition,
      contentEncoding: input.ContentEncoding,
      contentLanguage: input.ContentLanguage,
      expires: input.Expires,
      storageClass: input.StorageClass,
      serverSideEncryption: input.ServerSideEncryption,
      sseKmsKeyId: input.SSEKMSKeyId,
      acl: input.ACL,
    });

    const response: CopyObjectCommandOutput = {
      $metadata: createResponseMetadata(),
      CopyObjectResult: {
        ETag: copiedObject.etag,
        LastModified: copiedObject.lastModified,
      },
    };

    return response;
  };
}

/**
 * Create a handler for DeleteObjects command
 */
export function createDeleteObjectsHandler(store: S3MockStore) {
  return async (input: DeleteObjectsCommandInput): Promise<DeleteObjectsCommandOutput> => {
    const { Bucket, Delete } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!Delete?.Objects || Delete.Objects.length === 0) {
      throw createAwsError('MalformedXML', 'The XML you provided was not well-formed', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    const deleted: Array<{ Key: string; VersionId?: string }> = [];
    const errors: Array<{ Key: string; Code: string; Message: string }> = [];

    for (const obj of Delete.Objects) {
      if (!obj.Key) continue;

      try {
        const existingObj = store.getObject(Bucket, obj.Key);
        store.deleteObject(Bucket, obj.Key);
        deleted.push({
          Key: obj.Key,
          VersionId: existingObj?.versionId,
        });
      } catch (error) {
        if (!Delete.Quiet) {
          errors.push({
            Key: obj.Key,
            Code: 'InternalError',
            Message: String(error),
          });
        }
      }
    }

    const response: DeleteObjectsCommandOutput = {
      $metadata: createResponseMetadata(),
      Deleted: Delete.Quiet ? undefined : deleted,
      Errors: errors.length > 0 ? errors : undefined,
    };

    return response;
  };
}

/**
 * Create a handler for ListObjects command (V1)
 */
export function createListObjectsHandler(store: S3MockStore) {
  return async (input: ListObjectsCommandInput): Promise<ListObjectsCommandOutput> => {
    const { Bucket } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    const result = store.listObjects(Bucket, {
      prefix: input.Prefix,
      delimiter: input.Delimiter,
      startAfter: input.Marker,
      maxKeys: input.MaxKeys,
    });

    const response: ListObjectsCommandOutput = {
      $metadata: createResponseMetadata(),
      Name: Bucket,
      Prefix: input.Prefix || '',
      Delimiter: input.Delimiter,
      MaxKeys: input.MaxKeys || 1000,
      IsTruncated: result.isTruncated,
      Contents: result.objects.map((obj) => ({
        Key: obj.key,
        LastModified: obj.lastModified,
        ETag: obj.etag,
        Size: obj.contentLength,
        StorageClass: obj.storageClass,
      })),
      CommonPrefixes: result.commonPrefixes.map((prefix) => ({ Prefix: prefix })),
      Marker: input.Marker,
      NextMarker: result.nextContinuationToken,
      EncodingType: input.EncodingType,
    };

    return response;
  };
}

/**
 * Create a handler for GetObjectAttributes command
 */
export function createGetObjectAttributesHandler(store: S3MockStore) {
  return async (input: GetObjectAttributesCommandInput): Promise<GetObjectAttributesCommandOutput> => {
    const { Bucket, Key } = input;

    if (!Bucket || !Key) {
      throw createAwsError('MissingRequiredParameter', 'Missing required parameters', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    const object = store.getObject(Bucket, Key);
    if (!object) {
      throw createAwsError('NoSuchKey', `The specified key does not exist: ${Key}`, 404);
    }

    const response: GetObjectAttributesCommandOutput = {
      $metadata: createResponseMetadata(),
      ETag: object.etag,
      LastModified: object.lastModified,
      StorageClass: object.storageClass,
      ObjectSize: object.contentLength,
      VersionId: object.versionId,
      Checksum: {
        ChecksumCRC32: object.checksumCRC32,
        ChecksumCRC32C: object.checksumCRC32C,
        ChecksumSHA1: object.checksumSHA1,
        ChecksumSHA256: object.checksumSHA256,
      },
    };

    return response;
  };
}

// ==================== Object Tagging Handlers ====================

export function createGetObjectTaggingHandler(store: S3MockStore) {
  return async (input: GetObjectTaggingCommandInput): Promise<GetObjectTaggingCommandOutput> => {
    const { Bucket, Key } = input;

    if (!Bucket || !Key) {
      throw createAwsError('MissingRequiredParameter', 'Missing required parameters', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    const object = store.getObject(Bucket, Key);
    if (!object) {
      throw createAwsError('NoSuchKey', `The specified key does not exist: ${Key}`, 404);
    }

    const response: GetObjectTaggingCommandOutput = {
      $metadata: createResponseMetadata(),
      TagSet: object.tags || [],
      VersionId: object.versionId,
    };

    return response;
  };
}

export function createPutObjectTaggingHandler(store: S3MockStore) {
  return async (input: PutObjectTaggingCommandInput): Promise<PutObjectTaggingCommandOutput> => {
    const { Bucket, Key, Tagging } = input;

    if (!Bucket || !Key || !Tagging) {
      throw createAwsError('MissingRequiredParameter', 'Missing required parameters', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    const object = store.getObject(Bucket, Key);
    if (!object) {
      throw createAwsError('NoSuchKey', `The specified key does not exist: ${Key}`, 404);
    }

    store.setObjectTags(Bucket, Key, Tagging.TagSet || []);

    const response: PutObjectTaggingCommandOutput = {
      $metadata: createResponseMetadata(),
      VersionId: object.versionId,
    };

    return response;
  };
}

export function createDeleteObjectTaggingHandler(store: S3MockStore) {
  return async (input: DeleteObjectTaggingCommandInput): Promise<DeleteObjectTaggingCommandOutput> => {
    const { Bucket, Key } = input;

    if (!Bucket || !Key) {
      throw createAwsError('MissingRequiredParameter', 'Missing required parameters', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    const object = store.getObject(Bucket, Key);
    if (!object) {
      throw createAwsError('NoSuchKey', `The specified key does not exist: ${Key}`, 404);
    }

    store.deleteObjectTags(Bucket, Key);

    const response: DeleteObjectTaggingCommandOutput = {
      $metadata: createResponseMetadata(),
      VersionId: object.versionId,
    };

    return response;
  };
}

// ==================== Object ACL Handlers ====================

export function createGetObjectAclHandler(store: S3MockStore) {
  return async (input: GetObjectAclCommandInput): Promise<GetObjectAclCommandOutput> => {
    const { Bucket, Key } = input;

    if (!Bucket || !Key) {
      throw createAwsError('MissingRequiredParameter', 'Missing required parameters', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    const object = store.getObject(Bucket, Key);
    if (!object) {
      throw createAwsError('NoSuchKey', `The specified key does not exist: ${Key}`, 404);
    }

    const response: GetObjectAclCommandOutput = {
      $metadata: createResponseMetadata(),
      Owner: {
        ID: 'default-owner-id',
        DisplayName: 'default-owner',
      },
      Grants: [
        {
          Grantee: {
            Type: 'CanonicalUser',
            ID: 'default-owner-id',
            DisplayName: 'default-owner',
          },
          Permission: 'FULL_CONTROL',
        },
      ],
    };

    return response;
  };
}

export function createPutObjectAclHandler(store: S3MockStore) {
  return async (input: PutObjectAclCommandInput): Promise<PutObjectAclCommandOutput> => {
    const { Bucket, Key } = input;

    if (!Bucket || !Key) {
      throw createAwsError('MissingRequiredParameter', 'Missing required parameters', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    const object = store.getObject(Bucket, Key);
    if (!object) {
      throw createAwsError('NoSuchKey', `The specified key does not exist: ${Key}`, 404);
    }

    const response: PutObjectAclCommandOutput = {
      $metadata: createResponseMetadata(),
    };

    return response;
  };
}

// ==================== Multipart Upload Handlers ====================

export function createCreateMultipartUploadHandler(store: S3MockStore) {
  return async (input: CreateMultipartUploadCommandInput): Promise<CreateMultipartUploadCommandOutput> => {
    const { Bucket, Key } = input;

    if (!Bucket || !Key) {
      throw createAwsError('MissingRequiredParameter', 'Missing required parameters', 400);
    }

    const upload = store.createMultipartUpload(Bucket, Key, {
      metadata: input.Metadata,
      contentType: input.ContentType,
      storageClass: input.StorageClass,
      serverSideEncryption: input.ServerSideEncryption,
      sseKmsKeyId: input.SSEKMSKeyId,
      acl: input.ACL,
    });

    const response: CreateMultipartUploadCommandOutput = {
      $metadata: createResponseMetadata(),
      Bucket,
      Key,
      UploadId: upload.uploadId,
      ServerSideEncryption: upload.serverSideEncryption as CreateMultipartUploadCommandOutput['ServerSideEncryption'],
      SSEKMSKeyId: upload.sseKmsKeyId,
    };

    return response;
  };
}

export function createUploadPartHandler(store: S3MockStore) {
  return async (input: UploadPartCommandInput): Promise<UploadPartCommandOutput> => {
    const { Bucket, Key, UploadId, PartNumber, Body } = input;

    if (!Bucket || !Key || !UploadId || PartNumber === undefined) {
      throw createAwsError('MissingRequiredParameter', 'Missing required parameters', 400);
    }

    const upload = store.getMultipartUpload(UploadId);
    if (!upload) {
      throw createAwsError('NoSuchUpload', `The specified multipart upload does not exist: ${UploadId}`, 404);
    }

    // Convert Body to Buffer
    let bodyBuffer: Buffer;
    if (Body === undefined || Body === null) {
      bodyBuffer = Buffer.alloc(0);
    } else if (Buffer.isBuffer(Body)) {
      bodyBuffer = Body;
    } else if (typeof Body === 'string') {
      bodyBuffer = Buffer.from(Body, 'utf-8');
    } else if (Body instanceof Uint8Array) {
      bodyBuffer = Buffer.from(Body);
    } else if (Body instanceof Readable || (typeof Body === 'object' && Body !== null && typeof (Body as any).read === 'function')) {
      const chunks: Buffer[] = [];
      for await (const chunk of Body as AsyncIterable<Buffer | string>) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as string));
      }
      bodyBuffer = Buffer.concat(chunks);
    } else if (typeof Blob !== 'undefined' && Body instanceof Blob) {
      bodyBuffer = Buffer.from(await Body.arrayBuffer());
    } else {
      bodyBuffer = Buffer.from(String(Body), 'utf-8');
    }

    const part = store.uploadPart(UploadId, PartNumber, bodyBuffer);

    const response: UploadPartCommandOutput = {
      $metadata: createResponseMetadata(),
      ETag: part.etag,
      ServerSideEncryption: upload.serverSideEncryption as UploadPartCommandOutput['ServerSideEncryption'],
      SSEKMSKeyId: upload.sseKmsKeyId,
    };

    return response;
  };
}

export function createUploadPartCopyHandler(store: S3MockStore) {
  return async (input: UploadPartCopyCommandInput): Promise<UploadPartCopyCommandOutput> => {
    const { Bucket, Key, UploadId, PartNumber, CopySource } = input;

    if (!Bucket || !Key || !UploadId || PartNumber === undefined || !CopySource) {
      throw createAwsError('MissingRequiredParameter', 'Missing required parameters', 400);
    }

    const upload = store.getMultipartUpload(UploadId);
    if (!upload) {
      throw createAwsError('NoSuchUpload', `The specified multipart upload does not exist: ${UploadId}`, 404);
    }

    const sourceMatch = CopySource.match(/^\/?(.*?)\/(.*)/);
    if (!sourceMatch) {
      throw createAwsError('InvalidArgument', 'Invalid CopySource format', 400);
    }

    const sourceBucket = sourceMatch[1];
    const sourceKey = decodeURIComponent(sourceMatch[2]);

    const sourceObject = store.getObject(sourceBucket, sourceKey);
    if (!sourceObject) {
      throw createAwsError('NoSuchKey', `The specified source key does not exist: ${sourceKey}`, 404);
    }

    let data = sourceObject.body;
    if (input.CopySourceRange) {
      const rangeMatch = input.CopySourceRange.match(/^bytes=(\d+)-(\d+)$/);
      if (rangeMatch) {
        const start = parseInt(rangeMatch[1], 10);
        const end = parseInt(rangeMatch[2], 10);
        data = sourceObject.body.slice(start, end + 1);
      }
    }

    const part = store.uploadPart(UploadId, PartNumber, data);

    const response: UploadPartCopyCommandOutput = {
      $metadata: createResponseMetadata(),
      CopyPartResult: {
        ETag: part.etag,
        LastModified: part.lastModified,
      },
    };

    return response;
  };
}

export function createCompleteMultipartUploadHandler(store: S3MockStore) {
  return async (input: CompleteMultipartUploadCommandInput): Promise<CompleteMultipartUploadCommandOutput> => {
    const { Bucket, Key, UploadId, MultipartUpload } = input;

    if (!Bucket || !Key || !UploadId || !MultipartUpload?.Parts) {
      throw createAwsError('MissingRequiredParameter', 'Missing required parameters', 400);
    }

    const upload = store.getMultipartUpload(UploadId);
    if (!upload) {
      throw createAwsError('NoSuchUpload', `The specified multipart upload does not exist: ${UploadId}`, 404);
    }

    const parts = MultipartUpload.Parts.map((p) => ({
      PartNumber: p.PartNumber!,
      ETag: p.ETag!,
    }));

    const object = store.completeMultipartUpload(UploadId, parts);

    const response: CompleteMultipartUploadCommandOutput = {
      $metadata: createResponseMetadata(),
      Location: `https://${Bucket}.s3.amazonaws.com/${Key}`,
      Bucket,
      Key,
      ETag: object.etag,
      ServerSideEncryption: object.serverSideEncryption as CompleteMultipartUploadCommandOutput['ServerSideEncryption'],
      SSEKMSKeyId: object.sseKmsKeyId,
      VersionId: object.versionId,
    };

    return response;
  };
}

export function createAbortMultipartUploadHandler(store: S3MockStore) {
  return async (input: AbortMultipartUploadCommandInput): Promise<AbortMultipartUploadCommandOutput> => {
    const { Bucket, Key, UploadId } = input;

    if (!Bucket || !Key || !UploadId) {
      throw createAwsError('MissingRequiredParameter', 'Missing required parameters', 400);
    }

    const upload = store.getMultipartUpload(UploadId);
    if (!upload) {
      throw createAwsError('NoSuchUpload', `The specified multipart upload does not exist: ${UploadId}`, 404);
    }

    store.abortMultipartUpload(UploadId);

    const response: AbortMultipartUploadCommandOutput = {
      $metadata: createResponseMetadata(),
    };

    return response;
  };
}

export function createListMultipartUploadsHandler(store: S3MockStore) {
  return async (input: ListMultipartUploadsCommandInput): Promise<ListMultipartUploadsCommandOutput> => {
    const { Bucket } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    const uploads = store.listMultipartUploads(Bucket, {
      prefix: input.Prefix,
      maxUploads: input.MaxUploads,
    });

    const response: ListMultipartUploadsCommandOutput = {
      $metadata: createResponseMetadata(),
      Bucket,
      Uploads: uploads.map((upload) => ({
        UploadId: upload.uploadId,
        Key: upload.key,
        Initiated: upload.initiated,
        StorageClass: upload.storageClass,
        Owner: {
          ID: 'default-owner-id',
          DisplayName: 'default-owner',
        },
      })),
      Prefix: input.Prefix,
      MaxUploads: input.MaxUploads || 1000,
      IsTruncated: false,
    };

    return response;
  };
}

export function createListPartsHandler(store: S3MockStore) {
  return async (input: ListPartsCommandInput): Promise<ListPartsCommandOutput> => {
    const { Bucket, Key, UploadId } = input;

    if (!Bucket || !Key || !UploadId) {
      throw createAwsError('MissingRequiredParameter', 'Missing required parameters', 400);
    }

    const upload = store.getMultipartUpload(UploadId);
    if (!upload) {
      throw createAwsError('NoSuchUpload', `The specified multipart upload does not exist: ${UploadId}`, 404);
    }

    const parts = store.listParts(UploadId, {
      maxParts: input.MaxParts,
      partNumberMarker: input.PartNumberMarker ? parseInt(input.PartNumberMarker, 10) : undefined,
    });

    const response: ListPartsCommandOutput = {
      $metadata: createResponseMetadata(),
      Bucket,
      Key,
      UploadId,
      StorageClass: upload.storageClass,
      PartNumberMarker: input.PartNumberMarker,
      MaxParts: input.MaxParts || 1000,
      IsTruncated: false,
      Parts: parts.map((part) => ({
        PartNumber: part.partNumber,
        ETag: part.etag,
        Size: part.size,
        LastModified: part.lastModified,
      })),
      Owner: {
        ID: 'default-owner-id',
        DisplayName: 'default-owner',
      },
    };

    return response;
  };
}

// ==================== Bucket Configuration Handlers ====================

export function createGetBucketVersioningHandler(store: S3MockStore) {
  return async (input: GetBucketVersioningCommandInput): Promise<GetBucketVersioningCommandOutput> => {
    const { Bucket } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    const config = store.getBucketConfiguration(Bucket);

    const response: GetBucketVersioningCommandOutput = {
      $metadata: createResponseMetadata(),
      Status: config.versioning?.Status,
      MFADelete: config.versioning?.MFADelete,
    };

    return response;
  };
}

export function createPutBucketVersioningHandler(store: S3MockStore) {
  return async (input: PutBucketVersioningCommandInput): Promise<PutBucketVersioningCommandOutput> => {
    const { Bucket, VersioningConfiguration } = input;

    if (!Bucket || !VersioningConfiguration) {
      throw createAwsError('MissingRequiredParameter', 'Missing required parameters', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    store.updateBucketConfiguration(Bucket, {
      versioning: VersioningConfiguration,
    });

    const response: PutBucketVersioningCommandOutput = {
      $metadata: createResponseMetadata(),
    };

    return response;
  };
}

export function createGetBucketCorsHandler(store: S3MockStore) {
  return async (input: GetBucketCorsCommandInput): Promise<GetBucketCorsCommandOutput> => {
    const { Bucket } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    const config = store.getBucketConfiguration(Bucket);

    if (!config.cors) {
      throw createAwsError('NoSuchCORSConfiguration', 'The CORS configuration does not exist', 404);
    }

    const response: GetBucketCorsCommandOutput = {
      $metadata: createResponseMetadata(),
      CORSRules: config.cors,
    };

    return response;
  };
}

export function createPutBucketCorsHandler(store: S3MockStore) {
  return async (input: PutBucketCorsCommandInput): Promise<PutBucketCorsCommandOutput> => {
    const { Bucket, CORSConfiguration } = input;

    if (!Bucket || !CORSConfiguration) {
      throw createAwsError('MissingRequiredParameter', 'Missing required parameters', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    store.updateBucketConfiguration(Bucket, {
      cors: CORSConfiguration.CORSRules,
    });

    const response: PutBucketCorsCommandOutput = {
      $metadata: createResponseMetadata(),
    };

    return response;
  };
}

export function createDeleteBucketCorsHandler(store: S3MockStore) {
  return async (input: DeleteBucketCorsCommandInput): Promise<DeleteBucketCorsCommandOutput> => {
    const { Bucket } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    store.updateBucketConfiguration(Bucket, {
      cors: undefined,
    });

    const response: DeleteBucketCorsCommandOutput = {
      $metadata: createResponseMetadata(),
    };

    return response;
  };
}

export function createGetBucketPolicyHandler(store: S3MockStore) {
  return async (input: GetBucketPolicyCommandInput): Promise<GetBucketPolicyCommandOutput> => {
    const { Bucket } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    const config = store.getBucketConfiguration(Bucket);

    if (!config.policy) {
      throw createAwsError('NoSuchBucketPolicy', 'The bucket policy does not exist', 404);
    }

    const response: GetBucketPolicyCommandOutput = {
      $metadata: createResponseMetadata(),
      Policy: config.policy,
    };

    return response;
  };
}

export function createPutBucketPolicyHandler(store: S3MockStore) {
  return async (input: PutBucketPolicyCommandInput): Promise<PutBucketPolicyCommandOutput> => {
    const { Bucket, Policy } = input;

    if (!Bucket || !Policy) {
      throw createAwsError('MissingRequiredParameter', 'Missing required parameters', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    store.updateBucketConfiguration(Bucket, {
      policy: Policy,
    });

    const response: PutBucketPolicyCommandOutput = {
      $metadata: createResponseMetadata(),
    };

    return response;
  };
}

export function createDeleteBucketPolicyHandler(store: S3MockStore) {
  return async (input: DeleteBucketPolicyCommandInput): Promise<DeleteBucketPolicyCommandOutput> => {
    const { Bucket } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    store.updateBucketConfiguration(Bucket, {
      policy: undefined,
    });

    const response: DeleteBucketPolicyCommandOutput = {
      $metadata: createResponseMetadata(),
    };

    return response;
  };
}

export function createGetBucketAclHandler(store: S3MockStore) {
  return async (input: GetBucketAclCommandInput): Promise<GetBucketAclCommandOutput> => {
    const { Bucket } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    const config = store.getBucketConfiguration(Bucket);

    const response: GetBucketAclCommandOutput = {
      $metadata: createResponseMetadata(),
      Owner: config.acl?.Owner || {
        ID: 'default-owner-id',
        DisplayName: 'default-owner',
      },
      Grants: (config.acl?.Grants || [
        {
          Grantee: {
            Type: 'CanonicalUser',
            ID: 'default-owner-id',
            DisplayName: 'default-owner',
          },
          Permission: 'FULL_CONTROL',
        },
      ]) as GetBucketAclCommandOutput['Grants'],
    };

    return response;
  };
}

export function createPutBucketAclHandler(store: S3MockStore) {
  return async (input: PutBucketAclCommandInput): Promise<PutBucketAclCommandOutput> => {
    const { Bucket } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    if (input.AccessControlPolicy) {
      store.updateBucketConfiguration(Bucket, {
        acl: input.AccessControlPolicy as any,
      });
    }

    const response: PutBucketAclCommandOutput = {
      $metadata: createResponseMetadata(),
    };

    return response;
  };
}

export function createGetBucketEncryptionHandler(store: S3MockStore) {
  return async (input: GetBucketEncryptionCommandInput): Promise<GetBucketEncryptionCommandOutput> => {
    const { Bucket } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    const config = store.getBucketConfiguration(Bucket);

    if (!config.encryption) {
      throw createAwsError(
        'ServerSideEncryptionConfigurationNotFoundError',
        'The server side encryption configuration was not found',
        404
      );
    }

    const response: GetBucketEncryptionCommandOutput = {
      $metadata: createResponseMetadata(),
      ServerSideEncryptionConfiguration: config.encryption as GetBucketEncryptionCommandOutput['ServerSideEncryptionConfiguration'],
    };

    return response;
  };
}

export function createPutBucketEncryptionHandler(store: S3MockStore) {
  return async (input: PutBucketEncryptionCommandInput): Promise<PutBucketEncryptionCommandOutput> => {
    const { Bucket, ServerSideEncryptionConfiguration } = input;

    if (!Bucket || !ServerSideEncryptionConfiguration) {
      throw createAwsError('MissingRequiredParameter', 'Missing required parameters', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    store.updateBucketConfiguration(Bucket, {
      encryption: ServerSideEncryptionConfiguration as any,
    });

    const response: PutBucketEncryptionCommandOutput = {
      $metadata: createResponseMetadata(),
    };

    return response;
  };
}

export function createDeleteBucketEncryptionHandler(store: S3MockStore) {
  return async (input: DeleteBucketEncryptionCommandInput): Promise<DeleteBucketEncryptionCommandOutput> => {
    const { Bucket } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    store.updateBucketConfiguration(Bucket, {
      encryption: undefined,
    });

    const response: DeleteBucketEncryptionCommandOutput = {
      $metadata: createResponseMetadata(),
    };

    return response;
  };
}

export function createGetBucketLifecycleConfigurationHandler(store: S3MockStore) {
  return async (input: GetBucketLifecycleConfigurationCommandInput): Promise<GetBucketLifecycleConfigurationCommandOutput> => {
    const { Bucket } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    const config = store.getBucketConfiguration(Bucket);

    if (!config.lifecycle) {
      throw createAwsError('NoSuchLifecycleConfiguration', 'The lifecycle configuration does not exist', 404);
    }

    const response: GetBucketLifecycleConfigurationCommandOutput = {
      $metadata: createResponseMetadata(),
      Rules: config.lifecycle,
    };

    return response;
  };
}

export function createPutBucketLifecycleConfigurationHandler(store: S3MockStore) {
  return async (input: PutBucketLifecycleConfigurationCommandInput): Promise<PutBucketLifecycleConfigurationCommandOutput> => {
    const { Bucket, LifecycleConfiguration } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    store.updateBucketConfiguration(Bucket, {
      lifecycle: LifecycleConfiguration?.Rules,
    });

    const response: PutBucketLifecycleConfigurationCommandOutput = {
      $metadata: createResponseMetadata(),
    };

    return response;
  };
}

export function createDeleteBucketLifecycleHandler(store: S3MockStore) {
  return async (input: DeleteBucketLifecycleCommandInput): Promise<DeleteBucketLifecycleCommandOutput> => {
    const { Bucket } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    store.updateBucketConfiguration(Bucket, {
      lifecycle: undefined,
    });

    const response: DeleteBucketLifecycleCommandOutput = {
      $metadata: createResponseMetadata(),
    };

    return response;
  };
}

export function createGetBucketWebsiteHandler(store: S3MockStore) {
  return async (input: GetBucketWebsiteCommandInput): Promise<GetBucketWebsiteCommandOutput> => {
    const { Bucket } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    const config = store.getBucketConfiguration(Bucket);

    if (!config.website) {
      throw createAwsError('NoSuchWebsiteConfiguration', 'The specified bucket does not have a website configuration', 404);
    }

    const response: GetBucketWebsiteCommandOutput = {
      $metadata: createResponseMetadata(),
      ...(config.website as Omit<GetBucketWebsiteCommandOutput, '$metadata'>),
    };

    return response;
  };
}

export function createPutBucketWebsiteHandler(store: S3MockStore) {
  return async (input: PutBucketWebsiteCommandInput): Promise<PutBucketWebsiteCommandOutput> => {
    const { Bucket, WebsiteConfiguration } = input;

    if (!Bucket || !WebsiteConfiguration) {
      throw createAwsError('MissingRequiredParameter', 'Missing required parameters', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    store.updateBucketConfiguration(Bucket, {
      website: WebsiteConfiguration as any,
    });

    const response: PutBucketWebsiteCommandOutput = {
      $metadata: createResponseMetadata(),
    };

    return response;
  };
}

export function createDeleteBucketWebsiteHandler(store: S3MockStore) {
  return async (input: DeleteBucketWebsiteCommandInput): Promise<DeleteBucketWebsiteCommandOutput> => {
    const { Bucket } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    store.updateBucketConfiguration(Bucket, {
      website: undefined,
    });

    const response: DeleteBucketWebsiteCommandOutput = {
      $metadata: createResponseMetadata(),
    };

    return response;
  };
}

export function createGetBucketTaggingHandler(store: S3MockStore) {
  return async (input: GetBucketTaggingCommandInput): Promise<GetBucketTaggingCommandOutput> => {
    const { Bucket } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    const config = store.getBucketConfiguration(Bucket);

    if (!config.tags) {
      throw createAwsError('NoSuchTagSet', 'The TagSet does not exist', 404);
    }

    const response: GetBucketTaggingCommandOutput = {
      $metadata: createResponseMetadata(),
      TagSet: config.tags,
    };

    return response;
  };
}

export function createPutBucketTaggingHandler(store: S3MockStore) {
  return async (input: PutBucketTaggingCommandInput): Promise<PutBucketTaggingCommandOutput> => {
    const { Bucket, Tagging } = input;

    if (!Bucket || !Tagging) {
      throw createAwsError('MissingRequiredParameter', 'Missing required parameters', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    store.updateBucketConfiguration(Bucket, {
      tags: Tagging.TagSet,
    });

    const response: PutBucketTaggingCommandOutput = {
      $metadata: createResponseMetadata(),
    };

    return response;
  };
}

export function createDeleteBucketTaggingHandler(store: S3MockStore) {
  return async (input: DeleteBucketTaggingCommandInput): Promise<DeleteBucketTaggingCommandOutput> => {
    const { Bucket } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    store.updateBucketConfiguration(Bucket, {
      tags: undefined,
    });

    const response: DeleteBucketTaggingCommandOutput = {
      $metadata: createResponseMetadata(),
    };

    return response;
  };
}

export function createGetBucketLoggingHandler(store: S3MockStore) {
  return async (input: GetBucketLoggingCommandInput): Promise<GetBucketLoggingCommandOutput> => {
    const { Bucket } = input;

    if (!Bucket) {
      throw createAwsError('MissingRequiredParameter', 'Missing required key \'Bucket\' in params', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    const config = store.getBucketConfiguration(Bucket);

    const response: GetBucketLoggingCommandOutput = {
      $metadata: createResponseMetadata(),
      LoggingEnabled: config.logging?.LoggingEnabled,
    };

    return response;
  };
}

export function createPutBucketLoggingHandler(store: S3MockStore) {
  return async (input: PutBucketLoggingCommandInput): Promise<PutBucketLoggingCommandOutput> => {
    const { Bucket, BucketLoggingStatus } = input;

    if (!Bucket || !BucketLoggingStatus) {
      throw createAwsError('MissingRequiredParameter', 'Missing required parameters', 400);
    }

    if (!store.bucketExists(Bucket)) {
      throw createAwsError('NoSuchBucket', `The specified bucket does not exist: ${Bucket}`, 404);
    }

    store.updateBucketConfiguration(Bucket, {
      logging: BucketLoggingStatus,
    });

    const response: PutBucketLoggingCommandOutput = {
      $metadata: createResponseMetadata(),
    };

    return response;
  };
}
