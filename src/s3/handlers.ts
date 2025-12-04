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
      tagging: input.Tagging,
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
