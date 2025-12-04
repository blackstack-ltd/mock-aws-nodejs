import type { MetadataBearer } from '@aws-sdk/types';

/**
 * Represents an AWS SDK V3 Command
 */
export interface AwsCommand<Input extends object, Output extends MetadataBearer> {
  input: Input;
  readonly middlewareStack: unknown;
}

/**
 * Constructor type for AWS SDK V3 Commands
 */
export interface AwsCommandConstructor<
  Input extends object = object,
  Output extends MetadataBearer = MetadataBearer
> {
  new (input: Input): AwsCommand<Input, Output>;
}

/**
 * Command handler function type
 */
export type CommandHandler<
  Input extends object = object,
  Output extends MetadataBearer = MetadataBearer
> = (input: Input) => Promise<Output> | Output;

/**
 * Configuration for a mocked command behavior
 */
export interface MockedCommandBehavior<Output extends MetadataBearer = MetadataBearer> {
  /** Return a specific response */
  response?: Output;
  /** Return a response based on call count */
  responses?: Output[];
  /** Throw an error */
  error?: Error;
  /** Custom handler function */
  handler?: CommandHandler;
  /** Number of times this behavior has been called */
  callCount: number;
}

/**
 * Record of a command call for verification
 */
export interface CommandCall<Input extends object = object> {
  commandName: string;
  input: Input;
  timestamp: Date;
}

/**
 * Base interface for service-specific mock stores
 */
export interface MockStore {
  reset(): void;
}

/**
 * AWS Error shape for SDK V3 compatibility
 */
export interface AwsError extends Error {
  name: string;
  $fault: 'client' | 'server';
  $metadata: {
    httpStatusCode: number;
    requestId?: string;
    attempts?: number;
    totalRetryDelay?: number;
  };
  Code?: string;
}

/**
 * Creates an AWS-compatible error
 */
export function createAwsError(
  name: string,
  message: string,
  httpStatusCode: number,
  fault: 'client' | 'server' = 'client'
): AwsError {
  const error = new Error(message) as AwsError;
  error.name = name;
  error.$fault = fault;
  error.$metadata = {
    httpStatusCode,
    requestId: `mock-request-${Date.now()}`,
    attempts: 1,
    totalRetryDelay: 0,
  };
  error.Code = name;
  return error;
}

/**
 * Default metadata for successful responses
 */
export function createResponseMetadata(): MetadataBearer['$metadata'] {
  return {
    httpStatusCode: 200,
    requestId: `mock-request-${Date.now()}`,
    attempts: 1,
    totalRetryDelay: 0,
  };
}
