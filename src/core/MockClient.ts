import type { MetadataBearer } from '@aws-sdk/types';
import type {
  AwsCommand,
  AwsCommandConstructor,
  CommandHandler,
  CommandCall,
  MockedCommandBehavior,
} from './types';

/**
 * Generic mock client that can intercept AWS SDK V3 client calls.
 * This is the base class for service-specific mock implementations.
 *
 * @example
 * ```typescript
 * const mockClient = new MockClient(S3Client);
 * mockClient.on(GetObjectCommand).resolves({ Body: ... });
 * ```
 */
export class MockClient<Client extends object> {
  private originalSend: Function | null = null;
  private clientInstance: Client | null = null;
  private commandHandlers: Map<string, MockedCommandBehavior> = new Map();
  private defaultHandlers: Map<string, CommandHandler> = new Map();
  private calls: CommandCall[] = [];
  private isInstalled = false;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  constructor(private readonly ClientClass: new (...args: any[]) => Client) {}

  /**
   * Install the mock on a specific client instance
   */
  install(client: Client): this {
    if (this.isInstalled) {
      throw new Error('Mock is already installed. Call restore() first.');
    }

    this.clientInstance = client;
    this.originalSend = (client as Record<string, unknown>).send as Function;

    (client as Record<string, unknown>).send = async (
      command: AwsCommand<object, MetadataBearer>
    ) => {
      return this.handleCommand(command);
    };

    this.isInstalled = true;
    return this;
  }

  /**
   * Restore the original client behavior
   */
  restore(): this {
    if (this.clientInstance && this.originalSend) {
      (this.clientInstance as Record<string, unknown>).send = this.originalSend;
    }
    this.clientInstance = null;
    this.originalSend = null;
    this.isInstalled = false;
    return this;
  }

  /**
   * Reset all mocked behaviors and call history
   */
  reset(): this {
    this.commandHandlers.clear();
    this.calls = [];
    return this;
  }

  /**
   * Register a default handler for a command type.
   * Default handlers are used when no specific mock behavior is set.
   */
  registerDefaultHandler<Input extends object, Output extends MetadataBearer>(
    CommandClass: AwsCommandConstructor<Input, Output>,
    handler: CommandHandler<Input, Output>
  ): this {
    this.defaultHandlers.set(CommandClass.name, handler as unknown as CommandHandler);
    return this;
  }

  /**
   * Set up mock behavior for a specific command
   */
  on<Input extends object, Output extends MetadataBearer>(
    CommandClass: AwsCommandConstructor<Input, Output>
  ): CommandBehaviorBuilder<Input, Output, this> {
    return new CommandBehaviorBuilder(this, CommandClass.name);
  }

  /**
   * Get all calls made to a specific command
   */
  commandCalls<Input extends object>(
    CommandClass: AwsCommandConstructor<Input, MetadataBearer>
  ): CommandCall<Input>[] {
    return this.calls.filter(
      (call) => call.commandName === CommandClass.name
    ) as CommandCall<Input>[];
  }

  /**
   * Get all recorded calls
   */
  allCalls(): CommandCall[] {
    return [...this.calls];
  }

  /**
   * Internal: Set a command behavior
   */
  _setBehavior(commandName: string, behavior: MockedCommandBehavior): void {
    this.commandHandlers.set(commandName, behavior);
  }

  /**
   * Internal: Get a command behavior
   */
  _getBehavior(commandName: string): MockedCommandBehavior | undefined {
    return this.commandHandlers.get(commandName);
  }

  /**
   * Internal: Handle a command execution
   */
  private async handleCommand(
    command: AwsCommand<object, MetadataBearer>
  ): Promise<MetadataBearer> {
    const commandName = command.constructor.name;
    const input = command.input;

    // Record the call
    this.calls.push({
      commandName,
      input,
      timestamp: new Date(),
    });

    // Check for specific mock behavior
    const behavior = this.commandHandlers.get(commandName);
    if (behavior) {
      behavior.callCount++;

      if (behavior.error) {
        throw behavior.error;
      }

      if (behavior.handler) {
        return behavior.handler(input);
      }

      if (behavior.responses && behavior.responses.length > 0) {
        const index = Math.min(behavior.callCount - 1, behavior.responses.length - 1);
        return behavior.responses[index];
      }

      if (behavior.response) {
        return behavior.response;
      }
    }

    // Check for default handler
    const defaultHandler = this.defaultHandlers.get(commandName);
    if (defaultHandler) {
      return defaultHandler(input);
    }

    // No handler found
    throw new Error(
      `No mock behavior configured for ${commandName}. ` +
      `Either configure a mock with mockClient.on(${commandName}).resolves(...) ` +
      `or register a default handler.`
    );
  }
}

/**
 * Builder class for configuring command mock behaviors
 */
export class CommandBehaviorBuilder<
  Input extends object,
  Output extends MetadataBearer,
  MockClientType extends MockClient<object>
> {
  constructor(
    private readonly mockClient: MockClientType,
    private readonly commandName: string
  ) {}

  /**
   * Configure the mock to resolve with a specific response
   */
  resolves(response: Output): MockClientType {
    this.mockClient._setBehavior(this.commandName, {
      response,
      callCount: 0,
    });
    return this.mockClient;
  }

  /**
   * Configure the mock to resolve with different responses on subsequent calls
   */
  resolvesOnce(response: Output): CommandBehaviorBuilder<Input, Output, MockClientType> {
    const existing = this.mockClient._getBehavior(this.commandName);
    const responses = existing?.responses || [];
    responses.push(response);

    this.mockClient._setBehavior(this.commandName, {
      responses,
      callCount: existing?.callCount || 0,
    });
    return this;
  }

  /**
   * Configure the mock to reject with an error
   */
  rejects(error: Error): MockClientType {
    this.mockClient._setBehavior(this.commandName, {
      error,
      callCount: 0,
    });
    return this.mockClient;
  }

  /**
   * Configure a custom handler function
   */
  callsFake(handler: (input: Input) => Promise<Output> | Output): MockClientType {
    this.mockClient._setBehavior(this.commandName, {
      handler: handler as unknown as CommandHandler,
      callCount: 0,
    });
    return this.mockClient;
  }
}
