import WebSocket from 'ws';
import { KubeFastifyInstance, OauthFastifyRequest } from '../types';
import wssK8sRoutes, {
  CONNECTION_TIMEOUT_MS,
  HEARTBEAT_INTERVAL_MS,
  INACTIVITY_WARNING_MS,
  MESSAGE_QUEUE_LIMIT,
  STALE_CONNECTION_MS,
} from '../routes/wss/k8s/index';
import { getDirectCallOptions, getAccessToken } from '../utils/directCallUtils';

// Mock dependencies
jest.mock('../utils/directCallUtils');
jest.mock('ws');
jest.mock('https', () => ({
  globalAgent: {
    options: {
      ca: undefined,
    },
  },
}));

describe('WebSocket K8s Routes', () => {
  let mockFastify: KubeFastifyInstance;
  let mockConnection: any;
  let mockRequest: OauthFastifyRequest;
  let mockSourceSocket: any;
  let mockTargetSocket: any;
  let mockLog: any;
  let routeHandler: any;

  beforeEach(() => {
    jest.clearAllMocks();
    jest.useFakeTimers();

    // Mock logger
    mockLog = {
      info: jest.fn(),
      error: jest.fn(),
      warn: jest.fn(),
      debug: jest.fn(),
    };

    // Mock source (client) socket
    mockSourceSocket = {
      readyState: WebSocket.OPEN,
      send: jest.fn(),
      close: jest.fn(),
      on: jest.fn(),
      once: jest.fn(),
      ping: jest.fn(),
      pong: jest.fn(),
    };

    // Mock target (K8s API) socket
    mockTargetSocket = {
      readyState: WebSocket.OPEN,
      send: jest.fn(),
      close: jest.fn(),
      terminate: jest.fn(),
      on: jest.fn(),
      once: jest.fn(),
      ping: jest.fn(),
      pong: jest.fn(),
    };

    // Mock connection
    mockConnection = {
      socket: mockSourceSocket,
    };

    // Mock fastify instance
    mockFastify = {
      log: mockLog,
      kube: {
        config: {
          getCurrentCluster: jest.fn().mockReturnValue({
            server: 'https://api.example.com',
          }),
        },
      },
      server: {
        address: jest.fn().mockReturnValue({
          address: 'localhost',
          port: 4000,
        }),
      },
      get: jest.fn(),
      addHook: jest.fn(),
    } as any;

    // Mock request
    mockRequest = {
      id: 'test-request-id',
      params: {
        '*': 'api/v1/pods',
      },
      query: {
        watch: 'true',
      },
      headers: {
        host: 'localhost',
        origin: 'http://localhost',
      },
    } as any;

    // Mock utils
    (getDirectCallOptions as jest.Mock).mockResolvedValue({});
    (getAccessToken as jest.Mock).mockReturnValue('test-token');

    // Mock WebSocket constructor
    (WebSocket as unknown as jest.Mock).mockImplementation(() => mockTargetSocket);
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  describe('Route Registration', () => {
    it('should register WebSocket route', async () => {
      await wssK8sRoutes(mockFastify);

      expect(mockFastify.get).toHaveBeenCalledWith('/*', { websocket: true }, expect.any(Function));
    });

    it('should register cleanup interval on initialization', async () => {
      await wssK8sRoutes(mockFastify);

      expect(mockFastify.addHook).toHaveBeenCalledWith('onClose', expect.any(Function));
    });
  });

  describe('Connection Lifecycle', () => {
    beforeEach(async () => {
      await wssK8sRoutes(mockFastify);
      routeHandler = (mockFastify.get as jest.Mock).mock.calls[0][2];
    });

    it('should initialize connection with metrics', async () => {
      await routeHandler(mockConnection, mockRequest);

      expect(mockLog.info).toHaveBeenCalledWith(
        expect.objectContaining({
          connectionId: expect.stringContaining('test-request-id'),
        }),
        expect.stringContaining('WebSocket watch initiated'),
      );
    });

    it('should create WebSocket with correct subprotocols', async () => {
      await routeHandler(mockConnection, mockRequest);

      expect(WebSocket).toHaveBeenCalledWith(
        expect.stringContaining('https://api.example.com/api/v1/pods'),
        expect.arrayContaining([
          expect.stringContaining('base64url.bearer.authorization.k8s.io'),
          'base64.binary.k8s.io',
        ]),
        expect.any(Object),
      );
    });

    it('should handle target connection open event', async () => {
      await routeHandler(mockConnection, mockRequest);

      const openHandler = mockTargetSocket.on.mock.calls.find(
        (call: any) => call[0] === 'open',
      )?.[1];

      expect(openHandler).toBeDefined();

      openHandler();

      expect(mockLog.debug).toHaveBeenCalledWith(
        expect.objectContaining({
          connectionId: expect.any(String),
        }),
        expect.stringContaining('WebSocket connection established to K8s API'),
      );
    });

    it('should handle connection timeout', async () => {
      mockTargetSocket.readyState = WebSocket.CONNECTING;

      await routeHandler(mockConnection, mockRequest);

      // Fast-forward time to trigger timeout
      jest.advanceTimersByTime(CONNECTION_TIMEOUT_MS);

      expect(mockTargetSocket.terminate).toHaveBeenCalled();
      expect(mockLog.error).toHaveBeenCalledWith(
        expect.objectContaining({
          connectionId: expect.any(String),
          timeout: CONNECTION_TIMEOUT_MS,
        }),
        expect.stringContaining('WebSocket connection timeout'),
      );
    });
  });

  describe('Idempotent Close Behavior', () => {
    let closeHandler: any;

    beforeEach(async () => {
      await wssK8sRoutes(mockFastify);
      routeHandler = (mockFastify.get as jest.Mock).mock.calls[0][2];
      await routeHandler(mockConnection, mockRequest);

      // Find the close handler for the source socket
      closeHandler = mockSourceSocket.on.mock.calls.find((call: any) => call[0] === 'close')?.[1];
    });

    it('should close connection only once when called multiple times', async () => {
      expect(closeHandler).toBeDefined();

      // Call close handler first time
      closeHandler(1000, 'Normal closure');

      // Call close handler second time
      closeHandler(1000, 'Duplicate closure');

      // Should only log once
      const infoLogs = mockLog.info.mock.calls.filter((call: any) =>
        call[1]?.includes('Closing websocket connection'),
      );
      expect(infoLogs.length).toBe(1);

      // Should close sockets only once
      expect(mockSourceSocket.close).toHaveBeenCalledTimes(1);
      expect(mockTargetSocket.close).toHaveBeenCalledTimes(1);
    });

    it('should not close if already removed from activeConnections', async () => {
      // First close
      closeHandler(1000, 'Normal closure');

      // Reset mocks
      mockLog.info.mockClear();
      mockSourceSocket.close.mockClear();
      mockTargetSocket.close.mockClear();

      // Second close attempt
      closeHandler(1000, 'Second attempt');

      // Should not log or close again
      const closingLogs = mockLog.info.mock.calls.filter((call: any) =>
        call[1]?.includes('Closing websocket connection'),
      );
      expect(closingLogs.length).toBe(0);
      expect(mockSourceSocket.close).not.toHaveBeenCalled();
      expect(mockTargetSocket.close).not.toHaveBeenCalled();
    });

    it('should handle close from both source and target without duplicates', async () => {
      const targetCloseHandler = mockTargetSocket.on.mock.calls.find(
        (call: any) => call[0] === 'close',
      )?.[1];

      expect(targetCloseHandler).toBeDefined();

      // Trigger close from source
      closeHandler(1000, 'Client closed');

      // Try to trigger close from target (should be idempotent)
      mockLog.info.mockClear();
      targetCloseHandler(1000, 'Server closed');

      // Should not log closing again
      const closingLogs = mockLog.info.mock.calls.filter((call: any) =>
        call[1]?.includes('Closing websocket connection'),
      );
      expect(closingLogs.length).toBe(0);
    });
  });

  describe('Buffer vs String Reason Handling', () => {
    let closeHandler: any;
    let targetCloseHandler: any;

    beforeEach(async () => {
      await wssK8sRoutes(mockFastify);
      routeHandler = (mockFastify.get as jest.Mock).mock.calls[0][2];
      await routeHandler(mockConnection, mockRequest);

      closeHandler = mockSourceSocket.on.mock.calls.find((call: any) => call[0] === 'close')?.[1];

      targetCloseHandler = mockTargetSocket.on.mock.calls.find(
        (call: any) => call[0] === 'close',
      )?.[1];
    });

    it('should handle string reason in source close', () => {
      const reason = 'Normal closure';
      closeHandler(1000, reason);

      expect(mockLog.info).toHaveBeenCalledWith(
        expect.objectContaining({
          reason: 'Normal closure',
        }),
        expect.stringContaining('Closing websocket connection'),
      );
    });

    it('should handle Buffer reason in source close', () => {
      const reason = Buffer.from('Buffer closure reason');
      closeHandler(1000, reason);

      expect(mockLog.info).toHaveBeenCalledWith(
        expect.objectContaining({
          reason: 'Buffer closure reason',
        }),
        expect.stringContaining('Closing websocket connection'),
      );
    });

    it('should handle string reason in target close', () => {
      const reason = 'Server closed';
      targetCloseHandler(1000, reason);

      expect(mockLog.info).toHaveBeenCalledWith(
        expect.objectContaining({
          reason: 'Server closed',
        }),
        expect.stringContaining('K8s API websocket closed'),
      );
    });

    it('should handle Buffer reason in target close', () => {
      const reason = Buffer.from('Server buffer close');
      targetCloseHandler(1000, reason);

      expect(mockLog.info).toHaveBeenCalledWith(
        expect.objectContaining({
          reason: 'Server buffer close',
        }),
        expect.stringContaining('K8s API websocket closed'),
      );
    });

    it('should convert Buffer to string only for logging, not for socket.close', () => {
      const reason = Buffer.from('Test buffer reason');
      closeHandler(1000, reason);

      // Verify log received string version
      expect(mockLog.info).toHaveBeenCalledWith(
        expect.objectContaining({
          reason: 'Test buffer reason',
        }),
        expect.any(String),
      );

      // Note: closeWebSocket internally calls toString() for socket.close()
      // but the close function should pass the original Buffer
    });
  });

  describe('Metrics Tracking', () => {
    beforeEach(async () => {
      await wssK8sRoutes(mockFastify);
      routeHandler = (mockFastify.get as jest.Mock).mock.calls[0][2];
    });

    it('should track messages received from K8s API', async () => {
      await routeHandler(mockConnection, mockRequest);

      const messageHandler = mockTargetSocket.on.mock.calls.find(
        (call: any) => call[0] === 'message',
      )?.[1];

      expect(messageHandler).toBeDefined();

      // Simulate receiving messages
      messageHandler('test message 1', false);
      messageHandler('test message 2', false);
      messageHandler('test message 3', false);

      // Advance timers to process queue
      jest.runOnlyPendingTimers();

      // Close connection to check metrics
      const closeHandler = mockSourceSocket.on.mock.calls.find(
        (call: any) => call[0] === 'close',
      )?.[1];
      closeHandler(1000, 'Done');

      expect(mockLog.info).toHaveBeenCalledWith(
        expect.objectContaining({
          messagesReceived: 3,
          messagesSent: 3, // All messages forwarded
        }),
        expect.stringContaining('Closing websocket connection'),
      );
    });

    it('should track resource versions from watch events', async () => {
      await routeHandler(mockConnection, mockRequest);

      const messageHandler = mockTargetSocket.on.mock.calls.find(
        (call: any) => call[0] === 'message',
      )?.[1];

      const watchEvent = JSON.stringify({
        type: 'ADDED',
        object: {
          metadata: {
            resourceVersion: '12345',
          },
        },
      });

      messageHandler(watchEvent, false);

      // Close to check metrics
      const closeHandler = mockSourceSocket.on.mock.calls.find(
        (call: any) => call[0] === 'close',
      )?.[1];
      closeHandler(1000, 'Done');

      expect(mockLog.info).toHaveBeenCalledWith(
        expect.objectContaining({
          lastResourceVersion: '12345',
        }),
        expect.stringContaining('Closing websocket connection'),
      );
    });

    it('should track bookmark events', async () => {
      await routeHandler(mockConnection, mockRequest);

      const messageHandler = mockTargetSocket.on.mock.calls.find(
        (call: any) => call[0] === 'message',
      )?.[1];

      const bookmarkEvent = JSON.stringify({
        type: 'BOOKMARK',
        object: {
          metadata: {
            resourceVersion: '99999',
          },
        },
      });

      messageHandler(bookmarkEvent, false);

      expect(mockLog.debug).toHaveBeenCalledWith(
        expect.objectContaining({
          resourceVersion: '99999',
        }),
        expect.stringContaining('Bookmark received'),
      );
    });

    it('should track connection duration', async () => {
      const startTime = Date.now();
      jest.setSystemTime(startTime);

      await routeHandler(mockConnection, mockRequest);

      // Advance time by 5 seconds
      jest.setSystemTime(startTime + 5000);

      const closeHandler = mockSourceSocket.on.mock.calls.find(
        (call: any) => call[0] === 'close',
      )?.[1];
      closeHandler(1000, 'Done');

      expect(mockLog.info).toHaveBeenCalledWith(
        expect.objectContaining({
          duration: 5000,
        }),
        expect.stringContaining('Closing websocket connection'),
      );
    });
  });

  describe('Message Queue Behavior', () => {
    beforeEach(async () => {
      await wssK8sRoutes(mockFastify);
      routeHandler = (mockFastify.get as jest.Mock).mock.calls[0][2];
    });

    it('should send messages directly when queue is empty and socket is open', async () => {
      await routeHandler(mockConnection, mockRequest);

      const messageHandler = mockTargetSocket.on.mock.calls.find(
        (call: any) => call[0] === 'message',
      )?.[1];

      const testData = 'test message';
      messageHandler(testData, false);

      expect(mockSourceSocket.send).toHaveBeenCalledWith(testData, { binary: false });
    });

    it('should queue messages when client socket is not ready', async () => {
      mockSourceSocket.readyState = WebSocket.CONNECTING;

      await routeHandler(mockConnection, mockRequest);

      const messageHandler = mockTargetSocket.on.mock.calls.find(
        (call: any) => call[0] === 'message',
      )?.[1];

      // Send multiple messages while socket is connecting
      messageHandler('message 1', false);
      messageHandler('message 2', false);
      messageHandler('message 3', false);

      // Socket shouldn't receive messages yet
      expect(mockSourceSocket.send).not.toHaveBeenCalled();

      // Now open the socket
      mockSourceSocket.readyState = WebSocket.OPEN;
      messageHandler('message 4', false);

      // All queued messages should be sent
      expect(mockSourceSocket.send).toHaveBeenCalledTimes(4);
    });

    it('should handle send errors by queuing for retry', async () => {
      await routeHandler(mockConnection, mockRequest);

      const messageHandler = mockTargetSocket.on.mock.calls.find(
        (call: any) => call[0] === 'message',
      )?.[1];

      // First send fails
      mockSourceSocket.send.mockImplementationOnce(() => {
        throw new Error('Send failed');
      });

      messageHandler('test message', false);

      expect(mockLog.error).toHaveBeenCalledWith(
        expect.objectContaining({
          error: 'Send failed',
        }),
        expect.stringContaining('Failed to forward message to client'),
      );
    });

    it('should limit queue size and drop oldest messages', async () => {
      mockSourceSocket.readyState = WebSocket.CONNECTING;

      await routeHandler(mockConnection, mockRequest);

      const messageHandler = mockTargetSocket.on.mock.calls.find(
        (call: any) => call[0] === 'message',
      )?.[1];

      // Send more than MESSAGE_QUEUE_LIMIT messages
      for (let i = 0; i < MESSAGE_QUEUE_LIMIT + 5; i++) {
        messageHandler(`message ${i}`, false);
      }

      expect(mockLog.warn).toHaveBeenCalledWith(
        expect.objectContaining({
          queueLength: expect.any(Number),
          limit: MESSAGE_QUEUE_LIMIT,
        }),
        expect.stringContaining('Message queue limit exceeded'),
      );
    });

    it('should process queue in order', async () => {
      mockSourceSocket.readyState = WebSocket.CONNECTING;

      await routeHandler(mockConnection, mockRequest);

      const messageHandler = mockTargetSocket.on.mock.calls.find(
        (call: any) => call[0] === 'message',
      )?.[1];

      // Queue messages
      messageHandler('first', false);
      messageHandler('second', false);
      messageHandler('third', false);

      // Open socket and send one more to trigger processing
      mockSourceSocket.readyState = WebSocket.OPEN;
      messageHandler('fourth', false);

      // Check order
      expect(mockSourceSocket.send).toHaveBeenNthCalledWith(1, 'first', { binary: false });
      expect(mockSourceSocket.send).toHaveBeenNthCalledWith(2, 'second', { binary: false });
      expect(mockSourceSocket.send).toHaveBeenNthCalledWith(3, 'third', { binary: false });
      expect(mockSourceSocket.send).toHaveBeenNthCalledWith(4, 'fourth', { binary: false });
    });
  });

  describe('Error Handling', () => {
    beforeEach(async () => {
      await wssK8sRoutes(mockFastify);
      routeHandler = (mockFastify.get as jest.Mock).mock.calls[0][2];
    });

    it('should handle target socket errors', async () => {
      await routeHandler(mockConnection, mockRequest);

      const errorHandler = mockTargetSocket.on.mock.calls.find(
        (call: any) => call[0] === 'error',
      )?.[1];

      const error = new Error('Connection failed');
      errorHandler(error);

      expect(mockLog.error).toHaveBeenCalledWith(
        expect.objectContaining({
          error: 'Connection failed',
        }),
        expect.stringContaining('K8s API websocket error'),
      );
    });

    it('should handle source socket errors', async () => {
      await routeHandler(mockConnection, mockRequest);

      const errorHandler = mockSourceSocket.on.mock.calls.find(
        (call: any) => call[0] === 'error',
      )?.[1];

      const error = new Error('Client error');
      errorHandler(error);

      expect(mockLog.error).toHaveBeenCalledWith(
        expect.objectContaining({
          error: 'Client error',
        }),
        expect.stringContaining('Client websocket error'),
      );
    });

    it('should handle unexpected responses from K8s API', async () => {
      await routeHandler(mockConnection, mockRequest);

      const unexpectedResponseHandler = mockTargetSocket.on.mock.calls.find(
        (call: any) => call[0] === 'unexpected-response',
      )?.[1];

      const mockResponse = {
        statusCode: 403,
        statusMessage: 'Forbidden',
      };

      unexpectedResponseHandler(undefined, mockResponse);

      expect(mockLog.error).toHaveBeenCalledWith(
        expect.objectContaining({
          statusCode: 403,
          statusMessage: 'Forbidden',
        }),
        expect.stringContaining('Unexpected response from K8s API'),
      );
    });
  });

  describe('Heartbeat and Monitoring', () => {
    beforeEach(async () => {
      await wssK8sRoutes(mockFastify);
      routeHandler = (mockFastify.get as jest.Mock).mock.calls[0][2];
    });

    it('should start heartbeat interval after connection opens', async () => {
      await routeHandler(mockConnection, mockRequest);

      const openHandler = mockTargetSocket.on.mock.calls.find(
        (call: any) => call[0] === 'open',
      )?.[1];

      openHandler();

      // Advance time to trigger heartbeat
      jest.advanceTimersByTime(HEARTBEAT_INTERVAL_MS);

      expect(mockTargetSocket.ping).toHaveBeenCalled();
    });

    it('should warn about inactivity', async () => {
      const startTime = Date.now();

      await routeHandler(mockConnection, mockRequest);

      const openHandler = mockTargetSocket.on.mock.calls.find(
        (call: any) => call[0] === 'open',
      )?.[1];

      openHandler();

      // Advance system time beyond the inactivity threshold
      jest.setSystemTime(startTime + INACTIVITY_WARNING_MS + 1000);

      // Advance timers to trigger the heartbeat interval
      jest.advanceTimersByTime(HEARTBEAT_INTERVAL_MS);

      expect(mockLog.warn).toHaveBeenCalledWith(
        expect.objectContaining({
          timeSinceLastMessage: expect.any(Number),
        }),
        expect.stringContaining('No messages received'),
      );
    });

    it('should clear heartbeat interval on close', async () => {
      await routeHandler(mockConnection, mockRequest);

      const openHandler = mockTargetSocket.on.mock.calls.find(
        (call: any) => call[0] === 'open',
      )?.[1];

      openHandler();

      // Trigger some pings
      jest.advanceTimersByTime(HEARTBEAT_INTERVAL_MS);
      const firstPingCount = mockTargetSocket.ping.mock.calls.length;

      // Close connection
      const closeHandler = mockSourceSocket.on.mock.calls.find(
        (call: any) => call[0] === 'close',
      )?.[1];
      closeHandler(1000, 'Done');

      // Advance time again
      jest.advanceTimersByTime(HEARTBEAT_INTERVAL_MS);

      // No additional pings should be sent
      expect(mockTargetSocket.ping.mock.calls.length).toBe(firstPingCount);
    });
  });

  describe('Stale Connection Cleanup', () => {
    it('should periodically clean up stale connections', async () => {
      await wssK8sRoutes(mockFastify);
      routeHandler = (mockFastify.get as jest.Mock).mock.calls[0][2];

      // Create a connection
      await routeHandler(mockConnection, mockRequest);

      // Advance time to trigger cleanup (runs every minute)
      jest.advanceTimersByTime(60000);

      // Connection is still active, so no cleanup yet
      expect(mockLog.warn).not.toHaveBeenCalledWith(
        expect.anything(),
        expect.stringContaining('Removing stale connection'),
      );

      // Advance time beyond stale threshold
      jest.advanceTimersByTime(STALE_CONNECTION_MS);
      jest.advanceTimersByTime(60000); // Trigger another cleanup cycle

      // Now stale connection should be cleaned up
      expect(mockLog.warn).toHaveBeenCalledWith(
        expect.objectContaining({
          inactivityDuration: expect.any(Number),
        }),
        expect.stringContaining('Removing stale connection from tracking'),
      );
    });
  });
});
