import WebSocket, { WebSocketServer } from "ws";
import http from "http";
import Redis from "ioredis";
import { timeStamp } from "console";
import SubscriptionSet from "ioredis/built/SubscriptionSet";

/**
 * WebsocketManager handles WebSocket connections and Redis Pub/Sub communication
 *
 * ARCHITECTURE:
 * 1. WebSocket clients connect and send room join message
 * 2. Server extracts room ID and maps WebSocket to room
 * 3. Server subscribes to Redis channel for that room
 * 4. When client sends message -> Published to Redis channel
 * 5. Redis notifies all subscribers listening to that channel
 * 6. Messages broadcast to all WebSocket clients in that room
 */
export class WebsocketManager {
  private static Instance: WebsocketManager;
  public wss: WebSocketServer;

  // Redis Pub/Sub instances (separate instances for pub and sub)
  private publisher: Redis; // Publishes messages to Redis channels
  private subscriber: Redis; // Subscribes to Redis channels

  // Track WebSocket connections to rooms
  private roomMap: Map<WebSocket, string>;

  // Track which rooms are already subscribed to prevent duplicate subscriptions
  private SubscriptionSet = new Set<string>();

  constructor() {
    this.roomMap = new Map();
    const redisUrl = process.env.REDIS_URL || "";
    console.log(redisUrl, "redis");

    // Initialize Redis publisher instance
    this.publisher = new Redis(redisUrl);
    // Initialize Redis subscriber instance
    this.subscriber = new Redis(redisUrl);

    // Event: Subscriber connected to Redis
    this.subscriber.on("connect", () => {
      console.log("✅ Subscriber connected to Redis successfully");
    });

    // Event: Publisher connected to Redis
    this.publisher.on("connect", () => {
      console.log("✅ Publisher connected to Redis successfully");
    });

    /**
     * CRITICAL: Subscribe listener for Redis Pub/Sub
     * When a message is published to a subscribed channel, this handler fires
     * Flow: Redis Channel -> Received here -> Broadcast to WebSocket clients in room
     */
    this.subscriber.on("message", (channel, message) => {
      console.log(`[Redis Message Received] Channel: ${channel}`);
      this.BroadCast(channel, message);
    });

    // Error handling for subscriber
    this.subscriber.on("error", (err) => {
      console.error("Error in Redis subscriber:", err);
    });

    // Error handling for publisher
    this.publisher.on("error", (err) => {
      console.error("Error in Redis publisher:", err);
    });

    // Initialize WebSocket server with noServer (we handle upgrade manually)
    this.wss = new WebSocketServer({ noServer: true });
  }

  /**
   * BROADCAST STEP: Send Redis message to all WebSocket clients in the room
   * This is called when Redis delivers a pub/sub message
   * @param userChannel - Room/channel name
   * @param message - Message from Redis
   */
  private BroadCast(userChannel: string, message: string) {
    console.log(`[Broadcasting] Room: ${userChannel}`);
    this.roomMap.forEach((channelName, ws) => {
      // Only send to clients in this room and with active connection
      if (channelName === userChannel && ws.readyState === WebSocket.OPEN) {
        console.log(`[Send to Client] ${message}`);
        ws.send(message);
      }
    });
  }

  /**
   * ROOM MAPPING: Associate a WebSocket with a room ID
   * Called when client sends first message with join type
   * @param ws - WebSocket connection
   * @param room - Room ID to join
   */
  public setRoom(ws: WebSocket, room: string) {
    this.roomMap.set(ws, room);
    console.log(`[Room Set] WebSocket -> Room: ${room}`);
  }

  public HandleHttpRequest = (
    req: http.IncomingMessage,
    res: http.ServerResponse,
  ) => {
    res.writeHead(200);
    res.end("Web Socket running");
  };

  /**
   * REDIS SUBSCRIPTION STEP: Subscribe to a Redis channel
   * This ensures the subscriber listens to messages on this channel
   * Only subscribes once per room (checked via SubscriptionSet)
   * @param room - Room ID to subscribe to
   */
  private async Subscribe(room: string) {
    // Check if already subscribed to avoid duplicate subscriptions
    if (this.SubscriptionSet.has(room)) {
      console.log(`[Already Subscribed] Room: ${room}`);
      return;
    }

    // Subscribe the subscriber instance to the Redis channel
    await this.subscriber.subscribe(room);
    this.SubscriptionSet.add(room);
    console.log(`[Redis Subscribe] Now listening to channel: ${room}`);
  }

  /**
   * Singleton pattern - ensure only one instance
   */
  static getsocket() {
    if (!this.Instance) {
      this.Instance = new WebsocketManager();
    }
    return this.Instance;
  }

  /**
   * Initialize WebSocket message listeners
   * Handles: messages, errors, and connection close
   */
  public initlisteners() {
    const wss = this.wss;
    wss.on("connection", (ws) => {
      console.log("[WebSocket] New connection established");
      const room = this.roomMap.get(ws);

      // Subscribe to Redis channel if room is set
      if (room) {
        console.log(`[WebSocket Handler] Client in room: ${room}`);
        this.Subscribe(room);
      }

      // Handle WebSocket errors
      ws.on("error", console.error);

      /**
       * MESSAGE FLOW:
       * 1. Client sends message via WebSocket
       * 2. Publish to Redis channel (room)
       * 3. Redis notifies all subscribers
       * 4. BroadCast receives it and sends to all clients in room
       *
       * Expected message format from client:
       * {
       *     "type": "message",
       *     "content": "Hello everyone",
       *     "sender": "user123"
       * }
       */
      ws.on("message", async (message) => {
        if (!room) {
          console.warn("[Message] No room assigned to client");
          return;
        }
        try {
          const messageText = message.toString();
          console.log(`[Raw Message from Client] ${messageText}`);

          // Try to parse as JSON, if fails treat as plain text
          let parsedMessage: any;
          try {
            parsedMessage = JSON.parse(messageText);
          } catch {
            // Plain text message
            parsedMessage = {
              type: "message",
              content: messageText,
              sender: "unknown",
            };
          }

          // Only process message-type messages (skip other commands)
          if (
            parsedMessage.type !== "message" &&
            parsedMessage.type !== "leave"
          ) {
            console.warn(`[Ignored] Message type: ${parsedMessage.type}`);
            return;
          }
          if (parsedMessage.type === "leave") {
            console.warn(
              `[Closed connection] Message type: ${parsedMessage.type}`,
            );
            const redisPayload = {
              type: "leave",
              content: `${parsedMessage.sender} left the chat`,
              timeStamp: Date.now(),
              room: room,
            };
            this.publisher.publish(room, JSON.stringify(redisPayload));
            this.roomMap.delete(ws);
            ws.close();
            return;
          }

          /**
           * PUBLISH STEP: Send message to Redis channel
           * This triggers the subscriber.on("message") handler
           * All servers subscribed to this channel will receive it
           */
          const redisPayload = {
            type: "message",
            content: parsedMessage.content || messageText,
            sender: parsedMessage.sender || "anonymous",
            timestamp: Date.now(),
            room: room,
          };

          this.publisher.publish(room, JSON.stringify(redisPayload));

          console.log(
            `[Published to Redis] Room: ${room} | Sender: ${redisPayload.sender}`,
          );
       
          
        } catch (error) {
          console.error(`[Error Publishing Message] ${error}`);
          ws.send(JSON.stringify({ error: "Failed to process message" }));
        }
      });

      // Handle connection close
      ws.on("close", (code, reason) => {
        if (room) {
          if (![...this.roomMap.values()].includes(room)) {
            this.subscriber.unsubscribe(room);
            this.SubscriptionSet.delete(room);
          }
        }
        console.log(`[WebSocket Closed] Code: ${code}, Reason: ${reason}`);
        this.roomMap.delete(ws);
      });
    });
  }
}
