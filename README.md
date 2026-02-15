# 🕸️ wsbackend

A simple WebSocket backend server built with **TypeScript**, **Bun**, **http**, and **ws** for handling real-time connections.

This backend is meant to work with a client (e.g., a frontend app or real-time project) that connects via WebSockets and joins “rooms” for messaging.

---

## 🚀 What This Backend Does

- Listens for WebSocket upgrade requests
- Requires the **first message** from a client to be a join message
- Manages rooms and broadcasts based on socket events  
- Uses a central `WebsocketManager` to handle room logic

📌 If the client doesn’t send a valid join message within ~5 seconds, the connection is closed with an error.

---

## 📦 Installation

To install dependencies:

```bash
bun install
```
To start the server:
```bash
bun run start
```
By default the server listens on:
```bash
ws://localhost:5000
```
You can override the port by setting PORT in your environment.

📡 WebSocket API
📥 Expected First Message (Join)
When the client connects, the server expects JSON as the first message with type: "join".
```bash
{
  "type": "join",
  "room": "roomId"
}
type must be "join"

room is a string or number identifying the room

If this message is missing or malformed within ~5 seconds, the server sends an error and disconnects

📤 Example Join (Client Side)
const ws = new WebSocket("ws://localhost:5000");

ws.onopen = () => {
  ws.send(JSON.stringify({
    type: "join",
    room: "123"
  }));
};

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log("Message from server:", data);
};
```
📁 Project Structure
```bash
wsbackend/
├── src/
│   └── index.ts           # Main server
│   └── RedisManager.ts    # WebSocket manager logic
├── .env.example           # Sample env variables
├── package.json
├── tsconfig.json
├── bun.lock
└── README.md
```
⚠️ Notes & Current Status
❗ Initial Join Required
The server only accepts a client that sends a valid join message as the first message.
If this is not sent within ~5 seconds, the server sends:

```bash
{
  "type": "Error",
  "message": "No join message received"
}
```
Then disconnects the client.

🚧 Future Improvements
Add support for broadcast messages

Provide proper client events documentation

Add reconnection and heartbeat (ping/pong support)

Add automated tests

📜 License
This project is open source and available under the MIT License.
