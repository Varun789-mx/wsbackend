import { WebsocketManager } from "./RedisManager";
import http from "http";
import WebSocket from "ws";

const port = process.env.PORT || 5000;

interface ClientMessage {
  type: string;
  room?: string | number;
  message?: string;
  [key: string]: any;
}

async function Init() {
  const webSocketManager = WebsocketManager.getsocket();
  const server = http.createServer((req, res) => {
    res.writeHead(200);
    res.end("web socket running");
  });

  server.on("upgrade", (request, socket, head) => {
    webSocketManager.wss.handleUpgrade(request, socket, head, (ws) => {
      const RequestTimeout = setTimeout(() => {
        ws.send(
          JSON.stringify({
            type: "Error",
            message: "No join message received",
          }),
        );
        console.log(
          "Server disconnected because client did't send the join request",
        );
        ws.close(1002, "Server disconnected");
      }, 5000);
      ws.once("message", (data: Buffer) => {
        clearTimeout(RequestTimeout);
        try {
          const ClientMessage: ClientMessage = JSON.parse(data.toString());
          if (ClientMessage.type == "join" && ClientMessage.room && String(ClientMessage.room).trim() !== "") {
            console.log("Received join message from client:", ClientMessage);
            const roomId = String(ClientMessage.room);
            webSocketManager.setRoom(ws, roomId);
            console.log(`Client succesfully Joined the ${roomId} `);
          }
          webSocketManager.wss.emit("connection", ws);
        } catch (error) {
          console.log("Error in parsing the first message", error);
          ws.close();
        }
      });
    });
  });
  webSocketManager.initlisteners();
  server.listen(port, () => {
    console.log(`Server is running on ws://localhost:${port}`);
  });
}

Init();
