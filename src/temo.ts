import { WebsocketManager } from "./RedisManager";
import http from "http"
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
    })

    server.on('upgrade', (request, socket, head) => {
        webSocketManager.wss.handleUpgrade(request, socket, head, (ws: WebSocket) => {
            // Wait for the first message to extract room ID
            ws.once('message', (data: Buffer) => {
                try {
                    const clientMessage: ClientMessage = JSON.parse(data.toString());
                    
                    if (clientMessage.type === 'join' && clientMessage.room) {
                        const roomId = String(clientMessage.room);
                        webSocketManager.setRoom(ws, roomId);
                        console.log(`Client joined room: ${roomId}`);
                    }
                    
                    // Emit connection event to initialize listeners
                    webSocketManager.wss.emit("connection", ws);
                } catch (error) {
                    console.error('Error parsing initial message:', error);
                    ws.close();
                }
            });
        });
    })
    
    webSocketManager.initlisteners();
    server.listen(port, () => {
        console.log(`Server is running on ws://localhost:${port}`)
    })
}

Init();