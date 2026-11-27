"use strict";

const { createServer } = require("node:http");
const { createReadStream } = require("node:fs");
const { stat } = require("node:fs/promises");
const { randomBytes, randomInt } = require("node:crypto");
const { dirname, extname, join } = require("node:path");
const { pipeline } = require("node:stream/promises");
const { Chess } = require("chess.js");
const { WebSocket, WebSocketServer } = require("ws");

const HOST = process.env.HOST || "127.0.0.1";
const PORT = Number(process.env.PORT || 3000);
const PUBLIC_ORIGIN = "https://chess-on.duckdns.org";
const MAX_CONNECTIONS = 500;
const MAX_ROOMS = 1000;
const ROOM_IDLE_MS = 60 * 60 * 1000;
const SEAT_REPLACED = 4001;
const ROOM_CODE = /^[A-F0-9]{12}$/;
const SEAT_TOKEN = /^[a-f0-9]{48}$/;
const MOVE_CODE = /^[a-h][1-8][a-h][1-8][qrbn]?$/;

const indexFile = join(__dirname, "index.html");
const files = new Map([
  ["/", indexFile],
  ["/index.html", indexFile],
  ["/client.html", indexFile],
  ["/chess.js", join(dirname(require.resolve("chess.js")), "../esm/chess.js")],
]);
for (const filename of [
  "chessonlogo.png", "WPawn.png", "WRook.png", "WHorse.png", "WBishop.png",
  "WQueen.png", "Wking.png", "BPawn.png", "BRook.png", "BHorse.png", "BBishop.png",
  "BQueen.png", "Bking.png", "engine/stockfish-19-lite-single.js",
  "engine/stockfish-19-lite-single.wasm", "bB.svg", "bK.svg", "bN.svg", "bP.svg", "bQ.svg", "bR.svg", "wB.svg", "wK.svg", "wN.svg", "wP.svg", "wQ.svg", "wR.svg"
]) {
  files.set(`/${filename}`, join(__dirname, filename));
}
const mimeTypes = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".png": "image/png",
  ".wasm": "application/wasm",
};

async function serve(req, res) {
  res.setHeader("X-Content-Type-Options", "nosniff");
  if (req.method !== "GET" && req.method !== "HEAD") {
    res.writeHead(405, { Allow: "GET, HEAD" }).end();
    return;
  }

  const path = req.url.split("?", 1)[0];
  if (path === "/healthz") {
    res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" }).end("OK\n");
    return;
  }
  const file = files.get(path);
  if (!file) {
    res.writeHead(404).end();
    return;
  }

  try {
    const info = await stat(file);
    if (!info.isFile()) {
      res.writeHead(404).end();
      return;
    }
    const extension = extname(file);
    res.writeHead(200, {
      "Content-Type": mimeTypes[extension],
      "Content-Length": info.size,
      "Cache-Control": extension === ".html" ? "no-cache" : "public, max-age=3600",
    });
    if (req.method === "HEAD") res.end();
    else await pipeline(createReadStream(file), res);
  } catch (error) {
    if (res.headersSent || res.destroyed) return;
    if (error.code === "ENOENT") res.writeHead(404).end();
    else {
      console.error("Could not serve", path, error);
      res.writeHead(500).end();
    }
  }
}

const rooms = new Map();
const seats = new Map();
const queue = new Map();
const server = createServer(serve);
const sockets = new WebSocketServer({ noServer: true, maxPayload: 4096, perMessageDeflate: false });

function allowedOrigin(origin) {
  return origin === PUBLIC_ORIGIN || origin === "null" ||
    /^http:\/\/(?:localhost|127\.0\.0\.1|\[::1\])(?::\d+)?$/.test(origin);
}

server.on("upgrade", (req, socket, head) => {
  if (req.url !== "/multiplayer" || !allowedOrigin(req.headers.origin)) {
    socket.end("HTTP/1.1 403 Forbidden\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
    return;
  }
  if (sockets.clients.size >= MAX_CONNECTIONS) {
    socket.end("HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
    return;
  }
  sockets.handleUpgrade(req, socket, head, ws => sockets.emit("connection", ws));
});

const opposite = color => color === "w" ? "b" : "w";
const connected = player => player?.ws?.readyState === WebSocket.OPEN;

function send(ws, message) {
  if (ws?.readyState !== WebSocket.OPEN) return;
  if (ws.bufferedAmount > 256 * 1024) ws.terminate();
  else ws.send(JSON.stringify(message));
}

function error(ws, message) {
  send(ws, { type: "error", message });
}

function createRoom(kind, analysisAllowed = false) {
  let code;
  do { code = randomBytes(6).toString("hex").toUpperCase(); } while (rooms.has(code));
  const room = {
    code, kind, analysisAllowed,
    game: new Chess(),
    moves: [],
    players: { w: null, b: null },
    resigned: null,
    touchedAt: Date.now(),
  };
  rooms.set(code, room);
  return room;
}

function deleteRoom(room) {
  rooms.delete(room.code);
  for (const player of Object.values(room.players)) {
    if (player && seats.get(player.token)?.room === room) seats.delete(player.token);
  }
}

function broadcast(room) {
  room.touchedAt = Date.now();
  for (const color of ["w", "b"]) {
    const opponent = room.players[opposite(color)];
    send(room.players[color]?.ws, {
      type: "state",
      moves: room.moves,
      opponentJoined: Boolean(opponent),
      opponentConnected: connected(opponent),
      resigned: room.resigned,
    });
  }
}

function unqueue(ws) {
  if (queue.get(ws.queuedToken) === ws) queue.delete(ws.queuedToken);
  ws.queuedToken = null;
}

function attach(ws, room, color, token) {
  const previous = room.players[color]?.ws;
  unqueue(ws);
  room.players[color] = { token, ws };
  seats.set(token, { room, color });
  ws.seat = { room, color };
  if (previous && previous !== ws) previous.close(SEAT_REPLACED, "Seat opened in another tab");
  send(ws, { type: "joined", code: room.code, kind: room.kind, color, analysisAllowed: room.analysisAllowed });
}

function match(ws, token) {
  if (rooms.size >= MAX_ROOMS) return error(ws, "The server is full. Try again shortly.");
  const previous = queue.get(token);
  if (previous && previous !== ws) previous.close(SEAT_REPLACED, "Queue opened in another tab");
  unqueue(ws);
  queue.delete(token);
  const opponents = [...queue].filter(([, opponent]) => opponent.readyState === WebSocket.OPEN);
  if (!opponents.length) {
    ws.queuedToken = token;
    queue.set(token, ws);
    send(ws, { type: "waiting" });
    return;
  }

  const [opponentToken, opponent] = opponents[randomInt(opponents.length)];
  const room = createRoom("random");
  const color = randomInt(2) === 0 ? "w" : "b";
  attach(ws, room, color, token);
  attach(opponent, room, opposite(color), opponentToken);
  broadcast(room);
}

function enterRoom(ws, message) {
  const { type, token, code } = message;
  if (typeof token !== "string" || !SEAT_TOKEN.test(token)) return error(ws, "Invalid seat token.");

  const saved = seats.get(token);
  if (saved) {
    if ((type === "join" || type === "resume") && saved.room.code !== code) {
      return error(ws, "This seat belongs to a different room.");
    }
    attach(ws, saved.room, saved.color, token);
    broadcast(saved.room);
    return;
  }
  if (type === "resume") return error(ws, "Your room has expired. Start a new game.");
  if (type === "match") return match(ws, token);

  let room;
  let color;
  if (type === "create") {
    if (rooms.size >= MAX_ROOMS) return error(ws, "The server is full. Try again shortly.");
    room = createRoom("private", message.analysisAllowed === true);
    color = "w";
  } else {
    if (typeof code !== "string" || !ROOM_CODE.test(code)) return error(ws, "Enter a valid room code.");
    room = rooms.get(code);
    if (!room || room.kind !== "private") return error(ws, "Invite room not found.");
    if (room.players.b) return error(ws, "This room already has two players.");
    color = "b";
  }
  attach(ws, room, color, token);
  broadcast(room);
}

function playMove(ws, room, color, message) {
  if (room.resigned || room.game.isGameOver()) return error(ws, "This game has ended.");
  if (!connected(room.players[opposite(color)])) return error(ws, "Wait for your opponent to connect.");
  if (room.game.turn() !== color) return error(ws, "It is your opponent’s turn.");
  if (message.ply !== room.moves.length) return error(ws, "The board changed. Try the move again.");

  const code = message.move;
  if (typeof code !== "string" || !MOVE_CODE.test(code)) return error(ws, "Invalid move.");
  const move = room.game.moves({ square: code.slice(0, 2), verbose: true })
    .find(candidate => candidate.from + candidate.to + (candidate.promotion || "") === code);
  if (!move) return error(ws, "Illegal move.");
  room.game.move(move);
  room.moves.push(code);
  broadcast(room);
}

function leaveRoom(ws, room, color) {
  const player = room.players[color];
  const opponent = room.players[opposite(color)];
  seats.delete(player.token);
  player.ws = null;
  ws.seat = null;
  if (!opponent || seats.get(opponent.token)?.room !== room) deleteRoom(room);
  else {
    if (!room.resigned && !room.game.isGameOver()) room.resigned = color;
    broadcast(room);
  }
  ws.close();
}

sockets.on("connection", ws => {
  ws.alive = true;
  ws.on("pong", () => { ws.alive = true; });
  ws.on("error", () => ws.terminate());
  ws.on("close", () => {
    unqueue(ws);
    if (!ws.seat) return;
    const { room, color } = ws.seat;
    if (room.players[color].ws !== ws) return;
    room.players[color].ws = null;
    broadcast(room);
  });

  ws.on("message", (data, binary) => {
    if (ws.readyState !== WebSocket.OPEN) return;
    let message;
    if (!binary) {
      try { message = JSON.parse(data.toString()); } catch { /* Invalid JSON. */ }
    }
    if (!message || typeof message !== "object") return error(ws, "Invalid message.");

    const { type } = message;
    if (!ws.seat) {
      if (type === "leave") { ws.close(); return; }
      if (["create", "join", "resume", "match"].includes(type)) enterRoom(ws, message);
      else error(ws, "Join a game first.");
      return;
    }
    const { room, color } = ws.seat;
    if (room.players[color].ws !== ws) return;
    if (type === "move") playMove(ws, room, color, message);
    else if (type === "leave") leaveRoom(ws, room, color);
    else error(ws, "You are already in a game.");
  });
});

setInterval(() => {
  for (const ws of sockets.clients) {
    if (!ws.alive) ws.terminate();
    else {
      ws.alive = false;
      ws.ping();
    }
  }
  for (const room of rooms.values()) {
    if (!Object.values(room.players).some(connected) && Date.now() - room.touchedAt >= ROOM_IDLE_MS) {
      deleteRoom(room);
    }
  }
}, 20_000).unref();

server.listen(PORT, HOST, () => console.log(`ChessOn listening on http://${HOST}:${PORT}`));

