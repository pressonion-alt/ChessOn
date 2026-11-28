"use strict";

const { createServer } = require("node:http");
const { createReadStream, existsSync, unlinkSync, writeFileSync } = require("node:fs");
const { readFile, stat } = require("node:fs/promises");
const { createHash, randomBytes, randomInt, timingSafeEqual } = require("node:crypto");
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
const MAINTENANCE_NOTICE = "ATTENTION - Site down for maintenance, thank you for your patience";
const maintenanceFile = join(process.env.STATE_DIRECTORY || __dirname, "maintenance-active");
let maintenanceActive = existsSync(maintenanceFile);
const digestKey = key => createHash("sha256").update(key).digest();
const adminKey = process.env.CHESSON_ADMIN_KEY ? digestKey(process.env.CHESSON_ADMIN_KEY) : null;

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
  "BQueen.png", "Bking.png",
  "bB.svg", "bK.svg", "bN.svg", "bP.svg", "bQ.svg", "bR.svg",
  "wB.svg", "wK.svg", "wN.svg", "wP.svg", "wQ.svg", "wR.svg",
  "engine/stockfish-19-lite-single.js", "engine/stockfish-19-lite-single.wasm",
]) {
  files.set(`/${filename}`, join(__dirname, filename));
}
const mimeTypes = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".png": "image/png",
  ".svg": "image/svg+xml",
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
  if (path === "/site-status") {
    res.writeHead(200, {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
      "Access-Control-Allow-Origin": "*",
    }).end(JSON.stringify({ maintenanceActive }));
    return;
  }
  const file = files.get(path);
  if (!file) {
    res.writeHead(404).end();
    return;
  }

  try {
    if (file === indexFile && maintenanceActive) {
      const html = (await readFile(file, "utf8")).replace(/<html\b/i, '<html data-maintenance="true"');
      res.writeHead(503, {
        "Content-Type": mimeTypes[".html"],
        "Content-Length": Buffer.byteLength(html),
        "Cache-Control": "no-store",
      }).end(req.method === "HEAD" ? undefined : html);
      return;
    }
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
const admins = new Set();
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

function adminError(ws, message) {
  send(ws, { type: "adminError", message });
}

function ended(room) {
  return room.stopped || room.gameOver || Boolean(room.resigned);
}

function adminSnapshot() {
  const describeSeat = player => ({ joined: Boolean(player), connected: connected(player) });
  return {
    type: "adminState", maintenanceActive,
    rooms: Array.from(rooms.values(), room => ({
      code: room.code, kind: room.kind, turn: room.game.turn(), ply: room.moves.length,
      fen: room.game.fen(), stopped: room.stopped, ended: ended(room),
      players: { w: describeSeat(room.players.w), b: describeSeat(room.players.b) },
    })),
  };
}

let adminUpdatePending = false;
function updateAdmins() {
  if (!admins.size || adminUpdatePending) return;
  adminUpdatePending = true;
  setImmediate(() => {
    adminUpdatePending = false;
    if (!admins.size) return;
    const snapshot = adminSnapshot();
    for (const admin of admins) send(admin, snapshot);
  });
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
    stopped: false,
    gameOver: false,
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
  updateAdmins();
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
      stopped: room.stopped,
    });
  }
  updateAdmins();
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
  if (maintenanceActive) return maintenanceNotice(ws);
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
    if (ended(room)) return error(ws, "This game has ended.");
    if (room.players.b) return error(ws, "This room already has two players.");
    color = "b";
  }
  attach(ws, room, color, token);
  broadcast(room);
}

function applyMove(room, message) {
  if (ended(room)) return "This game has ended.";
  if (message.ply !== room.moves.length) return "The board changed. Try the move again.";

  const code = message.move;
  if (typeof code !== "string" || !MOVE_CODE.test(code)) return "Invalid move.";
  const move = room.game.moves({ square: code.slice(0, 2), verbose: true })
    .find(candidate => candidate.from + candidate.to + (candidate.promotion || "") === code);
  if (!move) return "Illegal move.";
  room.game.move(move);
  room.moves.push(code);
  room.gameOver = room.game.isGameOver();
  broadcast(room);
}

function maintenanceNotice(ws) {
  send(ws, { type: "siteStatus", maintenanceActive: true });
  send(ws, { type: "error", message: MAINTENANCE_NOTICE, maintenanceActive: true });
}

function playMove(ws, room, color, message) {
  if (maintenanceActive) return maintenanceNotice(ws);
  if (ended(room)) return error(ws, "This game has ended.");
  if (!connected(room.players[opposite(color)])) return error(ws, "Wait for your opponent to connect.");
  if (room.game.turn() !== color) return error(ws, "It is your opponent’s turn.");
  const problem = applyMove(room, message);
  if (problem) error(ws, problem);
}

function leaveRoom(ws, room, color) {
  const player = room.players[color];
  const opponent = room.players[opposite(color)];
  seats.delete(player.token);
  player.ws = null;
  ws.seat = null;
  if (!opponent || seats.get(opponent.token)?.room !== room) deleteRoom(room);
  else {
    if (!ended(room)) room.resigned = color;
    broadcast(room);
  }
  ws.close();
}

function authenticateAdmin(ws, key) {
  if (!adminKey) return adminError(ws, "Set CHESSON_ADMIN_KEY on the server to enable the panel.");
  if (ws.seat || ws.queuedToken) return adminError(ws, "Open a separate admin connection.");
  if (typeof key !== "string" || !timingSafeEqual(adminKey, digestKey(key))) {
    return adminError(ws, "Incorrect admin key.");
  }
  admins.add(ws);
  send(ws, adminSnapshot());
}

function setMaintenance(ws, active) {
  if (typeof active !== "boolean") return adminError(ws, "Choose an active or inactive maintenance state.");
  if (maintenanceActive !== active) {
    try {
      if (active) writeFileSync(maintenanceFile, "active\n", { mode: 0o600 });
      else unlinkSync(maintenanceFile);
    } catch (failure) {
      if (active || failure.code !== "ENOENT") {
        console.error("Could not update maintenance state", failure);
        return adminError(ws, "Could not save maintenance. Check the server's state directory permissions.");
      }
    }
    maintenanceActive = active;
    if (!active) {
      for (const room of rooms.values()) room.touchedAt = Date.now();
    }
    for (const client of sockets.clients) send(client, { type: "siteStatus", maintenanceActive });
  }
  updateAdmins();
  send(ws, { type: "adminOk", message: active ? "Maintenance is active." : "The site is open." });
}

function adminCommand(ws, message) {
  if (!admins.has(ws)) return adminError(ws, "Sign in to the admin panel.");
  if (message.type === "adminMaintenance") return setMaintenance(ws, message.active);
  const room = rooms.get(message.code);
  if (!room) return adminError(ws, "Game not found.");
  if (message.type === "adminMove") {
    if (maintenanceActive) return adminError(ws, MAINTENANCE_NOTICE);
    const problem = applyMove(room, message);
    if (problem) return adminError(ws, problem);
    return send(ws, { type: "adminOk", message: "Move played." });
  }
  if (message.type === "adminStop") {
    if (ended(room)) return adminError(ws, "This game has ended.");
    room.stopped = true;
    broadcast(room);
    return send(ws, { type: "adminOk", message: "Game stopped." });
  }
  if (message.type === "adminBan") {
    if (message.color !== "w" && message.color !== "b") return adminError(ws, "Choose White or Black.");
    const player = room.players[message.color];
    if (!connected(player)) return adminError(ws, "This player must be connected to receive the ban.");
    const target = player.ws;
    send(target, { type: "banned" });
    target.close(4003, "Banned from online play");
    seats.delete(player.token);
    target.seat = null;
    player.ws = null;
    if (!ended(room)) room.stopped = true;
    broadcast(room);
    return send(ws, { type: "adminOk", message: "Ban sent to the player's browser." });
  }
  adminError(ws, "Unknown admin action.");
}

sockets.on("connection", ws => {
  ws.alive = true;
  if (maintenanceActive) send(ws, { type: "siteStatus", maintenanceActive });
  ws.on("pong", () => { ws.alive = true; });
  ws.on("error", () => ws.terminate());
  ws.on("close", () => {
    admins.delete(ws);
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
    if (type === "adminAuth") return authenticateAdmin(ws, message.key);
    if (admins.has(ws) || (typeof type === "string" && type.startsWith("admin"))) {
      return adminCommand(ws, message);
    }
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
    if (!maintenanceActive && !Object.values(room.players).some(connected) && Date.now() - room.touchedAt >= ROOM_IDLE_MS) {
      deleteRoom(room);
    }
  }
}, 20_000).unref();

server.listen(PORT, HOST, () => console.log(`ChessOn listening on http://${HOST}:${PORT}`));
