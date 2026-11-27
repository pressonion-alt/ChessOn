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
