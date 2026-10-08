/* PeerJS bridge for PeerJsTransport. Game data travels over WebRTC data channels. */
(function (root) {
  'use strict';
  var peer = null, objectName = 'PeerJsTransport';
  var isHost = false, promoted = false, migrating = false, closing = false;
  var room = '', clients = Object.create(null), mesh = Object.create(null);
  var toHost = null, migrationTimer = null, migrationStartedAt = 0;
  var peerOptions = {}, roomUpdateTimer = null, targetPeerId = '';
  var session = 0, clientJoined = false, libraryWaitStartedAt = 0;
  var endingSession = false, hostDisconnectNotified = false, pendingEndAcks = Object.create(null);
  // Link health: peers on flaky networks can starve without the channel ever
  // closing. Every link carries heartbeats so a silent peer is detected within
  // RESPONSE_TIMEOUT_MS and its ghost spider can be removed. A link that stops
  // delivering packets for WAITING_AFTER_MS reports its player as waiting first,
  // so the waiting circle can show above their head before the final drop.
  var HEARTBEAT_MS = 1000, LINK_STALL_MS = 6500, RESPONSE_TIMEOUT_MS = 15000;
  var WAITING_AFTER_MS = 3000;
  var heartbeatTimer = null, linkStatusReported = true;

  // Every callback belongs to one session; closed sessions must not restart or alter a later join.
  function defer(callback, delay) {
    var owner = session;
    return setTimeout(function () { if (owner === session && !closing && !endingSession) callback(); }, delay);
  }
  function isCurrent(owner, source) { return owner === session && !closing && peer === source; }
  function reportError(error) { unity('OnPeerError', error && error.message ? error.message : error); }
  function notifyHostDisconnected(id) {
    if (hostDisconnectNotified) return;
    hostDisconnectNotified = true;
    unity('OnPeerDisconnected', id);
  }
  function notifySessionEnded(id) {
    // A deliberate host end is final: the lobby is gone, so no reconnection offer.
    if (hostDisconnectNotified) return;
    hostDisconnectNotified = true;
    unity('OnPeerSessionEnded', id);
  }

  // --- link health -------------------------------------------------------
  function trackLink(connection) {
    connection.__hb = { lastHeard: Date.now(), lastBeat: 0, waiting: false };
    return connection;
  }
  function reportLinkWaiting(connection, waiting) {
    // Only gameplay links drive the waiting circle; mesh links exist for migration.
    var record = connection && connection.__hb;
    if (!record || record.waiting === waiting) return;
    record.waiting = waiting;
    unity('OnPeerLinkStale', connection.peer + '|' + (waiting ? '1' : '0'));
  }
  function heardLink(connection) {
    if (connection && connection.__hb) {
      connection.__hb.lastHeard = Date.now();
      reportLinkWaiting(connection, false);
    }
    updateLinkStatus();
  }
  function linkSilent(connection) {
    var record = connection && connection.__hb;
    return !connection || !connection.open || !record || Date.now() - record.lastHeard > LINK_STALL_MS;
  }
  function eachLink(visitor) {
    Object.keys(clients).forEach(function (id) { visitor(clients[id], 'client', id); });
    if (toHost) visitor(toHost, 'host', toHost.peer);
    Object.keys(mesh).forEach(function (id) { visitor(mesh[id], 'mesh', id); });
  }
  function updateLinkStatus() {
    // Only gameplay links drive the indicator; mesh links exist for migration.
    // A link that never opened is still joining and does not count either way.
    var healthy = true;
    eachLink(function (connection, role) {
      if (role === 'mesh' || !connection || !connection.open) return;
      if (linkSilent(connection)) healthy = false;
    });
    if (healthy === linkStatusReported) return;
    linkStatusReported = healthy;
    unity('OnPeerLinkStatus', healthy ? '1' : '0');
  }
  function isHeartbeat(data) {
    if (typeof data !== 'string') return false;
    var control;
    try { control = JSON.parse(data); } catch (_) { return false; }
    return !!control && control.type === 'hb';
  }
  function sendHeartbeat(connection) {
    try { connection.send(JSON.stringify({ type: 'hb' })); } catch (_) {}
  }
  function sendHeartbeatAck(connection) {
    try { connection.send(JSON.stringify({ type: 'hb-ack' })); } catch (_) {}
  }
  function heartbeatTick() {
    heartbeatTimer = null;
    var now = Date.now();
    eachLink(function (connection, role, id) {
      if (!connection || !connection.open) return;
      var record = connection.__hb;
      if (!record) { record = connection.__hb = { lastHeard: now, lastBeat: 0, waiting: false }; }
      if (now - record.lastHeard >= RESPONSE_TIMEOUT_MS) { dropSilentLink(connection, role, id); return; }
      if (now - record.lastBeat >= HEARTBEAT_MS) { record.lastBeat = now; sendHeartbeat(connection); }
      if (role !== 'mesh') reportLinkWaiting(connection, now - record.lastHeard >= WAITING_AFTER_MS);
    });
    updateLinkStatus();
    scheduleHeartbeat();
  }
  function scheduleHeartbeat() {
    if (heartbeatTimer || closing || endingSession) return;
    heartbeatTimer = defer(heartbeatTick, 1000);
  }
  function dropSilentLink(connection, role, id) {
    // Remove the map entry first so the close event cannot double-report.
    if (role === 'client') {
      if (clients[id] === connection) delete clients[id];
      try { connection.close(); } catch (_) {}
      // Forgetting the client here lets NGO despawn its ghost spider. The same
      // peer is welcome back: acceptClient takes its fresh channel in again.
      unity('OnPeerDisconnected', id);
      broadcastMesh();
    } else if (role === 'mesh') {
      if (mesh[id] === connection) delete mesh[id];
      try { connection.close(); } catch (_) {}
      if (!isHost && !migrating && (!toHost || !toHost.open)) hostLinkLost(id, toHost);
    } else {
      if (toHost === connection) toHost = null;
      try { connection.close(); } catch (_) {}
      hostLinkLost(id, connection);
    }
  }
  function hostLinkLost(hostId, connection) {
    if (isHost || migrating || endingSession || hostDisconnectNotified) return;
    var record = connection && connection.__hb;
    // A channel that closes while traffic still flowed means the host ended the
    // link (or is gone): keep the original host election. A starving link is
    // more likely this machine's flaky network, so drop the session cleanly and
    // let Unity keep retrying the same lobby until it is accepted again.
    if (clientJoined && record && Date.now() - record.lastHeard <= LINK_STALL_MS) { electHost(); return; }
    if (clientJoined) {
      hostDisconnectNotified = true;
      unity('OnPeerConnectionLost', hostId);
    } else notifyHostDisconnected(hostId);
  }

  function unity(method, value) {
    if (root.unityInstance && root.unityInstance.SendMessage)
      root.unityInstance.SendMessage(objectName, method, value == null ? '' : String(value));
  }
  function getOptions() {
    var result = {};
    if (root.PEERJS_HOST) result.host = root.PEERJS_HOST;
    if (root.PEERJS_PORT) result.port = Number(root.PEERJS_PORT);
    if (root.PEERJS_PATH) result.path = root.PEERJS_PATH;
    if (root.PEERJS_KEY) result.key = root.PEERJS_KEY;
    if (root.PEERJS_SECURE !== undefined) result.secure = !!root.PEERJS_SECURE;
    return result;
  }
  function encodeBase64(data) {
    if (typeof data === 'string') return data;
    var bytes = new Uint8Array(data), binary = '';
    for (var i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
    return btoa(binary);
  }
  function sendPacket(connection, encoded) {
    var binary = atob(encoded || ''), bytes = new Uint8Array(binary.length);
    for (var i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    connection.send(bytes.buffer);
  }
  function sendMigration(hostId, electedId) {
    var msg = JSON.stringify({ type: 'migration', host: hostId, elected: electedId });
    Object.keys(mesh).forEach(function (id) {
      if (mesh[id] && mesh[id].open) mesh[id].send(msg);
    });
  }
  function processMigration(data) {
    if (endingSession) return true;
    if (typeof data !== 'string') return false;
    var control;
    try { control = JSON.parse(data); } catch (_) { return false; }
    if (!control || control.type !== 'migration' || !control.host || !control.elected) return false;
    if (!isHost && !clientJoined) return true;
    // The lowest reachable peer ID is selected; repeated notifications are harmless.
    if (migrating) {
      if (control.host < migrationHostId) {
        migrationHostId = control.host;
        targetPeerId = control.elected;
        promoted = !!peer && peer.id === control.elected;
        isHost = promoted;
        defer(function () { if (peer && peer.id !== targetPeerId) connectHost(targetPeerId); }, 300);
      }
      return true;
    }
    migrating = true;
    migrationRoomReady = false;
    migrationHostId = control.host;
    targetPeerId = control.elected;
    promoted = !!peer && peer.id === control.elected;
    isHost = promoted;
    if (!promoted && toHost) {
      try { toHost.close(); } catch (_) {}
      toHost = null;
    }
    unity('OnPeerMigration', control.host + '|' + control.elected);
    return true;
  }
  var migrationHostId = '', migrationRoomReady = false;

  function connectMesh(id) {
    if (!peer || peer.destroyed || !id || id === peer.id || mesh[id]) return;
    var owner = session, source = peer, connection;
    try { connection = peer.connect(id, { reliable: true, serialization: 'raw' }); }
    catch (error) { reportError(error); return; }
    if (!connection) { reportError('PeerJS could not create a mesh connection.'); return; }
    mesh[id] = connection;
    trackLink(connection);
    function active() { return isCurrent(owner, source) && mesh[id] === connection; }
    connection.on('open', function () {
      if (!active()) return;
      heardLink(connection);
      if (migrating && targetPeerId && peer.id === targetPeerId) startMigrationHost();
    });
    connection.on('data', function (data) {
      if (!active()) return;
      heardLink(connection);
      if (isHeartbeat(data)) { sendHeartbeatAck(connection); return; }
      if (processMigration(data)) return;
      if (typeof data !== 'string') return;
      var control;
      try { control = JSON.parse(data); } catch (_) { return; }
      if (control && control.type === 'migration-room' && control.room) {
        room = String(control.room).toUpperCase();
        migrationRoomReady = true;
        if (!promoted) {
          if (toHost) { try { toHost.close(); } catch (_) {} toHost = null; }
          connectHost(targetPeerId);
        }
      }
    });
    connection.on('close', function () {
      if (!active()) return;
      delete mesh[id];
      if (!isHost && !migrating && (!toHost || !toHost.open)) hostLinkLost(id, toHost);
    });
    connection.on('error', function (e) { if (active()) reportError(e); });
  }
  function broadcastMesh() {
    if (endingSession) return;
    var ids = Object.keys(clients);
    var message = JSON.stringify({ type: 'mesh', peers: ids });
    ids.forEach(function (id) {
      var connection = clients[id];
      if (connection && connection.open) connection.send(message);
    });
  }
  function handleClientData(id, data) {
    if (isHeartbeat(data)) {
      var beat = clients[id];
      if (beat && beat.open) sendHeartbeatAck(beat);
      return;
    }
    if (typeof data === 'string') {
      var control;
      try { control = JSON.parse(data); } catch (_) { control = null; }
      if (control && control.type === 'session-end-ack') {
        if (endingSession) delete pendingEndAcks[id];
        return;
      }
      if (endingSession) return;
      if (control && control.type === 'migration') { processMigration(data); return; }
      if (control && control.type === 'migration-room' && control.room) {
        room = String(control.room).toUpperCase();
        migrationRoomReady = true;
        if (!promoted && targetPeerId && peer && peer.id !== targetPeerId) {
          if (toHost) { try { toHost.close(); } catch (_) {} toHost = null; }
          connectHost(targetPeerId);
          defer(function () { if (peer) unity('OnPeerMigration', room + '|' + targetPeerId); }, 150);
        } else if (promoted) startMigrationHost();
        return;
      }
      if (control && control.type === 'mesh' && Array.isArray(control.peers)) {
        control.peers.forEach(connectMesh);
        return;
      }
      // NGO packet bytes are binary. Ignore application strings/control messages.
      return;
    }
    unity('OnPeerData', id + '|' + encodeBase64(data));
  }
  function acceptClient(connection) {
    var id = connection.peer;
    if (endingSession) { try { connection.close(); } catch (_) {} return; }
    if (!id) return;
    if (clients[id]) {
      if (clients[id] === connection) return;
      // A fresh channel from a known peer means the old one starved silently
      // (a rejoin after a flaky drop). Replace it so the peer is let back in.
      var stale = clients[id];
      delete clients[id];
      try { stale.close(); } catch (_) {}
    }
    var owner = session, source = peer;
    clients[id] = connection;
    trackLink(connection);
    function active() { return isCurrent(owner, source) && clients[id] === connection; }
    connection.on('open', function () {
      if (!active()) return;
      heardLink(connection);
      if (isHost && !migrating) unity('OnPeerConnected', id);
      if (migrating && peer && peer.id === targetPeerId) startMigrationHost();
      broadcastMesh();
    });
    connection.on('data', function (data) { if (active()) { heardLink(connection); handleClientData(id, data); } });
    connection.on('close', function () {
      if (!active()) return;
      delete clients[id];
      if (isHost && !migrating) unity('OnPeerDisconnected', id);
      broadcastMesh();
    });
    connection.on('error', function (e) { if (active()) reportError(e); });
  }
  function connectHost(id) {
    if (!peer || peer.destroyed || !id || id === peer.id) return;
    if (toHost && toHost.peer === id) {
      if (toHost.open && !migrating) unity('OnPeerConnected', id);
      return;
    }
    if (toHost) { var previous = toHost; toHost = null; try { previous.close(); } catch (_) {} }
    var owner = session, source = peer, connection;
    try { connection = peer.connect(id, { reliable: true, serialization: 'raw' }); }
    catch (error) { reportError(error); return; }
    if (!connection) { reportError('PeerJS could not connect to the host.'); return; }
    toHost = connection;
    trackLink(connection);
    function active() { return isCurrent(owner, source) && toHost === connection; }
    connection.on('open', function () {
      if (!active()) return;
      heardLink(connection);
      if (!migrating) unity('OnPeerConnected', id);
      if (migrating && peer.id !== targetPeerId) {
        defer(function () { if (active()) unity('OnPeerMigration', room + '|' + targetPeerId); }, 150);
      }
    });
    connection.on('data', function (data) {
      if (!active()) return;
      heardLink(connection);
      if (isHeartbeat(data)) { sendHeartbeatAck(connection); return; }
      // Only the current host channel can intentionally end this session.
      if (typeof data === 'string') {
        var terminal;
        try { terminal = JSON.parse(data); } catch (_) { terminal = null; }
        if (terminal && terminal.type === 'session-ended') {
          try { connection.send(JSON.stringify({ type: 'session-end-ack' })); } catch (_) {}
          if (!endingSession) {
            endingSession = true;
            clientJoined = false;
            clearTimeout(migrationTimer);
            migrationTimer = null;
            notifySessionEnded(id);
          }
          return;
        }
      }
      if (processMigration(data)) return;
      if (typeof data === 'string') {
        var message;
        try { message = JSON.parse(data); } catch (_) { message = null; }
        if (message && message.type === 'mesh' && Array.isArray(message.peers)) {
          message.peers.forEach(connectMesh);
          return;
        }
        return;
      }
      if (!migrating) unity('OnPeerData', id + '|' + encodeBase64(data));
    });
    connection.on('close', function () {
      if (!active()) return;
      toHost = null;
      hostLinkLost(id, connection);
    });
    connection.on('error', function (e) { if (active()) reportError(e); });
  }
  function electHost() {
    if (!clientJoined || endingSession || migrationTimer || migrating || !peer || peer.destroyed || closing) return;
    var candidates = [peer.id].concat(Object.keys(mesh)).filter(function (id, index, list) {
      return !!id && list.indexOf(id) === index;
    }).sort();
    if (!candidates.length) {
      unity('OnPeerError', 'Host disconnected and no migration candidate remains.');
      return;
    }
    var elected = candidates[0];
    migrationStartedAt = Date.now();
    migrationTimer = defer(function () {
      migrationTimer = null;
      if (!peer || peer.destroyed || closing) return;
      var currentCandidates = [peer.id].concat(Object.keys(mesh)).sort();
      elected = currentCandidates[0];
      targetPeerId = elected;
      migrating = true;
      migrationRoomReady = false;
      migrationHostId = elected;
      promoted = peer.id === elected;
      isHost = promoted;
      sendMigration(elected, elected);
      unity('OnPeerMigration', elected + '|' + elected);
      if (promoted) startMigrationHost();
    }, 600);
  }
  function startMigrationHost() {
    if (!peer || peer.destroyed || peer.id !== targetPeerId || !migrating || !migrationRoomReady) return;
    isHost = true;
    promoted = true;
    if (roomUpdateTimer) { clearInterval(roomUpdateTimer); roomUpdateTimer = null; }
    defer(function () {
      if (peer && peer.id === targetPeerId && migrating) sendMigrationRoom(room);
    }, 150);
  }
  function sendMigrationRoom(code) {
    if (endingSession || !peer || peer.destroyed || !peer.id || peer.id !== targetPeerId) return;
    var payload = JSON.stringify({ type: 'migration-room', room: String(code || '').toUpperCase() });
    Object.keys(mesh).forEach(function (id) {
      if (mesh[id] && mesh[id].open) mesh[id].send(payload);
    });
    Object.keys(clients).forEach(function (id) {
      if (clients[id] && clients[id].open) clients[id].send(payload);
    });
  }
  function beginPeer(host, code, targetHost) {
    if (!root.Peer) {
      if (Date.now() - libraryWaitStartedAt >= 10000) {
        reportError('PeerJS library did not load. Reload the page and try again.');
        return;
      }
      defer(function () { beginPeer(host, code, targetHost); }, 250);
      return;
    }
    closing = false;
    endingSession = false;
    hostDisconnectNotified = false;
    pendingEndAcks = Object.create(null);
    isHost = !!host;
    promoted = false;
    migrating = false;
    migrationHostId = '';
    migrationStartedAt = Date.now();
    linkStatusReported = true;
    room = String(code || '').trim();
    targetPeerId = host ? '' : String(targetHost || ('ph-' + room));
    var owner = session, source;
    try { source = new root.Peer(host ? 'ph-' + room : 'pc-' + room + '-' + Math.random().toString(36).slice(2, 10), peerOptions); }
    catch (error) { reportError(error); return; }
    peer = source;
    scheduleHeartbeat();
    source.on('open', function (id) {
      if (!isCurrent(owner, source)) return;
      unity('OnPeerReady', id);
      if (!host) connectHost(targetHost || ('ph-' + room));
    });
    source.on('connection', function (connection) {
      if (!isCurrent(owner, source)) { try { connection.close(); } catch (_) {} return; }
      connection.on('data', function (data) {
        if (!isCurrent(owner, source)) return;
        if (!isHost || endingSession || typeof data !== 'string') return;
        var control;
        try { control = JSON.parse(data); } catch (_) { return; }
        if (control && control.type === 'migration-room' && control.room) {
          room = String(control.room).toUpperCase();
          migrationRoomReady = true;
          sendMigrationRoom(room);
          startMigrationHost();
        }
      });
    });
    if (host) source.on('connection', function (connection) { if (isCurrent(owner, source)) acceptClient(connection); });
    source.on('error', function (e) {
      if (!isCurrent(owner, source)) return;
      if (host && e && e.type === 'unavailable-id') {
        room = Math.random().toString(36).slice(2, 8).toUpperCase();
        unity('OnPeerError', 'Generated lobby code collided; retrying with a new code.');
        peer = null;
        source.destroy();
        defer(function () { beginPeer(true, room, ''); }, 50);
        return;
      }
      reportError(e);
    });
    source.on('disconnected', function () {
      if (!isCurrent(owner, source) || source.destroyed) return;
      if (!host && !clientJoined) { reportError('PeerJS signaling disconnected before the lobby join completed.'); return; }
      try { source.reconnect(); } catch (error) { reportError(error); }
    });
    source.on('close', function () {
      if (!isCurrent(owner, source)) return;
      if (!isHost && !migrating && !endingSession) notifyHostDisconnected(targetPeerId);
    });
  }

  root.PeerJS_Initialize = function (name, host, code, maxPeers, targetHost, migrationMode) {
    objectName = name || objectName;
    peerOptions = getOptions();
    if (migrationMode && peer && migrating) {
      // Preserve peer identity and mesh, but the restarted NGO client still needs approval.
      if (!host) clientJoined = false;
      scheduleHeartbeat();
      return;
    }
    root.PeerJS_Close();
    closing = false;
    endingSession = false;
    libraryWaitStartedAt = Date.now();
    beginPeer(!!host, code, targetHost);
  };
  root.PeerJS_ConfirmClientJoin = function () { if (peer && !closing && !endingSession) clientJoined = true; };
  root.PeerJS_EndSession = function () {
    if (!peer || closing || endingSession) return;
    endingSession = true;
    clientJoined = false;
    clearTimeout(migrationTimer);
    migrationTimer = null;
    // An explicit host departure ends the session; an unexpected channel loss still elects.
    if (!isHost || migrating) return;
    var message = JSON.stringify({ type: 'session-ended' });
    Object.keys(clients).forEach(function (id) {
      var connection = clients[id];
      if (!connection || !connection.open) return;
      pendingEndAcks[id] = true;
      try { connection.send(message); } catch (_) { delete pendingEndAcks[id]; }
    });
  };
  root.PeerJS_SessionEndPending = function () {
    return Object.keys(pendingEndAcks).some(function (id) {
      return clients[id] && clients[id].open;
    });
  };
  root.PeerJS_Send = function (clientId, encoded) {
    if (!peer || migrating) return;
    try {
      if (isHost) {
        if (clients[clientId] && clients[clientId].open) sendPacket(clients[clientId], encoded);
      } else if (toHost && toHost.open) sendPacket(toHost, encoded);
    } catch (error) { reportError(error); }
  };
  root.PeerJS_Disconnect = function (id) { if (clients[id]) clients[id].close(); };
  root.PeerJS_StartServer = function () {
    if (endingSession || !peer || peer.destroyed || !promoted) return;
    isHost = true;
    var owner = session, source = peer;
    source.on('connection', function (connection) { if (isCurrent(owner, source)) acceptClient(connection); });
  };
  root.PeerJS_PromoteHost = function () { if (!endingSession) { promoted = true; isHost = true; } };
  root.PeerJS_CompleteMigration = function () {
    if (endingSession || !peer || peer.destroyed || !migrating) return;
    migrating = false;
    migrationStartedAt = Date.now();
    if (roomUpdateTimer) { clearInterval(roomUpdateTimer); roomUpdateTimer = null; }
    if (promoted) root.PeerJS_StartServer();
    else connectHost(targetPeerId || migrationHostId);
  };
  root.PeerJS_SetMigrationRoom = function (code) {
    if (endingSession) return;
    room = String(code || '').toUpperCase();
    migrationRoomReady = true;
    sendMigrationRoom(room);
    if (promoted) startMigrationHost();
  };
  root.PeerJS_Close = function () {
    // C# skips this call during an intentional migration restart; explicit close always tears down.
    session++;
    closing = true;
    if (heartbeatTimer) clearTimeout(heartbeatTimer);
    heartbeatTimer = null;
    linkStatusReported = true;
    clientJoined = false;
    pendingEndAcks = Object.create(null);
    clearTimeout(migrationTimer);
    migrationTimer = null;
    Object.keys(clients).forEach(function (id) { try { clients[id].close(); } catch (_) {} });
    Object.keys(mesh).forEach(function (id) { try { mesh[id].close(); } catch (_) {} });
    clients = Object.create(null);
    mesh = Object.create(null);
    if (toHost) { try { toHost.close(); } catch (_) {} toHost = null; }
    if (peer) { try { peer.destroy(); } catch (_) {} peer = null; }
    isHost = false;
    promoted = false;
    migrating = false;
    migrationHostId = '';
    migrationRoomReady = false;
    targetPeerId = '';
    if (roomUpdateTimer) { clearInterval(roomUpdateTimer); roomUpdateTimer = null; }
  };
}(window));
