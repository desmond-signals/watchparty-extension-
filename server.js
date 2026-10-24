const WebSocket = require('ws');
const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });
console.log(`Serveur WatchParty démarré sur le port ${PORT}`);
// Dictionnaire pour stocker les salons : { codeSalon: [websocket1, websocket2, ...] }
const rooms = {};

console.log(`Serveur WatchParty démarré sur le port ${PORT}`);

wss.on('connection', (ws) => {
  let currentRoom = null;

  ws.on('message', (message) => {
    let data;
    try {
      data = JSON.parse(message);
    } catch (e) {
      console.error("Erreur de parsing JSON", e);
      return;
    }

    // Ignorer les pings de maintien en vie
    if (data.type === 'ping') {
      ws.send(JSON.stringify({ type: 'pong' }));
      return;
    }

    // Action : Rejoindre ou créer un salon
    if (data.type === 'join') {
      // Si l'utilisateur est déjà dans ce salon précis, on ne fait rien !
      if (currentRoom === data.roomCode && rooms[currentRoom] && rooms[currentRoom].includes(ws)) {
        return;
      }

      // Retirer le client de son ANCIEN salon uniquement s'il change de salon
      if (currentRoom && currentRoom !== data.roomCode && rooms[currentRoom]) {
        rooms[currentRoom] = rooms[currentRoom].filter(client => client !== ws);
        if (rooms[currentRoom].length === 0) {
          delete rooms[currentRoom];
        }
      }

      currentRoom = data.roomCode;
      if (!rooms[currentRoom]) {
        rooms[currentRoom] = [];
      }
      rooms[currentRoom].push(ws);
      console.log(`Un utilisateur a rejoint le salon : ${currentRoom} (${rooms[currentRoom].length} connecté(s))`);
    }

    // Action : Transmettre une action de l'hôte aux autres membres du salon (Play, Pause, Seek)
    if (data.type === 'sync' && currentRoom) {
      if (rooms[currentRoom]) {
        rooms[currentRoom].forEach((client) => {
          // Envoyer le message à tout le monde SAUF à celui qui l'a envoyé
          if (client !== ws && client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify(data));
          }
        });
      }
    }
  });

  ws.on('close', () => {
    if (currentRoom && rooms[currentRoom]) {
      rooms[currentRoom] = rooms[currentRoom].filter(client => client !== ws);
      if (rooms[currentRoom].length === 0) {
        delete rooms[currentRoom];
        console.log(`Salon ${currentRoom} fermé car vide.`);
      }
    }
    console.log("Un utilisateur s'est déconnecté.");
  });
});