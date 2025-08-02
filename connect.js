const jwt = require('jsonwebtoken');

function generateLiveKitToken({
  apiKey,
  apiSecret,
  room,
  participantName,
  ttlSeconds = 600
}) {
  const now = Math.floor(Date.now() / 1000);
  const payload = {
    'iss': apiKey,
    'sub': participantName, // identity
    'nbf': now,
    'exp': now + ttlSeconds,
    'name': participantName,
    'video': {
      'roomJoin': true,
      'room': room,
      
      'canPublish': true,
      'canSubscribe': true,
      'canPublishData': true,
      'hidden': false
    }
  };
  return jwt.sign(payload, apiSecret, { algorithm: 'HS256' });
}

// Example usage:
const apiKey = "APIAMrTXLVoxLqe";
const apiSecret = "3pFSQsUzLLeEEWrvO1hJaP4QA97CNeoMEkQA6wWSkuS";
const room = "my_private_sales_room_2024";
const participantName = "Sachin"; // Must be unique for each user

const token = generateLiveKitToken({
  apiKey,
  apiSecret,
  room,
  participantName,
  ttlSeconds: 7200 // optional: 10min expiry
});
console.log(token); // Paste this into the client
