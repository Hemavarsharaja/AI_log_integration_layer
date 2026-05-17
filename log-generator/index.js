const { v4: uuidv4 } = require('uuid');

// Log levels with weights
const LEVELS = [
  { level: 'DEBUG', weight: 10 },
  { level: 'INFO', weight: 50 },
  { level: 'WARN', weight: 20 },
  { level: 'ERROR', weight: 15 },
  { level: 'FATAL', weight: 5 }
];

// Message templates
const MESSAGES = [
  'Request processed successfully',
  'Database connection established',
  'Cache hit for key {key}',
  'User session started',
  'Background job completed',
  'Memory usage at {mem}%',
  'CPU utilization at {cpu}%',
  'Disk I/O wait: {io}ms',
  'Connection pool: {used}/{max} connections',
  'Health check passed',
  'Metrics exported successfully',
  'Log rotation completed',
  'Configuration reloaded',
  'Worker thread {id} active',
  'Queue depth: {depth} messages'
];

// High-severity messages for tripwire testing
const HIGH_SEVERITY_MESSAGES = [
  'No space left on device',
  'Container OOMKilled',
  'Pod entered CrashLoopBackOff state',
  'Database connection pool reaching limits',
  'Failed to flush transaction log to disk',
  'PANIC: unrecoverable error',
  'Critical failure in main loop'
];

function getRandomLevel() {
  const totalWeight = LEVELS.reduce((sum, l) => sum + l.weight, 0);
  let rand = Math.random() * totalWeight;
  for (const level of LEVELS) {
    if (rand < level.weight) return level.level;
    rand -= level.weight;
  }
  return 'INFO';
}

function getRandomMessage() {
  // 15% chance of high-severity message
  if (Math.random() < 0.15) {
    return HIGH_SEVERITY_MESSAGES[Math.floor(Math.random() * HIGH_SEVERITY_MESSAGES.length)];
  }
  
  const msg = MESSAGES[Math.floor(Math.random() * MESSAGES.length)];
  
  // Replace placeholders
  return msg.replace('{key}', uuidv4().substring(0, 8))
            .replace('{mem}', Math.floor(Math.random() * 100))
            .replace('{cpu}', Math.floor(Math.random() * 100))
            .replace('{io}', Math.floor(Math.random() * 500))
            .replace('{used}', Math.floor(Math.random() * 100))
            .replace('{max}', 100)
            .replace('{id}', Math.floor(Math.random() * 10))
            .replace('{depth}', Math.floor(Math.random() * 1000));
}

function generateLogLine() {
  const timestamp = new Date();
  const hours = String(timestamp.getHours()).padStart(2, '0');
  const minutes = String(timestamp.getMinutes()).padStart(2, '0');
  const seconds = String(timestamp.getSeconds()).padStart(2, '0');
  const level = getRandomLevel();
  const message = getRandomMessage();
  
  return `${hours}:${minutes}:${seconds} - ${level} - ${message}`;
}

// Generate logs continuously
setInterval(() => {
  console.log(generateLogLine());
}, 100); // 10 logs per second
