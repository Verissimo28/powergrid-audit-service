import 'dotenv/config';
import Fastify from 'fastify';
import { auditPool } from './database/pool.js';
import { AuditConsumer } from './messaging/audit-consumer.js';

const port = Number(process.env.AUDIT_HTTP_PORT ?? 3003);
const rabbitUrl = process.env.RABBITMQ_URL;

if (!rabbitUrl) {
  throw new Error('RABBITMQ_URL é obrigatório.');
}

const app = Fastify({ logger: true });
const consumer = new AuditConsumer(rabbitUrl);

app.get('/health', async () => ({
  status: 'ok',
  service: 'audit-service',
  version: '0.1.0',
}));

const shutdown = async (signal: string): Promise<void> => {
  app.log.info({ signal }, 'encerrando audit-service');
  await app.close();
  await consumer.close();
  await auditPool.end();
  process.exit(0);
};

process.on('SIGINT', () => void shutdown('SIGINT'));
process.on('SIGTERM', () => void shutdown('SIGTERM'));

try {
  await auditPool.query('SELECT 1');
  await consumer.start();
  await app.listen({ host: '0.0.0.0', port });
} catch (error) {
  app.log.error(error);
  await consumer.close();
  await auditPool.end();
  process.exit(1);
}
