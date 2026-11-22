import 'dotenv/config';
import pg from 'pg';

const { Pool } = pg;

const databaseUrl = process.env.AUDIT_DATABASE_URL;
if (!databaseUrl) {
  throw new Error('AUDIT_DATABASE_URL é obrigatório.');
}

export const auditPool = new Pool({
  connectionString: databaseUrl,
  max: Number(process.env.AUDIT_DB_POOL_SIZE ?? 10),
});
