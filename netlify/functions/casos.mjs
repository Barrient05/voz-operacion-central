import pg from 'pg';
import { timingSafeEqual } from 'node:crypto';

const { Pool } = pg;
let pool;
let schemaReady;

function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'Content-Type': 'application/json; charset=utf-8',
      'Cache-Control': 'no-store'
    }
  });
}

function authorized(request) {
  const expected = String(process.env.VOZ_SYNC_TOKEN || '');
  const header = request.headers.get('authorization') || '';
  const supplied = header.startsWith('Bearer ') ? header.slice(7) : '';
  if (!expected || !supplied) return false;
  const a = Buffer.from(expected);
  const b = Buffer.from(supplied);
  return a.length === b.length && timingSafeEqual(a, b);
}

function getPool() {
  if (!process.env.DATABASE_URL) throw new Error('Falta configurar DATABASE_URL en Netlify.');
  if (!pool) {
    pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      // Recomendado para proveedores PostgreSQL administrados con TLS.
      ssl: process.env.PGSSLMODE === 'disable' ? false : { rejectUnauthorized: false },
      max: 5,
      idleTimeoutMillis: 10000,
      connectionTimeoutMillis: 10000
    });
  }
  return pool;
}

async function ensureSchema(db) {
  if (!schemaReady) {
    schemaReady = db.query(`
      CREATE TABLE IF NOT EXISTS voz_operacion_cases (
        id TEXT PRIMARY KEY,
        data JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS voz_operacion_cases_created_at_idx
        ON voz_operacion_cases (created_at DESC);
    `).catch(err => { schemaReady = null; throw err; });
  }
  return schemaReady;
}

export default async (request) => {
  if (request.method !== 'GET' && request.method !== 'POST') {
    return json({ ok: false, error: 'Método no permitido.' }, 405);
  }
  if (!authorized(request)) {
    return json({ ok: false, error: 'No autorizado. Revisa la clave de sincronización.' }, 401);
  }

  try {
    const db = getPool();
    await ensureSchema(db);

    if (request.method === 'GET') {
      const url = new URL(request.url);
      const limit = Math.min(250, Math.max(1, Number.parseInt(url.searchParams.get('limit') || '250', 10) || 250));
      const offset = Math.max(0, Number.parseInt(url.searchParams.get('offset') || '0', 10) || 0);
      const [countResult, rowsResult] = await Promise.all([
        db.query('SELECT COUNT(*)::int AS total FROM voz_operacion_cases'),
        db.query(
          'SELECT data FROM voz_operacion_cases ORDER BY created_at ASC, id ASC LIMIT $1 OFFSET $2',
          [limit, offset]
        )
      ]);
      const cases = rowsResult.rows.map(row => row.data).filter(value => value && typeof value === 'object');
      return json({ ok: true, total: countResult.rows[0]?.total || 0, cases });
    }

    const contentLength = Number(request.headers.get('content-length') || 0);
    if (contentLength > 5_000_000) return json({ ok: false, error: 'La solicitud supera el límite de 5 MB.' }, 413);
    let body;
    try { body = await request.json(); }
    catch { return json({ ok: false, error: 'El cuerpo de la solicitud no contiene JSON válido.' }, 400); }

    const incoming = Array.isArray(body?.cases) ? body.cases : (body?.record ? [body.record] : []);
    if (!incoming.length) return json({ ok: false, error: 'No se recibieron casos para guardar.' }, 400);
    if (incoming.length > 200) return json({ ok: false, error: 'Envía como máximo 200 casos por lote.' }, 413);

    // Deduplicación en el lote y en PostgreSQL: el primer registro con cada folio se conserva.
    const seen = new Set();
    const records = [];
    let repeatedInRequest = 0;
    for (const raw of incoming) {
      if (!raw || typeof raw !== 'object' || Array.isArray(raw)) continue;
      const id = String(raw.id || '').trim();
      if (!id || id.length > 200) continue;
      if (seen.has(id)) { repeatedInRequest++; continue; }
      seen.add(id);
      records.push({ id, data: { ...raw, id } });
    }
    if (!records.length) return json({ ok: false, error: 'No hay registros válidos con folio (id).' }, 400);

    const client = await db.connect();
    let inserted = 0;
    try {
      await client.query('BEGIN');
      for (const record of records) {
        const result = await client.query(
          'INSERT INTO voz_operacion_cases (id, data) VALUES ($1, $2::jsonb) ON CONFLICT (id) DO NOTHING RETURNING id',
          [record.id, JSON.stringify(record.data)]
        );
        if (result.rowCount) inserted++;
      }
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
    const duplicates = incoming.length - inserted;
    return json({ ok: true, received: incoming.length, inserted, duplicates, repeatedInRequest });
  } catch (error) {
    console.error('Error en función central Voz de la Operación:', error);
    return json({ ok: false, error: error?.message || 'Error interno al conectar con PostgreSQL.' }, 500);
  }
};
