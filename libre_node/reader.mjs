// libre_node/reader.mjs
// -----------------------------------------------------------------------------
// Passerelle entre Python et l'API LibreLinkUp.
// - Utilise readRaw() de @diakem/libre-link-up-api-client
// - Récupère graphData (historique) + glucoseMeasurement (dernier point)
// - Renvoie une liste de points : [{ ts, mgdl, trend }, ...],
//   triée par ts croissant, avec éventuellement le point live ajouté à la fin.
// -----------------------------------------------------------------------------
import { LibreLinkUpClient } from '@diakem/libre-link-up-api-client';

const username = process.env.LIBRE_EMAIL;
const password = process.env.LIBRE_PASSWORD;
const clientVersion = process.env.LIBRE_CLIENT_VERSION || '4.16.0';

if (!username || !password) {
  console.error('Missing LIBRE_EMAIL / LIBRE_PASSWORD');
  process.exit(1);
}

try {
  const { readRaw } = LibreLinkUpClient({ username, password, clientVersion });

  // 1️⃣ Appel /graph via readRaw()
  const raw = await readRaw();

  const connection = raw?.connection || raw?.data?.connection || null;
  const graphData = raw?.graphData || raw?.data?.graphData || [];

  // 2️⃣ Historique : mapping de graphData -> points
  const historyPoints = graphData
    .map((g) => {
      const tsRaw =
        g.Timestamp ??
        g.timestamp ??
        g.Date ??
        g.date;

      if (!tsRaw) return null;

      const ts = new Date(tsRaw);
      if (Number.isNaN(ts.getTime())) return null;

      const mgdl =
        g.ValueInMgPerDl ??
        g.value ??
        g.Value ??
        g.glucoseValue;

      return {
        ts: ts.toISOString(),
        mgdl: Number(mgdl),
        trend: g.TrendArrow ?? g.trend ?? g.trendArrow ?? null,
      };
    })
    .filter(Boolean);

  // 3️⃣ Dernière mesure : glucoseMeasurement / glucoseItem
  let livePoint = null;
  if (connection) {
    const gm =
      connection.glucoseMeasurement ||
      connection.glucoseItem ||
      connection.GlucoseMeasurement ||
      connection.GlucoseItem;

    if (gm) {
      const gmTsRaw =
        gm.Timestamp ??
        gm.timestamp ??
        gm.Date ??
        gm.date;

      if (gmTsRaw) {
        const gmTs = new Date(gmTsRaw);
        if (!Number.isNaN(gmTs.getTime())) {
          livePoint = {
            ts: gmTs.toISOString(),
            mgdl: Number(
              gm.ValueInMgPerDl ??
              gm.value ??
              gm.Value ??
              gm.glucoseValue
            ),
            trend: gm.TrendArrow ?? gm.trend ?? gm.trendArrow ?? null,
          };
        }
      }
    }
  }

  const allPoints = [...historyPoints];

  // 4️⃣ On n'ajoute le live que s'il est plus récent que le dernier point historique
  if (livePoint) {
    const lastHist = historyPoints[historyPoints.length - 1];

    if (!lastHist) {
      // Aucun historique -> au moins le live
      allPoints.push(livePoint);
    } else {
      const liveTs = new Date(livePoint.ts).getTime();
      const lastTs = new Date(lastHist.ts).getTime();

      if (liveTs > lastTs) {
        allPoints.push(livePoint);
      }
    }
  }

  // 5️⃣ Tri temporel croissant (sécurité)
  allPoints.sort((a, b) => new Date(a.ts) - new Date(b.ts));

  process.stdout.write(JSON.stringify(allPoints));
} catch (e) {
  console.error('Erreur LibreLinkUp:', e?.message || String(e));
  if (e?.response) {
    console.error('Status:', e.response.status);
    console.error('Response data:', JSON.stringify(e.response.data, null, 2));
  }
  process.exit(1);
}
