import { assertEquals } from "@std/assert";
import { createRelayFactory } from "@publicdomainrelay/hono-factory-atproto-relay-xrpc";

const DID = "did:plc:crawlreindextest0";
const RKEY = "3kabcrfprkey00";

function frame(seq: number, collection: string): Record<string, unknown> {
  return {
    $type: "com.atproto.sync.subscribeRepos#commit",
    seq,
    repo: DID,
    commit: { $link: "bafyreiboguscommit000000000000000000000000000000000000000" },
    rev: `3kabc${String(seq).padStart(3, "0")}rev0`,
    since: null,
    time: "2026-08-04T00:00:00.000Z",
    ops: [{ action: "create", path: `${collection}/${RKEY}`, cid: { $link: `bafyreirefid000000000000000000000000000000000000000${String(seq).padStart(3, "0")}` }, prev: null }],
    blocks: [] as unknown as Uint8Array,
  };
}

/**
 * Fake PDS: serves describeServer (identity) + subscribeRepos websocket that
 * REPLAYS all frames on every connection (models a restart / re-crawl). It does
 * NOT push frames to already-open sockets — so a re-requestCrawl must actually
 * reconnect + backfill to pick up newly appended records.
 */
async function serveFakePds(): Promise<{ port: number; stop(): Promise<void>; append(collection: string): void }> {
  const frames: Array<Record<string, unknown>> = [];
  const sockets = new Set<WebSocket>();
  const { promise: portReady, resolve } = Promise.withResolvers<number>();
  const server = Deno.serve(
    { port: 0, hostname: "127.0.0.1", onListen: (a) => resolve((a as Deno.NetAddr).port) },
    (req) => {
      const url = new URL(req.url);
      if (url.pathname === "/xrpc/com.atproto.server.describeServer") {
        return Response.json({ did: DID });
      }
      if (url.pathname === "/xrpc/com.atproto.sync.subscribeRepos") {
        const { socket, response } = Deno.upgradeWebSocket(req);
        sockets.add(socket);
        // Deno.upgradeWebSocket is not OPEN until the upgrade response is
        // returned — send the replay once the socket is actually open.
        socket.onopen = () => {
          for (const f of frames) socket.send(JSON.stringify(f));
        };
        socket.onclose = () => sockets.delete(socket);
        return response;
      }
      return new Response("not found", { status: 404 });
    },
  );
  const port = await portReady;
  return {
    port,
    async stop() {
      for (const s of sockets) s.close();
      sockets.clear();
      try { await server.shutdown(); } catch { /* already closed */ }
    },
    append(collection: string) {
      frames.push(frame(frames.length + 1, collection));
    },
  };
}

async function serveRelay(): Promise<{ port: number; stop(): Promise<void> }> {
  const tmpDir = await Deno.makeTempDir({ prefix: "relay-kv-" });
  const kv = await Deno.openKv(`${tmpDir}/kv`);
  const factory = createRelayFactory({ hostname: "localhost", kv, insecureHTTP: true });
  const { promise: portReady, resolve } = Promise.withResolvers<number>();
  const server = Deno.serve(
    { port: 0, hostname: "127.0.0.1", onListen: (a) => resolve((a as Deno.NetAddr).port) },
    factory.app.fetch,
  );
  const port = await portReady;
  return {
    port,
    async stop() {
      try { await server.shutdown(); } catch { /* already closed */ }
      kv.close();
      await Deno.remove(tmpDir, { recursive: true }).catch(() => {});
    },
  };
}

async function requestCrawl(relayPort: number, hostname: string): Promise<number> {
  const res = await fetch(`http://127.0.0.1:${relayPort}/xrpc/com.atproto.sync.requestCrawl`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ hostname }),
  });
  return res.status;
}

async function listRepos(relayPort: number, collection: string): Promise<string[]> {
  const res = await fetch(
    `http://127.0.0.1:${relayPort}/xrpc/com.atproto.sync.listReposByCollection?collection=${encodeURIComponent(collection)}&limit=1000`,
  );
  const body = await res.json() as { repos?: Array<{ did: string }> };
  return (body.repos ?? []).map((r) => r.did);
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

Deno.test("re-requestCrawl re-crawls and indexes records committed after the first crawl", async () => {
  const fake = await serveFakePds();
  const relay = await serveRelay();
  try {
    const pdsHost = `127.0.0.1:${fake.port}`;

    // Initial offering exists before the first requestCrawl.
    fake.append("com.publicdomainrelay.temp.market.offering");
    const firstStatus = await requestCrawl(relay.port, pdsHost);
    assertEquals(firstStatus, 200);
    await sleep(400);
    const offeringRepos = await listRepos(relay.port, "com.publicdomainrelay.temp.market.offering");
    assertEquals(offeringRepos.includes(DID), true, "offering indexed after first crawl");

    // A new record is committed AFTER the first crawl and delivered only by a
    // fresh crawl (the fake PDS does not push to open sockets).
    fake.append("com.publicdomainrelay.temp.market.event");
    await sleep(100);
    const before = await listRepos(relay.port, "com.publicdomainrelay.temp.market.event");
    assertEquals(before.includes(DID), false, "event not indexed without a re-crawl");

    // Re-requestCrawl must reset + backfill the new record.
    const secondStatus = await requestCrawl(relay.port, pdsHost);
    assertEquals(secondStatus, 200);
    await sleep(400);
    const eventRepos = await listRepos(relay.port, "com.publicdomainrelay.temp.market.event");
    assertEquals(eventRepos.includes(DID), true, "event indexed after re-requestCrawl");
  } finally {
    await relay.stop();
    await fake.stop();
  }
});
