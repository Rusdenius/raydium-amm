import { Decimal } from "decimal.js";
import { upsertPool, poolsForPair, poolById } from "../../pools/poolIndex.js";
import { safeConn } from "../../infra/rpc.js";
import { logger } from "../../utils/logger.js";
import { PublicKey } from "@solana/web3.js";
import { AccountLayout } from "@solana/spl-token";
import { getRaydiumPools, RaydiumPoolInfo } from "../../utils/raydiumFetcher.js";
import WebSocket from "ws";
import { ENV } from "../../config/env.js";

export interface QuoteResult {
    amountOut: Decimal;       // после сборов пула
    feeBps: number;           // фи пула
    virtualPriceImpactBps?: number;
    poolId?: string;
    dex?: "orca" | "raydium" | "meteora";
}

interface SubscribeInfo {
    poolId: string;
    side: "A" | "B";
    timeout?: NodeJS.Timeout;
}

export class RaydiumAmmAdapter {
    dex: "raydium" = "raydium";
    private static readonly FIRST_UPDATES = new Set<string>();
    private ws: WebSocket | null = null;

    private readonly commitment: "processed" | "confirmed" | "finalized";
    private nextId = 1;
    private pending = new Map<number, SubscribeInfo>();
    private subs = new Map<number, SubscribeInfo>();
    private targets: Array<{ acc: string; poolId: string; side: "A" | "B" }> = [];
    private reconnectDelay = 1000;

    constructor(commitment: "processed" | "confirmed" | "finalized" = "confirmed") {
        this.commitment = commitment;
    }

    private async connect() {
        const httpRpc = ENV.RPC_URLS_POOLS[0];
        const url = new URL(httpRpc);
        url.protocol = url.protocol === "https:" ? "wss:" : "ws:";

        const ws = new WebSocket(url.toString());
        this.ws = ws;

        ws.on("message", (data) => this.onMessage(data));
        ws.on("error", (err) => {
            logger.error({ dex: "raydium", err: (err as any)?.message }, "ws:pool:error");
            ws.close();
        });
        ws.on("close", () => {
            logger.warn({ dex: "raydium" }, "ws:pool:close");
            this.reconnect();
        });

        await new Promise<void>((resolve) => ws.once("open", resolve));
        this.reconnectDelay = 1000;
        this.resubscribeAll();
    }

    private reconnect() {
        setTimeout(() => this.connect(), this.reconnectDelay);
        this.reconnectDelay = Math.min(this.reconnectDelay * 2, 30000);
    }

    private resubscribeAll() {
        for (const t of this.targets) {
            this.subscribeAccount(t.acc, t.poolId, t.side);
        }
    }

    private onMessage(data: WebSocket.RawData) {
        try {
            const payload = JSON.parse(data.toString());
            if (payload.id !== undefined && payload.result !== undefined) {
                const info = this.pending.get(payload.id);
                if (info) {
                    if (info.timeout) clearTimeout(info.timeout);
                    this.subs.set(payload.result, info);
                    this.pending.delete(payload.id);
                    logger.info({ dex: "raydium", poolId: info.poolId, side: info.side }, "ws:pool:subscribed");
                }
                return;
            }
            if (payload.method === "accountNotification") {
                const sub = this.subs.get(payload.params.subscription);
                if (!sub) return;
                const dataB64 = payload.params.result.value.data[0];
                const buffer = Buffer.from(dataB64, "base64");
                const decoded = AccountLayout.decode(buffer);
                const amount = BigInt(decoded.amount.toString());
                const first = !RaydiumAmmAdapter.FIRST_UPDATES.has(sub.poolId);
                if (first) RaydiumAmmAdapter.FIRST_UPDATES.add(sub.poolId);
                logger[first ? "info" : "trace"](
                    { dex: "raydium", poolId: sub.poolId, side: sub.side, reserve: amount.toString() },
                    "ws:pool:update",
                );
                const snap = poolById.get(sub.poolId);
                if (!snap) return;
                if (sub.side === "A") snap.reserveA = amount; else snap.reserveB = amount;
                snap.lastUpdateTs = Date.now();
                poolById.set(sub.poolId, snap);
            }
        } catch (e: any) {
            logger.warn({ err: e.message }, "Raydium: ws message parse failed");
        }
    }

    private subscribeAccount(acc: string, poolId: string, side: "A" | "B") {
        if (!this.targets.find((t) => t.acc === acc)) {
            this.targets.push({ acc, poolId, side });
        }
        const ws = this.ws;
        if (!ws) return;
        const id = this.nextId++;
        const timeout = setTimeout(() => {
            this.pending.delete(id);
            logger.warn({ dex: "raydium", poolId, side }, "ws:pool:subscribe-timeout");
        }, 10000);
        this.pending.set(id, { poolId, side, timeout });
        ws.send(
            JSON.stringify({
                jsonrpc: "2.0",
                id,
                method: "accountSubscribe",
                params: [acc, { encoding: "base64", commitment: this.commitment }],
            }),
        );
    }

    async bootstrap() {
        const pools: RaydiumPoolInfo[] = getRaydiumPools();
        logger.info({ total: pools.length }, "Raydium: bootstrap start");
        if (pools.length === 0) {
            logger.warn("Raydium: список пулов пуст — добавь статический JSON или генератор списка.");
            return;
        }

        await this.connect();

        for (const p of pools) {
            const feeBps = p.feeNumerator !== undefined && p.feeDenominator !== undefined
                ? Math.floor((p.feeNumerator * 10_000) / p.feeDenominator)
                : Math.floor((p.feeRate ?? 0) * 10_000);
            logger.debug({ id: p.id, mints: [p.tokenA.id, p.tokenB.id], feeBps }, "Raydium: processing pool");
            let reserveA: bigint | undefined;
            let reserveB: bigint | undefined;

            if (p.vaultA && p.vaultB) {
                try {
                    await safeConn("pools", async (conn) => {
                        const infos = await conn.getMultipleAccountsInfo([
                            new PublicKey(p.vaultA!),
                            new PublicKey(p.vaultB!),
                        ]);
                        const infoA = infos[0];
                        const infoB = infos[1];
                        if (!infoA || !infoB) {
                            logger.warn({ id: p.id }, "Raydium: account lookup failed");
                            return;
                        }
                        const decodedA = AccountLayout.decode(infoA.data);
                        reserveA = BigInt(decodedA.amount.toString());
                        const decodedB = AccountLayout.decode(infoB.data);
                        reserveB = BigInt(decodedB.amount.toString());
                        RaydiumAmmAdapter.FIRST_UPDATES.add(p.id);
                        logger.info(
                            {
                                dex: "raydium",
                                poolId: p.id,
                                side: "both",
                                reserveA: reserveA.toString(),
                                reserveB: reserveB.toString(),
                            },
                            "ws:pool:update",
                        );
                        logger.info({ dex: "raydium", poolId: p.id }, "ws:pool:subscribing");
                        this.subscribeAccount(p.vaultA!, p.id, "A");
                        this.subscribeAccount(p.vaultB!, p.id, "B");
                    });
                } catch (err) {
                    logger.warn({ id: p.id, err }, "Raydium: account fetch failed");
                }
            } else if (p.mintAmountA !== undefined && p.mintAmountB !== undefined) {
                reserveA = BigInt(Math.floor(p.mintAmountA));
                reserveB = BigInt(Math.floor(p.mintAmountB));
            } else {
                logger.warn({ id: p.id }, "Raydium: no reserve info");
                continue; // skip pools without reserves info
            }

            upsertPool({
                dex: "raydium",
                id: p.id,
                mintA: p.tokenA.id,
                mintB: p.tokenB.id,
                feeBps,
                reserveA,
                reserveB,
                lastUpdateTs: Date.now(),
            });
        }

        logger.info(`Raydium: зарегистрировано пулов: ${pools.length}`);
    }

    quoteExactIn(mintIn: string, mintOut: string, amountIn: Decimal): QuoteResult | null {
        const pools = poolsForPair(mintIn, mintOut);
        if (pools.length === 0) return null;

        let best: QuoteResult | null = null;

        for (const pid of pools) {
            const s = poolById.get(pid);
            if (!s || s.reserveA === undefined || s.reserveB === undefined) continue;

            // Нормализуем направление
            const inIsA = s.mintA === mintIn;
            const R_in  = new Decimal(inIsA ? s.reserveA!.toString() : s.reserveB!.toString());
            const R_out = new Decimal(inIsA ? s.reserveB!.toString() : s.reserveA!.toString());

            const fee = new Decimal(1).minus(new Decimal(s.feeBps).div(10_000));
            const dx = amountIn.mul(fee);
            const k = R_in.mul(R_out);
            const newRin = R_in.add(dx);
            const newRout = k.div(newRin);
            const out = R_out.sub(newRout); // xy=k формула

            if (out.lte(0)) continue;

            const r: QuoteResult = {
                amountOut: out,
                feeBps: s.feeBps,
                poolId: s.id,
                dex: "raydium",
            };
            if (!best || r.amountOut.gt(best.amountOut)) best = r;
        }
        return best;
    }
}
