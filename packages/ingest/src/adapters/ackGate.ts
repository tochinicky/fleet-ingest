/**
 * Holds every QoS 1 PUBACK until the message it acknowledges is safely stored, then sends the PUBACKs in arrival order.
 * This is "acknowledge after commit" (D3), and it's what turns a slow database into the broker queueing for us.
 *
 * Why it patches mqtt.js (slice 0, decisions.md S0-1): with the documented `customHandleAcks` hook, mqtt.js reads
 * the next incoming packet only after the current PUBACK has been written. Holding that callback until a commit lets
 * in one message at a time, and also stalls keepalive. So the gate answers the callback at once, swallows the PUBACK
 * that mqtt.js then tries to write through its private `_sendPacket`, and writes its own PUBACK when the message is
 * released.
 * Risk: `_sendPacket` is private. mqtt.js is pinned to exactly 5.16.0, and the canary integration test fails if an
 * upgrade changes this behaviour.
 * Exit path: replace this adapter with our own subscriber built on `mqtt-packet`, behind the same interface.
 */
import type { MqttClient, Packet } from "mqtt";
import { AckTracker } from "../core/ackTracker.ts";

/** A claim to acknowledge one message, valid only on the connection it arrived on. */
export interface AckToken {
  readonly generation: number;
  readonly position: number;
  readonly messageId: number;
}

/** The private writer the gate wraps. */
type SendPacket = (packet: Packet, callback?: () => void) => void;

/** What the gate reports when it throws a stale acknowledgement away, so the caller can log and count it. */
export type StaleAckListener = (token: AckToken) => void;

/**
 * Wraps one mqtt.js client. Every message is registered on arrival and released after it's stored.
 * Every new connection starts a new **generation**. Acks still held from the previous generation are discarded and
 * never written on the new connection. After a session resumes, the broker may reuse a packet ID for a different
 * message, so a stale PUBACK could acknowledge a message that was never stored: silent loss. Discarding is safe,
 * because the broker redelivers those messages (DUP=1) and the idempotent insert absorbs them.
 */
export class AckGate {
  #generation = 0;
  #tracker = new AckTracker<AckToken>();
  readonly #send: SendPacket;
  readonly #onStale: StaleAckListener;

  /** Installs the gate on `client`. Must run before the client connects, so no PUBACK can slip past it. */
  constructor(client: MqttClient, onStale: StaleAckListener = () => {}) {
    const target = client as unknown as { _sendPacket: SendPacket };
    const original = target._sendPacket.bind(client);
    this.#send = original;
    this.#onStale = onStale;
    target._sendPacket = (packet, callback) => {
      if (packet.cmd === "puback") {
        callback?.();
        return;
      }
      original(packet, callback);
    };
    client.on("connect", () => this.#startGeneration());
    client.on("close", () => this.#startGeneration());
  }

  /** Registers a message the moment it arrives, fixing its place in the acknowledgement order. */
  register(messageId: number): AckToken {
    return { generation: this.#generation, position: this.#tracker.track(), messageId };
  }

  /**
   * Marks the message as stored. Its PUBACK, and those of any later messages already stored, are written as soon as
   * every earlier message on this connection is stored too. A token from an earlier connection is discarded.
   */
  release(token: AckToken): void {
    if (this.isStale(token)) {
      this.#onStale(token);
      return;
    }
    for (const ready of this.#tracker.complete(token.position, token)) {
      this.#send({ cmd: "puback", messageId: ready.messageId, reasonCode: 0 } as Packet);
    }
  }

  /** Number of messages on the current connection that haven't been acknowledged yet. */
  outstanding(): number {
    return this.#tracker.outstanding();
  }

  /** True if the token belongs to a connection that has since closed. The connection-generation rule lives here. */
  protected isStale(token: AckToken): boolean {
    return token.generation !== this.#generation;
  }

  /** Forgets everything held for the old connection and starts counting the new one from position 0. */
  #startGeneration(): void {
    this.#generation++;
    this.#tracker = new AckTracker<AckToken>();
  }
}
