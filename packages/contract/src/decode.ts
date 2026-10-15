/**
 * Turns raw MQTT payload bytes into a validated message, or a reason it was rejected. This is the single entry point
 * for untrusted bytes, so every consumer rejects the same things for the same reasons.
 */
import type { z } from "zod";
import { gatewayStatusSchema, telemetryMessageSchema } from "./messages.ts";
import type { GatewayStatus, TelemetryMessage } from "./messages.ts";

/** Why a payload was rejected. These strings end up in `dead_letters.reason`, so they're stable identifiers. */
export type DecodeFailure = "invalid_json" | "schema_mismatch";

/** Either the decoded value, or the reason and a human-readable detail. */
export type Decoded<T> = { ok: true; value: T } | { ok: false; reason: DecodeFailure; detail: string };

/** Parses the payload as UTF-8 JSON and validates it against `schema`. Never throws. */
function decodeWith<T>(schema: z.ZodType<T>, payload: Uint8Array): Decoded<T> {
  let json: unknown;
  try {
    json = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(payload));
  } catch (error) {
    return { ok: false, reason: "invalid_json", detail: error instanceof Error ? error.message : String(error) };
  }
  const result = schema.safeParse(json);
  if (!result.success) {
    const detail = result.error.issues.map((issue) => `${issue.path.join(".") || "(root)"}: ${issue.message}`).join("; ");
    return { ok: false, reason: "schema_mismatch", detail };
  }
  return { ok: true, value: result.data };
}

/** Decodes a telemetry payload. */
export function decodeTelemetry(payload: Uint8Array): Decoded<TelemetryMessage> {
  return decodeWith(telemetryMessageSchema, payload);
}

/** Decodes a gateway status payload. */
export function decodeStatus(payload: Uint8Array): Decoded<GatewayStatus> {
  return decodeWith(gatewayStatusSchema, payload);
}
