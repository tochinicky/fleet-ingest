/**
 * Zod schemas for the three message types on the wire, matching what `home-energy-gateway` serialises
 * (`TelemetryMessages.cs`: camelCase, nulls omitted, enums as strings, `DateTimeOffset` as ISO 8601 with an offset).
 * Every consumer validates against these, so the contract lives in exactly one place.
 */
import { z } from "zod";

/** The four device kinds the gateway knows (`DeviceKind.cs`). */
export const DEVICE_KINDS = ["Inverter", "Battery", "Charger", "GridMeter"] as const;

/** The charging modes the cloud can ask a gateway for. */
export const GATEWAY_MODES = ["surplus", "max", "off"] as const;

/**
 * An ISO 8601 timestamp with an explicit offset, such as `2026-10-14T11:07:52.4362141+00:00`. .NET writes seven
 * fractional digits, so the precision is left open.
 */
const timestampWithOffset = z.iso.datetime({ offset: true });

/** A non-negative safe integer: the type of epoch, seq and the gateway's buffer counters. */
const counter = z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER);

/**
 * One reading from one device. Measurements the device doesn't report are left out, never sent as null.
 * Signs: grid import is positive, battery charging is positive, EV charging is positive.
 * Unknown extra fields are stripped rather than rejected, so a newer gateway can add a field without its readings
 * being dead-lettered.
 */
export const telemetryMessageSchema = z.object({
  deviceId: z.string().min(1).max(128),
  epoch: counter,
  seq: counter,
  timestamp: timestampWithOffset,
  kind: z.enum(DEVICE_KINDS),
  pvPowerW: z.number().optional(),
  batteryPowerW: z.number().optional(),
  gridPowerW: z.number().optional(),
  evPowerW: z.number().optional(),
  energyWh: z.number().optional(),
  socPercent: z.number().min(0).max(100).optional(),
  chargeLimitA: z.number().nonnegative().optional(),
});

/**
 * A gateway's retained status. `online` is sent on connect and every 60 s with the buffer counters; `offline` is the
 * Last Will and carries nothing else. `mode` is a plain string here: a mode this cloud doesn't know yet must not
 * cause a status to be dead-lettered.
 */
export const gatewayStatusSchema = z.object({
  state: z.enum(["online", "offline"]),
  timestamp: timestampWithOffset.optional(),
  bufferedTelemetry: counter.optional(),
  droppedTelemetry: counter.optional(),
  mode: z.string().optional(),
});

/** The desired mode the cloud publishes, retained, for one gateway. */
export const gatewayCommandSchema = z.object({
  mode: z.enum(GATEWAY_MODES),
});

/** A validated reading, as the schema returns it. */
export type TelemetryMessage = z.infer<typeof telemetryMessageSchema>;

/** A validated gateway status. */
export type GatewayStatus = z.infer<typeof gatewayStatusSchema>;

/** A validated command. */
export type GatewayCommand = z.infer<typeof gatewayCommandSchema>;

/** One of the four device kinds. */
export type DeviceKind = (typeof DEVICE_KINDS)[number];

/** One of the three charging modes. */
export type GatewayMode = (typeof GATEWAY_MODES)[number];
