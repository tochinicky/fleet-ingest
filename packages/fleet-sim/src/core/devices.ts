/**
 * The devices behind one virtual gateway and the readings they produce. Pure: a reading is a function of the device,
 * the time, and a random source passed in, so tests can make it deterministic.
 * The values are plausible rather than physically exact. They exercise the contract, not the control loop.
 */
import type { DeviceKind, TelemetryMessage } from "@fleet-ingest/contract";

/** One simulated device: its ID and kind. */
export interface SimDevice {
  deviceId: string;
  kind: DeviceKind;
}

/** The order devices are added to a gateway: every home gets a meter, then an inverter, a battery and a charger. */
const KIND_ORDER: DeviceKind[] = ["GridMeter", "Inverter", "Battery", "Charger"];

/** Short codes used in fictional device IDs. */
const KIND_CODE: Record<DeviceKind, string> = { GridMeter: "mtr", Inverter: "inv", Battery: "bat", Charger: "evc" };

/** The devices of one gateway: `count` of them, kinds in {@link KIND_ORDER}, with IDs unique across the fleet. */
export function devicesFor(gatewayId: string, count: number): SimDevice[] {
  return Array.from({ length: count }, (_, i) => {
    const kind = KIND_ORDER[i % KIND_ORDER.length] as DeviceKind;
    return { deviceId: `${gatewayId}-${KIND_CODE[kind]}-${i + 1}`, kind };
  });
}

/** Solar output in watts at a time of day: zero at night, a sine-shaped day between 06:00 and 20:00 UTC. */
function solarW(at: Date, peakW: number): number {
  const hour = at.getUTCHours() + at.getUTCMinutes() / 60;
  if (hour < 6 || hour > 20) return 0;
  return Math.round(peakW * Math.sin(((hour - 6) / 14) * Math.PI));
}

/** Rounds to one decimal place, like the real devices report. */
function oneDecimal(value: number): number {
  return Math.round(value * 10) / 10;
}

/** The measurements one device reports at `at`, with the fields the real gateway sends for that kind. */
export function measure(device: SimDevice, at: Date, random: () => number): Omit<TelemetryMessage, "deviceId" | "epoch" | "seq" | "timestamp" | "kind"> {
  switch (device.kind) {
    case "Inverter":
      return { pvPowerW: solarW(at, 6000) + Math.round(random() * 100), energyWh: Math.round(at.getTime() / 3_600_000) * 10 };
    case "Battery":
      return { batteryPowerW: Math.round((random() - 0.5) * 5000), socPercent: oneDecimal(20 + random() * 75) };
    case "GridMeter":
      return { gridPowerW: Math.round((random() - 0.6) * 8000), energyWh: Math.round(at.getTime() / 3_600_000) };
    case "Charger":
      return { evPowerW: random() < 0.3 ? Math.round(random() * 11000) : 0, energyWh: 1_234_000, chargeLimitA: random() < 0.3 ? 16 : 0 };
  }
}

/**
 * A small seeded pseudo-random generator (mulberry32), so a simulated fleet run is repeatable for a given seed.
 */
export function seededRandom(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
