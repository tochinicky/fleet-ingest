/**
 * A virtual gateway's outbox, with the same rules as the real gateway's SQLite buffer (`LocalStore`, 50,000 messages):
 * readings wait here until the broker acknowledges them, they're sent oldest first, and when the buffer is full
 * the oldest are dropped and counted. The count is reported to the cloud as `droppedTelemetry`.
 */

/**
 * A bounded first-in, first-out queue that drops the oldest entry when full.
 * Messages being sent are taken off the front and put back with `returnToFront` if the connection drops, so a
 * replay after reconnecting keeps the original order.
 */
export class TelemetryBuffer<T> {
  #items: T[] = [];
  #head = 0;
  #dropped = 0;
  readonly #capacity: number;

  /** Creates an empty buffer that holds at most `capacity` messages. */
  constructor(capacity: number) {
    if (!Number.isInteger(capacity) || capacity < 1) throw new Error("capacity must be a positive integer");
    this.#capacity = capacity;
  }

  /** Adds a message at the back; if that makes the buffer overflow, the oldest message is dropped and counted. */
  push(item: T): void {
    this.#items.push(item);
    if (this.size() > this.#capacity) {
      this.#head++;
      this.#dropped++;
    }
    this.#compactIfWasteful();
  }

  /** Removes and returns the oldest message, or undefined when empty. */
  shift(): T | undefined {
    if (this.size() === 0) return undefined;
    const item = this.#items[this.#head] as T;
    this.#head++;
    this.#compactIfWasteful();
    return item;
  }

  /** Puts messages that were taken for sending back at the front, in their original order. */
  returnToFront(items: T[]): void {
    this.#items = [...items, ...this.#items.slice(this.#head)];
    this.#head = 0;
  }

  /** How many messages are waiting. */
  size(): number {
    return this.#items.length - this.#head;
  }

  /** How many messages were dropped because the buffer was full, since it was created. */
  dropped(): number {
    return this.#dropped;
  }

  /** Reclaims the array space in front of the head once it's more than half the array, so memory stays bounded. */
  #compactIfWasteful(): void {
    if (this.#head > 1024 && this.#head > this.#items.length / 2) {
      this.#items = this.#items.slice(this.#head);
      this.#head = 0;
    }
  }
}
