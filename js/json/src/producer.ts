import type * as Moq from "@moq/net";
import { Time } from "@moq/net";
import type * as z from "zod/mini";

import { Encoder } from "./compression.ts";
import { deepEqual, diff } from "./diff.ts";

// Maximum frames (snapshot + deltas) in a single group before a new snapshot is forced. Kept
// well below the per-group frame cap so a late joiner can always read the snapshot at frame 0.
const MAX_DELTA_FRAMES = 256;

// Delta ratio used when {@link Config.deltaRatio} is left unset.
const DEFAULT_DELTA_RATIO = 8;

export interface Config<T> {
	// Controls how aggressively the producer emits deltas (merge patches) instead of full snapshots.
	//
	// `0` disables deltas: every change is published as a new snapshot group.
	//
	// A positive number enables deltas: a delta is appended to the current group as long as the
	// accumulated deltas (excluding the snapshot frame) stay within `deltaRatio` times the size of a
	// fresh snapshot; otherwise a new snapshot group is started. So `1` allows deltas totalling up to
	// one snapshot before rolling.
	//
	// Defaults to `8` when unset.
	deltaRatio?: number;

	// Optional zod schema used to validate each value before publishing.
	schema?: z.ZodMiniType<T>;

	// Starting value for {@link Producer.mutate} before anything has been published. Required to
	// mutate a producer that hasn't published yet (e.g. a fresh catalog); ignored once a value exists.
	initial?: T;

	// Compress each group as one sync-flushed `deflate-raw` (RFC 1951) stream, so deltas reuse the
	// snapshot as context and shrink sharply. Interoperable with the Rust `moq-json` producer.
	// `false`/unset (the default) writes plaintext JSON frames. A {@link Consumer} reading the track
	// must set the same flag.
	compression?: boolean;
}

/** Publishes a JSON value over a track, choosing snapshots and deltas automatically. */
export class Producer<T> {
	#track: Moq.TrackProducer;
	#config: Config<T>;

	#group?: Moq.Group;
	#last?: unknown;
	// Bytes of deltas accumulated in the current group, excluding the snapshot frame. Always raw
	// (uncompressed) sizes, even when compressing: the delta-vs-snapshot decision measures raw bytes,
	// so a compressed producer rolls groups on raw sizes (still valid on the wire, just a touch sooner
	// than the Rust producer, which measures compressed sizes).
	#deltaBytes = 0;
	#groupFrames = 0;

	// Group-scoped `deflate-raw` compression. `#encoder` is the current group's stream, swapped for a
	// fresh one (cold window) at each snapshot, so a snapshot and its deltas share one DEFLATE stream.
	#compress = false;
	#encoder?: Encoder;

	constructor(track: Moq.TrackProducer, config: Config<T> = {}) {
		this.#track = track;
		this.#config = config;
		this.#compress = config.compression ?? false;
	}

	/** Publish a new value, emitting a snapshot or delta automatically. No-op if unchanged. */
	update(value: T): void {
		const valid = this.#config.schema ? this.#config.schema.parse(value) : value;

		// Serialize once; parse it back to a normalized JSON value for diffing and comparison
		// (dropping `undefined` fields, matching what lands on the wire).
		const text = JSON.stringify(valid);
		const json = JSON.parse(text);
		if (this.#last !== undefined && deepEqual(this.#last, json)) return;

		const snapshot = new TextEncoder().encode(text);
		const delta = this.#delta(json, snapshot.length);
		if (delta && this.#group) {
			this.#writeDelta(this.#group, delta);
			this.#deltaBytes += delta.length;
			this.#groupFrames += 1;
		} else {
			this.#snapshot(snapshot);
		}

		this.#last = json;
	}

	/**
	 * Mutate the current value in place and publish the result.
	 *
	 * The callback receives a deep clone of the last-published value, falling back to
	 * {@link Config.initial} if nothing has been published yet (throws if neither exists). Edit it in
	 * place; on return the result is published via {@link update}, a no-op if unchanged:
	 *
	 * ```ts
	 * producer.mutate((catalog) => {
	 * 	catalog.scte35 = { ... };
	 * });
	 * ```
	 *
	 * Independent owners can share a single Producer and each edit only their own keys: every call
	 * starts from the latest value, so sections compose instead of clobbering one another. Use
	 * {@link update} to replace the whole value instead.
	 */
	mutate(fn: (value: T) => void): void {
		// Start from the last-published value, falling back to the configured initial value. We
		// don't invent an empty object: mutating with nothing to start from is a usage error.
		const base = this.#last ?? this.#config.initial;
		if (base === undefined) {
			throw new Error("mutate() requires a prior update() or `initial` in the config");
		}

		const value = structuredClone(base) as T;
		fn(value);
		this.update(value);
	}

	/** Finish the track, closing any open group. */
	finish(): void {
		this.#group?.close();
		this.#group = undefined;
		this.#track.close();
	}

	// Resolved delta ratio: the configured value, or the default when unset. `0` disables deltas.
	get #deltaRatio(): number {
		return this.#config.deltaRatio ?? DEFAULT_DELTA_RATIO;
	}

	#delta(json: unknown, snapshotLen: number): Uint8Array | undefined {
		const ratio = this.#deltaRatio;
		if (ratio === 0) return undefined;
		if (this.#last === undefined) return undefined;
		if (!this.#group || this.#groupFrames >= MAX_DELTA_FRAMES) return undefined;

		const result = diff(this.#last, json);
		if (result.forcedSnapshot) return undefined;

		const delta = new TextEncoder().encode(JSON.stringify(result.patch));

		// Roll a snapshot once the deltas would outgrow the budget (snapshot frame excluded).
		if (this.#deltaBytes + delta.length > ratio * snapshotLen) return undefined;

		return delta;
	}

	#snapshot(snapshot: Uint8Array): void {
		// The previous group is complete; no more frames will be appended to it.
		this.#group?.close();

		const group = this.#track.appendGroup();
		this.#writeSnapshot(group, snapshot);
		this.#deltaBytes = 0;
		this.#groupFrames = 1;

		if (this.#deltaRatio !== 0) {
			// Keep the group open so future deltas can be appended.
			this.#group = group;
		} else {
			// Deltas disabled: one frame per group, identical to a plain JSON track.
			group.close();
			this.#group = undefined;
		}
	}

	// Write a group's snapshot (frame 0). On the compressed path this opens a fresh per-group encoder
	// (cold window), so the snapshot and its deltas share one DEFLATE stream.
	#writeSnapshot(group: Moq.Group, frame: Uint8Array): void {
		let data = frame;
		if (this.#compress) {
			this.#encoder = new Encoder();
			data = this.#encoder.frame(frame);
		}
		group.writeFrame({ data, timestamp: Time.Timestamp.now() });
	}

	// Write a delta frame, compressed against the current group's encoder when compressing.
	#writeDelta(group: Moq.Group, frame: Uint8Array): void {
		let data = frame;
		if (this.#compress) {
			if (!this.#encoder) throw new Error("compressed delta requires an open group");
			data = this.#encoder.frame(frame);
		}
		group.writeFrame({ data, timestamp: Time.Timestamp.now() });
	}
}
