import { MessageChannel, type MessagePort, receiveMessageOnPort } from "node:worker_threads";
import type { WorkerTaskPool } from "../infra/worker-task-pool.js";
import {
  observePluginNativeAdmissions,
  type PluginNativeAdmissionPhase,
} from "../plugins/plugin-native-admission-progress.js";
import type {
  PreparedModelCatalogWorkerTask,
  PreparedModelWorkerResult,
} from "./prepared-model-catalog-worker.js";
import { PreparedModelCatalogAdmissionStalledError } from "./prepared-model-catalog.errors.js";

export type PreparedModelCatalogAdmissionProgress = {
  completed: SharedArrayBuffer;
  phases: MessagePort;
};

/** A task owns this observer; callbacks retained by plugin code cannot revive it afterward. */
export async function observePreparedModelCatalogAdmission<T>(
  progress: PreparedModelCatalogAdmissionProgress | undefined,
  run: (stop: () => void) => Promise<T>,
): Promise<T> {
  if (!progress) {
    return run(() => {});
  }
  const completed = new Int32Array(progress.completed);
  let active = true;
  let previous: PluginNativeAdmissionPhase | undefined;
  const stop = () => {
    active = false;
    progress.phases.close();
  };
  try {
    return await observePluginNativeAdmissions(
      {
        phase(next) {
          if (active && (next.pluginId !== previous?.pluginId || next.stage !== previous.stage)) {
            previous = next;
            progress.phases.postMessage(next, []);
          }
        },
        advance() {
          if (active) {
            Atomics.add(completed, 0, 1);
          }
        },
      },
      () => run(stop),
    );
  } finally {
    stop();
  }
}

/** The existing catalog budget bounds idle preparation, not total progressing admission time. */
function createPreparedModelCatalogAdmissionWatchdog(
  idleMs: number,
  onStall: (error: PreparedModelCatalogAdmissionStalledError) => void,
) {
  const completed = new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT));
  const { port1, port2 } = new MessageChannel();
  let phase: PluginNativeAdmissionPhase | undefined;
  let observed = 0;
  let progressedAt = 0;
  let running = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const sample = () => {
    const count = Atomics.load(completed, 0);
    if (count !== observed) {
      observed = count;
      progressedAt = performance.now();
    }
  };
  const receivePhase = (next: PluginNativeAdmissionPhase) => {
    phase = next;
    sample();
  };
  port1.on("message", receivePhase);
  port1.unref();
  const stop = () => {
    running = false;
    clearTimeout(timer);
    port1.close();
    port2.close();
  };
  const check = () => {
    if (!running) {
      return;
    }
    // A synchronous filesystem call can block worker messages; its last phase is already queued.
    for (
      let message = receiveMessageOnPort(port1);
      message;
      message = receiveMessageOnPort(port1)
    ) {
      receivePhase(message.message);
    }
    sample();
    const idleFor = performance.now() - progressedAt;
    if (idleFor >= idleMs) {
      stop();
      onStall(
        new PreparedModelCatalogAdmissionStalledError(
          phase?.pluginId,
          phase?.stage ?? "plugin preparation",
          idleMs,
        ),
      );
      return;
    }
    timer = setTimeout(check, Math.min(1_000, idleMs - idleFor));
    timer.unref();
  };
  return {
    progress: {
      completed: completed.buffer,
      phases: port2,
    } satisfies PreparedModelCatalogAdmissionProgress,
    start() {
      progressedAt = performance.now();
      running = true;
      timer = setTimeout(check, Math.min(1_000, idleMs));
      timer.unref();
    },
    stop,
  };
}

/** Preparation is progress-bounded; only the readiness exchange starts the provider deadline. */
export async function runPreparedModelCatalogTask(
  pool: WorkerTaskPool<PreparedModelCatalogWorkerTask, PreparedModelWorkerResult>,
  input: () => PreparedModelCatalogWorkerTask,
  options: { signal: AbortSignal; isCurrent: () => boolean; timeoutMs: number },
): Promise<PreparedModelWorkerResult> {
  const admission = createPreparedModelCatalogAdmissionWatchdog(options.timeoutMs, (error) => {
    void pool.close(error).catch((failure: unknown) => {
      process.emitWarning(`Stalled catalog worker failed to retire: ${String(failure)}`);
    });
  });
  try {
    return await pool.run(
      () => {
        admission.start();
        return { ...input(), admissionProgress: admission.progress };
      },
      {
        signal: options.signal,
        transferList: () => [admission.progress.phases],
        onRequest: async () => {
          admission.stop();
          // A retired borrower must not turn admission into a shared worker failure.
          return { input: options.isCurrent(), timeoutMs: options.timeoutMs };
        },
      },
    );
  } finally {
    admission.stop();
  }
}
