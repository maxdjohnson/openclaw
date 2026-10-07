export type PluginNativeAdmissionPhase = { pluginId: string; stage: string };
export type PluginNativeAdmissionObserver = {
  phase: (phase: PluginNativeAdmissionPhase) => void;
  advance: () => void;
};

// Catalog tasks are serial per worker; retained plugin async contexts must see the active task.
let activeObserver: PluginNativeAdmissionObserver | undefined;
// Capture and hashing are synchronous; nested passes restore the caller's phase.
let current:
  | { observer: PluginNativeAdmissionObserver; phase: PluginNativeAdmissionPhase }
  | undefined;
const unobserved = { [Symbol.dispose]() {} };

export async function observePluginNativeAdmissions<T>(
  observer: PluginNativeAdmissionObserver,
  run: () => Promise<T>,
): Promise<T> {
  const previous = activeObserver;
  activeObserver = observer;
  try {
    return await run();
  } finally {
    activeObserver = previous;
  }
}

export function trackPluginNativeAdmission(pluginId: string, stage: string): Disposable {
  const observer = activeObserver;
  if (!observer) {
    return unobserved;
  }
  const previous = current;
  current = { observer, phase: { pluginId, stage } };
  observer.phase(current.phase);
  return {
    [Symbol.dispose]() {
      current = previous;
      if (previous) {
        previous.observer.phase(previous.phase);
      }
    },
  };
}

/** Only completed filesystem/verification work renews the admission watchdog. */
export function advancePluginNativeAdmission(): void {
  current?.observer.advance();
}
