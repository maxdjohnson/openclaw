import type { PluginMetadataSnapshot } from "../plugins/plugin-metadata-snapshot.types.js";
import { captureProviderSyntheticAuthFacts } from "../plugins/provider-runtime.js";
import type { PluginRegistry } from "../plugins/registry-types.js";
import { withPluginRuntimeGenerationScope } from "../plugins/runtime/generation-scope.js";
import { listManifestSyntheticAuthProviderRefs } from "../plugins/synthetic-auth.runtime.js";
import type { PreparedModelCatalogWorkerInput } from "./prepared-model-catalog-worker.js";
import {
  listRegistrySyntheticAuthProviderRefs,
  scopeSyntheticAuthProviderRefs,
} from "./prepared-model-runtime.synthetic-auth.js";

export function captureCatalogSyntheticAuth(params: {
  workerInput: Pick<PreparedModelCatalogWorkerInput, "input" | "providerIds">;
  metadataSnapshot: PluginMetadataSnapshot;
  pluginRegistry: PluginRegistry | undefined;
  providerIds: readonly string[] | undefined;
  fullCatalog: boolean;
  signal: AbortSignal;
}) {
  const { workerInput, metadataSnapshot, pluginRegistry } = params;
  const { input } = workerInput;
  // Worker reconstruction consumes startup auth facts even for a scoped catalog request.
  const providerScope = [...workerInput.providerIds, ...(params.providerIds ?? [])];
  return withPluginRuntimeGenerationScope({ metadataSnapshot, pluginRegistry }, () =>
    captureProviderSyntheticAuthFacts({
      config: input.config,
      env: input.env,
      workspaceDir: input.workspaceDir,
      providerRefs: params.fullCatalog
        ? [
            ...listManifestSyntheticAuthProviderRefs(metadataSnapshot.index),
            // Full discovery includes credential-only refs that no manifest declares.
            // The closed worker cannot probe those refs, so capture them here.
            ...listRegistrySyntheticAuthProviderRefs(pluginRegistry),
            ...workerInput.providerIds,
          ]
        : [
            ...providerScope,
            ...scopeSyntheticAuthProviderRefs(
              listManifestSyntheticAuthProviderRefs(metadataSnapshot.index),
              providerScope,
            ),
          ],
      signal: params.signal,
    }),
  );
}
