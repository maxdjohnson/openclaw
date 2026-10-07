export class PreparedModelCatalogConfigReplacedError extends Error {
  constructor(agentDir: string) {
    super(`prepared model catalog owner config was replaced during the read (${agentDir})`);
    this.name = "PreparedModelCatalogConfigReplacedError";
  }
}

export class PreparedModelCatalogAdmissionStalledError extends Error {
  constructor(pluginId: string | undefined, stage: string, idleMs: number) {
    super(
      `${pluginId ? `native admission for plugin ${pluginId}` : "catalog preparation"} stalled during ${stage} after ${idleMs} ms without progress; reload the plugin or restart the Gateway to retry`,
    );
    this.name = "PreparedModelCatalogAdmissionStalledError";
  }
}
