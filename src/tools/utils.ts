import { createWrapToolHandler } from "@us-all/mcp-toolkit";
import { config } from "../config.js";

export class WriteBlockedError extends Error {
  constructor() {
    super("Write operations are disabled. Set DBT_ALLOW_WRITE=true to enable.");
    this.name = "WriteBlockedError";
  }
}

export function assertWriteAllowed(): void {
  if (!config.allowWrite) {
    throw new WriteBlockedError();
  }
}

export class ConfigMissingError extends Error {
  constructor(envVar: string, feature: string) {
    super(`${feature} requires ${envVar} to be set`);
    this.name = "ConfigMissingError";
  }
}

export class DqStoreError extends Error {
  cause: unknown;
  constructor(message: string, cause: unknown) {
    super(message);
    this.name = "DqStoreError";
    this.cause = cause;
  }
}

export const wrapToolHandler = createWrapToolHandler({
  redactionPatterns: [
    /PG_CONNECTION_STRING/i,
    /-----BEGIN[^-]+-----[\s\S]*?-----END[^-]+-----/,
  ],
  errorExtractors: [
    {
      match: (error) => error instanceof WriteBlockedError,
      extract: (error) => ({
        kind: "passthrough",
        text: (error as WriteBlockedError).message,
      }),
    },
    {
      match: (error) => error instanceof ConfigMissingError,
      extract: (error) => ({
        kind: "passthrough",
        text: (error as ConfigMissingError).message,
      }),
    },
    {
      match: (error) => error instanceof DqStoreError,
      extract: (error) => {
        const err = error as DqStoreError;
        return {
          kind: "structured",
          data: { message: err.message, cause: String(err.cause) },
        };
      },
    },
  ],
});
