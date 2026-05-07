import { ToolRegistry, createSearchToolsMetaTool } from "@us-all/mcp-toolkit";
import { config } from "./config.js";

/**
 * Categories used by DBT_TOOLS / DBT_DISABLE env toggles.
 *
 * Default: all categories enabled.
 * `DBT_TOOLS=dbt` → only dbt artifact tools load (allowlist)
 * `DBT_DISABLE=quality` → DQ result table tools excluded (denylist)
 */
export const CATEGORIES = [
  "dbt",         // dbt artifact tools (manifest/run_results/sources/catalog)
  "quality",     // DQ result tables (quality_checks / quality_score_daily)
  "meta",        // search-tools (always enabled)
] as const;

export type Category = (typeof CATEGORIES)[number];

export const registry = new ToolRegistry<Category>({
  enabledCategories: config.enabledCategories,
  disabledCategories: config.disabledCategories,
});

const meta = createSearchToolsMetaTool(
  registry,
  CATEGORIES,
  "Discover available dbt MCP tools across dbt artifact / DQ result table / meta categories — call this first to find the right tool.",
);

export const searchToolsSchema = meta.schema;
export const searchTools = meta.handler;
