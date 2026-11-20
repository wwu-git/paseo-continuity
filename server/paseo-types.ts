import type {
  PluginHandlerContext,
  PluginHookAgent,
  PluginLifecycleEvents,
  PluginTurnOutcome,
} from "@getpaseo/plugin/server";

/**
 * Derived from `@getpaseo/plugin/server` rather than `@getpaseo/client` or `@getpaseo/protocol`:
 * Paseo only supplies `@getpaseo/plugin` to plugins at install/build time, and `devDependencies`
 * are not installed when Paseo installs a plugin.
 */
export type PaseoApi = PluginHandlerContext["paseo"];
export type AgentTimelineItem = PluginLifecycleEvents["agent.turn_ended"]["timeline"][number];
export type ToolCallDetail = Extract<AgentTimelineItem, { type: "tool_call" }>["detail"];
export type { PluginHookAgent, PluginTurnOutcome };
