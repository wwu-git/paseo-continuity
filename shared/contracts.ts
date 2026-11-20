import { defineRpc } from "@getpaseo/plugin";
import { z } from "zod";
import { ContinuitySettingsSchema } from "./settings";

export const TRANSITION_KIND = "transition";
export const TRANSITION_VERSION = 1;
export const TRANSITION_ROW_ID = "transition";

export const FailureClassSchema = z.enum(["quota", "rate_limit", "overloaded", "manual"]);
export type FailureClass = z.output<typeof FailureClassSchema>;

export const TransitionStatusSchema = z.enum([
  "creating",
  "created",
  "failed",
  "exhausted",
  "halted",
]);
export type TransitionStatus = z.output<typeof TransitionStatusSchema>;

export const TransitionCardSchema = z.object({
  status: TransitionStatusSchema,
  reason: FailureClassSchema,
  evidence: z.string(),
  fromProvider: z.string(),
  toProvider: z.string().nullable(),
  toAgentId: z.string().nullable(),
  toTitle: z.string().nullable(),
  detail: z.string().nullable(),
  at: z.string(),
});
export type TransitionCard = z.output<typeof TransitionCardSchema>;

export const failoverNowRpc = defineRpc({
  name: "continuity.failover-now",
  input: z.object({
    agentId: z.string().min(1),
    /** Provider to continue on; omitted picks the next ready provider in the chain. */
    targetProvider: z.string().min(1).optional(),
    /** "file" writes a compacted handoff file and points the successor at it. */
    handoff: z.enum(["inline", "file"]).optional(),
  }),
  output: z.object({
    status: z.enum(["created", "failed", "exhausted", "halted", "skipped"]),
    newAgentId: z.string().nullable(),
    message: z.string(),
  }),
});

export const SubagentFailoverEventTypeSchema = z.enum([
  "subagent.failover.started",
  "subagent.failover.completed",
  "subagent.failover.failed",
]);
export type SubagentFailoverEventType = z.output<typeof SubagentFailoverEventTypeSchema>;

export const SubagentFailoverEventSchema = z.object({
  type: SubagentFailoverEventTypeSchema,
  parentAgentId: z.string(),
  oldAgentId: z.string(),
  newAgentId: z.string().nullable(),
  taskId: z.string(),
  fromProvider: z.string(),
  toProvider: z.string().nullable(),
  reason: FailureClassSchema,
  status: TransitionStatusSchema,
  detail: z.string().nullable(),
  at: z.string(),
});
export type SubagentFailoverEvent = z.output<typeof SubagentFailoverEventSchema>;

export const TrackedTaskStatusSchema = z.enum([
  "running",
  "failing_over",
  "completed",
  "failed",
  "canceled",
  "archived",
]);
export type TrackedTaskStatus = z.output<typeof TrackedTaskStatusSchema>;

export const TrackedTaskSchema = z.object({
  taskId: z.string(),
  originalAgentId: z.string(),
  currentAgentId: z.string(),
  agentHistory: z.array(z.string()),
  originalParentId: z.string(),
  currentSupervisorId: z.string(),
  title: z.string().nullable(),
  provider: z.string(),
  status: TrackedTaskStatusSchema,
  switchCount: z.number().int().min(0),
  lastError: z.string().nullable(),
  updatedAt: z.string(),
});
export type TrackedTask = z.output<typeof TrackedTaskSchema>;

export const CoordinationSnapshotSchema = z.object({
  tasks: z.array(TrackedTaskSchema),
  supervisorRedirects: z.record(z.string(), z.string()),
  recentEvents: z.array(SubagentFailoverEventSchema),
});
export type CoordinationSnapshot = z.output<typeof CoordinationSnapshotSchema>;

export const getSettingsRpc = defineRpc({
  name: "continuity.get-settings",
  input: z.object({}),
  output: ContinuitySettingsSchema,
});

export const getCoordinationStateRpc = defineRpc({
  name: "continuity.get-coordination-state",
  input: z.object({}),
  output: CoordinationSnapshotSchema,
});

