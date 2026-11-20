import type {
  CoordinationSnapshot,
  FailureClass,
  SubagentFailoverEvent,
  TrackedTask,
  TrackedTaskStatus,
  TransitionStatus,
} from "../shared/contracts";
import { LABEL, PASEO_PARENT_AGENT_ID_LABEL, providerIdOf } from "../shared/labels";
import type { ContinuitySettings } from "../shared/settings";
import type { AgentTimelineItem, PaseoApi, PluginTurnOutcome } from "./paseo-types";
import { lastAssistantText, latestTurnItems } from "./timeline";

const MAX_RECENT_EVENTS = 100;
const RESULT_MESSAGE_LIMIT = 4_000;

const log = (...args: unknown[]) => console.log("[continuity:coord]", ...args);

interface PendingDelivery {
  key: string;
  text: string;
}

export interface SubagentRef {
  id: string;
  parentAgentId: string;
  provider: string;
  title: string | null;
  labels?: Readonly<Record<string, string>> | null;
}

export interface SubagentFailoverNotification {
  taskId: string;
  parentAgentId: string;
  oldAgentId: string;
  newAgentId: string | null;
  fromProvider: string;
  toProvider: string | null;
  reason: FailureClass;
  status: TransitionStatus;
  detail?: string | null;
}

export interface SubagentTurnEndedInput {
  agent: {
    id: string;
    parentAgentId: string | null;
    provider: string;
    title: string | null;
  };
  turnId: string | null;
  outcome: PluginTurnOutcome;
  timeline: readonly AgentTimelineItem[];
}

export type CoordinationEventListener = (event: SubagentFailoverEvent) => void;

/**
 * Coordinates parent/child agent relationships across provider failovers.
 *
 * Why this exists:
 * - Paseo's `workspace.agents.create({ parent })` sets `paseo.parent-agent-id` on the child,
 *   but does NOT register a daemon `finishSubscription` (only the MCP `create_agent` tool does).
 * - Paseo's public `PaseoApi` does not expose label updates for existing running agents, so when
 *   a supervisor switches providers, running children still point at the old supervisor ID.
 *
 * `TaskCoordinator` bridges both gaps: it keeps stable `taskId` -> agent history mappings,
 * tracks supervisor redirects (`S1 -> S2 -> ...`), buffers deliveries during in-flight supervisor
 * failovers, notifies supervisors of subagent switches, and relays final subagent results.
 */
export class TaskCoordinator {
  private readonly tasksById = new Map<string, TrackedTask>();
  private readonly taskIdByAgentId = new Map<string, string>();
  private readonly supervisorRedirects = new Map<string, string>();
  private readonly supervisorsInFailover = new Set<string>();
  private readonly pendingDeliveries = new Map<string, PendingDelivery[]>();
  private readonly deliveredKeys = new Set<string>();
  private readonly recentEvents: SubagentFailoverEvent[] = [];
  private readonly listeners = new Set<CoordinationEventListener>();

  constructor(private readonly now: () => Date = () => new Date()) {}

  onEvent(listener: CoordinationEventListener): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  /** Follows transitive supervisor redirects (S1 -> S2 -> S3) with cycle protection. */
  resolveSupervisor(agentId: string): string {
    let current = agentId;
    const seen = new Set<string>([current]);
    for (;;) {
      const next = this.supervisorRedirects.get(current);
      if (!next || seen.has(next)) {
        return current;
      }
      seen.add(next);
      current = next;
    }
  }

  hasSupervisorRedirected(agentId: string): boolean {
    return this.resolveSupervisor(agentId) !== agentId;
  }

  isSupervisorInFailover(supervisorId: string): boolean {
    return this.supervisorsInFailover.has(this.resolveSupervisor(supervisorId));
  }

  getTaskByAgentId(agentId: string): TrackedTask | null {
    const taskId = this.taskIdByAgentId.get(agentId);
    return taskId ? (this.tasksById.get(taskId) ?? null) : null;
  }

  getTask(taskId: string): TrackedTask | null {
    return this.tasksById.get(taskId) ?? null;
  }

  getTasksForSupervisor(supervisorId: string): TrackedTask[] {
    const resolved = this.resolveSupervisor(supervisorId);
    const matches: TrackedTask[] = [];
    for (const task of this.tasksById.values()) {
      if (this.resolveSupervisor(task.currentSupervisorId) === resolved) {
        matches.push(task);
      }
    }
    return matches;
  }

  /** Registers or updates a subagent in the task registry, preserving its stable `taskId`. */
  registerSubagent(ref: SubagentRef, status: TrackedTaskStatus = "running"): TrackedTask {
    const labelTaskId = ref.labels?.[LABEL.taskId]?.trim() || null;
    const existingTaskId = this.taskIdByAgentId.get(ref.id) ?? labelTaskId;
    const existing = existingTaskId ? this.tasksById.get(existingTaskId) : undefined;
    const originalParentId =
      existing?.originalParentId ??
      ref.labels?.[LABEL.rootParentAgentId]?.trim() ??
      ref.parentAgentId;
    const currentSupervisorId = this.resolveSupervisor(
      ref.labels?.[LABEL.supervisorAgentId]?.trim() || ref.parentAgentId,
    );
    const nowIso = this.now().toISOString();

    if (existing) {
      if (!existing.agentHistory.includes(ref.id)) {
        existing.agentHistory.push(ref.id);
      }
      existing.currentAgentId = ref.id;
      existing.currentSupervisorId = currentSupervisorId;
      existing.provider = providerIdOf(ref.provider);
      existing.title = ref.title ?? existing.title;
      existing.status = status;
      existing.updatedAt = nowIso;
      this.taskIdByAgentId.set(ref.id, existing.taskId);
      return existing;
    }

    const taskId = labelTaskId || ref.id;
    const task: TrackedTask = {
      taskId,
      originalAgentId: ref.id,
      currentAgentId: ref.id,
      agentHistory: [ref.id],
      originalParentId,
      currentSupervisorId,
      title: ref.title,
      provider: providerIdOf(ref.provider),
      status,
      switchCount: 0,
      lastError: null,
      updatedAt: nowIso,
    };
    this.tasksById.set(taskId, task);
    this.taskIdByAgentId.set(ref.id, taskId);
    return task;
  }

  markArchived(agentId: string): void {
    const task = this.getTaskByAgentId(agentId);
    if (!task) return;
    // Only archive the task if the archived agent is the currently active successor.
    if (task.currentAgentId === agentId) {
      task.status = "archived";
      task.updatedAt = this.now().toISOString();
    }
  }

  async notifySubagentFailoverStarted(
    paseo: PaseoApi,
    settings: ContinuitySettings,
    input: Omit<SubagentFailoverNotification, "newAgentId" | "status">,
  ): Promise<SubagentFailoverEvent> {
    const supervisorId = this.resolveSupervisor(input.parentAgentId);
    const task = this.tasksById.get(input.taskId);
    if (task) {
      task.status = "failing_over";
      task.currentSupervisorId = supervisorId;
      task.updatedAt = this.now().toISOString();
    }

    const event = this.recordEvent({
      type: "subagent.failover.started",
      parentAgentId: supervisorId,
      oldAgentId: input.oldAgentId,
      newAgentId: null,
      taskId: input.taskId,
      fromProvider: input.fromProvider,
      toProvider: input.toProvider,
      reason: input.reason,
      status: "creating",
      detail: input.detail ?? null,
      at: this.now().toISOString(),
    });

    if (settings.coordination.notifySupervisorOnFailover) {
      const text = formatFailoverEventMessage(event);
      await this.deliverToSupervisor(paseo, supervisorId, {
        key: `failover-started:${event.taskId}:${event.oldAgentId}`,
        text,
      });
    }
    return event;
  }

  async notifySubagentFailoverCompleted(
    paseo: PaseoApi,
    settings: ContinuitySettings,
    input: Omit<SubagentFailoverNotification, "status"> & { newAgentId: string; toProvider: string },
  ): Promise<SubagentFailoverEvent> {
    const supervisorId = this.resolveSupervisor(input.parentAgentId);
    const task = this.tasksById.get(input.taskId);
    const nowIso = this.now().toISOString();
    if (task) {
      if (!task.agentHistory.includes(input.newAgentId)) {
        task.agentHistory.push(input.newAgentId);
      }
      task.currentAgentId = input.newAgentId;
      task.currentSupervisorId = supervisorId;
      task.provider = providerIdOf(input.toProvider);
      task.status = "running";
      task.switchCount += 1;
      task.lastError = null;
      task.updatedAt = nowIso;
      this.taskIdByAgentId.set(input.newAgentId, task.taskId);
    }

    const event = this.recordEvent({
      type: "subagent.failover.completed",
      parentAgentId: supervisorId,
      oldAgentId: input.oldAgentId,
      newAgentId: input.newAgentId,
      taskId: input.taskId,
      fromProvider: input.fromProvider,
      toProvider: input.toProvider,
      reason: input.reason,
      status: "created",
      detail: input.detail ?? null,
      at: nowIso,
    });

    if (settings.coordination.notifySupervisorOnFailover) {
      const text = formatFailoverEventMessage(event);
      await this.deliverToSupervisor(paseo, supervisorId, {
        key: `failover-completed:${event.taskId}:${event.oldAgentId}:${event.newAgentId}`,
        text,
      });
    }
    return event;
  }

  async notifySubagentFailoverFailed(
    paseo: PaseoApi,
    settings: ContinuitySettings,
    input: Omit<SubagentFailoverNotification, "newAgentId"> & {
      status: "failed" | "exhausted" | "halted";
    },
  ): Promise<SubagentFailoverEvent> {
    const supervisorId = this.resolveSupervisor(input.parentAgentId);
    const task = this.tasksById.get(input.taskId);
    const nowIso = this.now().toISOString();
    if (task) {
      task.status = "failed";
      task.currentSupervisorId = supervisorId;
      task.lastError = input.detail ?? null;
      task.updatedAt = nowIso;
    }

    const event = this.recordEvent({
      type: "subagent.failover.failed",
      parentAgentId: supervisorId,
      oldAgentId: input.oldAgentId,
      newAgentId: null,
      taskId: input.taskId,
      fromProvider: input.fromProvider,
      toProvider: input.toProvider,
      reason: input.reason,
      status: input.status,
      detail: input.detail ?? null,
      at: nowIso,
    });

    if (settings.coordination.notifySupervisorOnFailover) {
      const text = formatFailoverEventMessage(event);
      await this.deliverToSupervisor(paseo, supervisorId, {
        key: `failover-failed:${event.taskId}:${event.oldAgentId}:${event.status}`,
        text,
      });
    }
    return event;
  }

  /**
   * Called when a subagent finishes a turn without triggering a new failover.
   * Relays the outcome & final assistant response to the current active supervisor whenever:
   * - the subagent is a continuity failover successor (no native finishSubscription exists), OR
   * - its supervisor previously failed over (native finishSubscription targets the old supervisor).
   */
  async onSubagentTurnEnded(
    paseo: PaseoApi,
    event: SubagentTurnEndedInput,
    settings: ContinuitySettings,
  ): Promise<boolean> {
    const { agent, outcome, timeline, turnId } = event;
    const parentId = agent.parentAgentId;
    const existingTask = this.getTaskByAgentId(agent.id);
    if (!existingTask && !parentId) {
      return false;
    }

    const task =
      existingTask ??
      this.registerSubagent({
        id: agent.id,
        parentAgentId: parentId!,
        provider: agent.provider,
        title: agent.title,
      });

    // Ignore stale turn-end events from an older superseded agent in the same task chain.
    if (task.currentAgentId !== agent.id) {
      return false;
    }

    const supervisorId = this.resolveSupervisor(task.currentSupervisorId);
    task.currentSupervisorId = supervisorId;
    task.updatedAt = this.now().toISOString();

    if (outcome.kind === "completed") {
      task.status = "completed";
      task.lastError = null;
    } else if (outcome.kind === "canceled") {
      task.status = "canceled";
      task.lastError = outcome.reason;
    } else {
      task.status = "failed";
      task.lastError = [outcome.error.code, outcome.error.message].filter(Boolean).join(": ");
    }

    const isFailoverSuccessor = task.switchCount > 0 || task.originalAgentId !== agent.id;
    const supervisorSwitched = supervisorId !== task.originalParentId;
    const supervisorSwitching = this.supervisorsInFailover.has(supervisorId);

    if (
      !settings.coordination.relaySubagentResults ||
      (!isFailoverSuccessor && !supervisorSwitched && !supervisorSwitching)
    ) {
      return false;
    }

    const deliveryKey = `result:${task.taskId}:${agent.id}:${turnId ?? outcome.kind}`;
    const body = formatSubagentResultMessage({
      task,
      agentId: agent.id,
      title: agent.title ?? task.title,
      outcome,
      timeline,
    });
    return this.deliverToSupervisor(paseo, supervisorId, { key: deliveryKey, text: body });
  }

  /**
   * Begins supervisor failover: marks the old supervisor as switching (buffering any subagent
   * completions that arrive during the transition), discovers all live child agents, and returns
   * a handoff prompt section so the new supervisor inherits the task tree without re-dispatching.
   */
  async prepareSupervisorFailover(paseo: PaseoApi, oldSupervisorId: string): Promise<string | null> {
    this.supervisorsInFailover.add(oldSupervisorId);

    try {
      const listed = await paseo.agents.list().catch((error: unknown) => {
        log("could not list agents during supervisor failover", oldSupervisorId, error);
        return null;
      });
      for (const entry of listed?.entries ?? []) {
        const snapshot = entry.agent;
        if (!snapshot || snapshot.archivedAt) continue;
        const rawParent = snapshot.labels?.[PASEO_PARENT_AGENT_ID_LABEL] ?? null;
        if (rawParent && this.resolveSupervisor(rawParent) === oldSupervisorId) {
          const status = mapSnapshotStatus(snapshot.status);
          this.registerSubagent(
            {
              id: snapshot.id,
              parentAgentId: rawParent,
              provider: snapshot.provider,
              title: snapshot.title ?? null,
              labels: snapshot.labels,
            },
            status,
          );
        }
      }
    } catch (error) {
      log("error scanning child agents for supervisor", oldSupervisorId, error);
    }

    const children = this.getTasksForSupervisor(oldSupervisorId).filter(
      (task) => task.status !== "archived",
    );
    if (children.length === 0) {
      return null;
    }

    const lines = [
      "## Active Subagent Tasks (Supervisor Handoff)",
      "",
      `You are replacing supervisor agent \`${oldSupervisorId}\`. The following delegated subagent tasks belong to this supervisor:`,
      "",
      ...children.map((task) => {
        const title = task.title ? ` (${task.title})` : "";
        const switches = task.switchCount > 0 ? `, switched ${task.switchCount}x` : "";
        return `- taskId=\`${task.taskId}\` | currentAgentId=\`${task.currentAgentId}\` | originalAgentId=\`${task.originalAgentId}\` | provider=\`${task.provider}\` | status=\`${task.status}\`${switches}${title}`;
      }),
      "",
      "Important coordination rules:",
      "- Do NOT re-dispatch tasks whose status is `running` or `failing_over`; they are already active.",
      "- Paseo Continuity will relay completion results and failover updates from these subagents directly to you.",
      "- Any subagent that finished while you were switching will have its result delivered immediately.",
    ];
    return lines.join("\n");
  }

  /**
   * Finishes supervisor failover: redirects all tasks from `oldSupervisorId` to `newSupervisorId`
   * and flushes any buffered subagent notifications/results that arrived mid-switch.
   */
  async completeSupervisorFailover(
    paseo: PaseoApi,
    oldSupervisorId: string,
    newSupervisorId: string,
  ): Promise<void> {
    this.supervisorRedirects.set(oldSupervisorId, newSupervisorId);
    this.supervisorsInFailover.delete(oldSupervisorId);

    const nowIso = this.now().toISOString();
    for (const task of this.tasksById.values()) {
      if (this.resolveSupervisor(task.currentSupervisorId) === newSupervisorId) {
        task.currentSupervisorId = newSupervisorId;
        task.updatedAt = nowIso;
      }
    }

    const queued = this.pendingDeliveries.get(oldSupervisorId);
    if (queued && queued.length > 0) {
      this.pendingDeliveries.delete(oldSupervisorId);
      for (const delivery of queued) {
        await this.deliverToSupervisor(paseo, newSupervisorId, delivery);
      }
    }
  }

  /** Aborts an in-flight supervisor failover and flushes buffered messages back to the old supervisor. */
  async abortSupervisorFailover(paseo: PaseoApi, oldSupervisorId: string): Promise<void> {
    this.supervisorsInFailover.delete(oldSupervisorId);
    const queued = this.pendingDeliveries.get(oldSupervisorId);
    if (queued && queued.length > 0) {
      this.pendingDeliveries.delete(oldSupervisorId);
      for (const delivery of queued) {
        await this.deliverToSupervisor(paseo, oldSupervisorId, delivery);
      }
    }
  }

  snapshot(): CoordinationSnapshot {
    return {
      tasks: [...this.tasksById.values()].map((task) => ({
        ...task,
        agentHistory: [...task.agentHistory],
      })),
      supervisorRedirects: Object.fromEntries(this.supervisorRedirects.entries()),
      recentEvents: [...this.recentEvents],
    };
  }

  private recordEvent(event: SubagentFailoverEvent): SubagentFailoverEvent {
    this.recentEvents.push(event);
    if (this.recentEvents.length > MAX_RECENT_EVENTS) {
      this.recentEvents.splice(0, this.recentEvents.length - MAX_RECENT_EVENTS);
    }
    for (const listener of this.listeners) {
      try {
        listener(event);
      } catch (error) {
        log("event listener threw", error);
      }
    }
    return event;
  }

  private async deliverToSupervisor(
    paseo: PaseoApi,
    rawSupervisorId: string,
    delivery: PendingDelivery,
  ): Promise<boolean> {
    const supervisorId = this.resolveSupervisor(rawSupervisorId);

    if (this.supervisorsInFailover.has(supervisorId)) {
      const queue = this.pendingDeliveries.get(supervisorId) ?? [];
      if (!queue.some((item) => item.key === delivery.key)) {
        queue.push(delivery);
        this.pendingDeliveries.set(supervisorId, queue);
      }
      return true;
    }

    if (this.deliveredKeys.has(delivery.key)) {
      return false;
    }

    const handle = paseo.agents.ref(supervisorId);
    try {
      // Use `activeTurnBehavior: "steer"` (supported by Paseo daemon >=0.12) so notifications
      // never interrupt an active supervisor turn. Fall back to plain `send(text)` if an older
      // daemon rejects extra options.
      await sendSteeredMessage(handle, delivery.text);
      this.deliveredKeys.add(delivery.key);
      return true;
    } catch (error) {
      log("could not deliver message to supervisor", supervisorId, error);
      return false;
    }
  }
}

async function sendSteeredMessage(
  handle: ReturnType<PaseoApi["agents"]["ref"]>,
  text: string,
): Promise<void> {
  const sendWithBehavior = handle.send as (
    message: string,
    options?: Record<string, unknown>,
  ) => Promise<void>;
  try {
    await sendWithBehavior(text, { activeTurnBehavior: "steer" });
  } catch {
    await handle.send(text);
  }
}

function mapSnapshotStatus(status: string | undefined): TrackedTaskStatus {
  if (status === "running") return "running";
  if (status === "error") return "failed";
  if (status === "closed") return "archived";
  return "completed";
}

function formatFailoverEventMessage(event: SubagentFailoverEvent): string {
  const payload = JSON.stringify(
    {
      event: event.type,
      parentAgentId: event.parentAgentId,
      oldAgentId: event.oldAgentId,
      newAgentId: event.newAgentId,
      taskId: event.taskId,
      fromProvider: event.fromProvider,
      toProvider: event.toProvider,
      reason: event.reason,
      status: event.status,
      ...(event.detail ? { detail: event.detail } : {}),
    },
    null,
    2,
  );

  if (event.type === "subagent.failover.started") {
    return [
      `[Paseo Continuity] Subagent failover started for task \`${event.taskId}\`.`,
      `Agent \`${event.oldAgentId}\` (${event.fromProvider}) hit \`${event.reason}\`; creating a successor on \`${event.toProvider ?? "fallback"}\`. Do not re-dispatch this task yet.`,
      `<continuity-event>\n${payload}\n</continuity-event>`,
    ].join("\n\n");
  }

  if (event.type === "subagent.failover.completed") {
    return [
      `[Paseo Continuity] Subagent failover completed for task \`${event.taskId}\`.`,
      `Agent \`${event.oldAgentId}\` (${event.fromProvider}) was replaced by \`${event.newAgentId}\` (${event.toProvider}). Continue tracking \`${event.newAgentId}\`; Continuity will relay its final result to you when it finishes.`,
      `<continuity-event>\n${payload}\n</continuity-event>`,
    ].join("\n\n");
  }

  return [
    `[Paseo Continuity] Subagent failover failed (${event.status}) for task \`${event.taskId}\`.`,
    `Agent \`${event.oldAgentId}\` (${event.fromProvider}) could not be continued${event.detail ? `: ${event.detail}` : "."}`,
    `<continuity-event>\n${payload}\n</continuity-event>`,
  ].join("\n\n");
}

function formatSubagentResultMessage(input: {
  task: TrackedTask;
  agentId: string;
  title: string | null;
  outcome: PluginTurnOutcome;
  timeline: readonly AgentTimelineItem[];
}): string {
  const { task, agentId, title, outcome, timeline } = input;
  const label = title ? ` (${title})` : "";
  const statusText =
    outcome.kind === "completed"
      ? "finished"
      : outcome.kind === "canceled"
        ? `was canceled (${outcome.reason})`
        : `errored (${[outcome.error.code, outcome.error.message].filter(Boolean).join(": ")})`;

  const sections = [
    `[Paseo Continuity] Subagent \`${agentId}\`${label} for task \`${task.taskId}\` ${statusText}.`,
    `Task lineage: original=\`${task.originalAgentId}\`, current=\`${agentId}\`, provider=\`${task.provider}\`, switches=${task.switchCount}.`,
  ];

  const turnItems = latestTurnItems(timeline);
  let lastMessage = lastAssistantText(turnItems);
  if (lastMessage) {
    if (lastMessage.length > RESULT_MESSAGE_LIMIT) {
      const omitted = lastMessage.length - RESULT_MESSAGE_LIMIT;
      lastMessage = `${lastMessage.slice(0, RESULT_MESSAGE_LIMIT)}\n[truncated ${omitted} chars]`;
    }
    sections.push(`<agent-response>\n${lastMessage}\n</agent-response>`);
  }

  return sections.join("\n\n");
}
