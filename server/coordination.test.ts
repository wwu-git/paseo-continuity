import type { PluginLifecycleEvents, PluginServerContext } from "@getpaseo/plugin/server";
import { describe, expect, it, vi } from "vitest";
import contribute from "../index.server";
import {
  getCoordinationStateRpc,
  type CoordinationSnapshot,
  type SubagentFailoverEvent,
} from "../shared/contracts";
import { LABEL, PASEO_PARENT_AGENT_ID_LABEL } from "../shared/labels";
import { DEFAULT_SETTINGS, type ContinuitySettings } from "../shared/settings";
import { TaskCoordinator } from "./coordination";
import { runFailover } from "./failover";
import type { AgentTimelineItem, PaseoApi } from "./paseo-types";
import { ContinuityState } from "./state";

interface FakeAgentRecord {
  id: string;
  workspaceId: string;
  provider: string;
  model: string;
  cwd: string;
  title: string | null;
  status: string;
  archivedAt: string | null;
  labels: Record<string, string>;
}

interface SentMessage {
  agentId: string;
  text: string;
  options: unknown;
}

interface CreatedCall {
  options: Record<string, unknown>;
  createdId: string;
}

function createFakePaseo(options?: {
  initialAgents?: FakeAgentRecord[];
  providers?: Array<{ provider: string; model: string; status?: string; enabled?: boolean }>;
  createHook?: (createOpts: Record<string, unknown>, id: string) => Promise<void> | void;
  failCreateForProvider?: string;
}) {
  let seq = 1;
  const agents = new Map<string, FakeAgentRecord>();
  for (const agent of options?.initialAgents ?? []) {
    agents.set(agent.id, { ...agent, labels: { ...agent.labels } });
  }

  const sentMessages: SentMessage[] = [];
  const createdCalls: CreatedCall[] = [];
  const appendedCards: Array<{ agentId: string; item: unknown }> = [];

  const providerEntries = (
    options?.providers ?? [
      { provider: "codex", model: "gpt-5.4", status: "ready", enabled: true },
      { provider: "claude", model: "claude-sonnet-4-6", status: "ready", enabled: true },
      { provider: "opencode", model: "qwen-coder", status: "ready", enabled: true },
    ]
  ).map((entry) => ({
    provider: entry.provider,
    enabled: entry.enabled ?? true,
    status: entry.status ?? "ready",
    models: [{ id: entry.model, isDefault: true, isSelectable: true }],
    modes: [{ id: "auto" }, { id: "default" }],
    defaultModeId: "default",
  }));

  const paseo = {
    providers: {
      snapshot: vi.fn(async () => ({ entries: providerEntries })),
    },
    agents: {
      list: vi.fn(async () => ({
        entries: [...agents.values()].map((agent) => ({ agent })),
      })),
      ref: vi.fn((agentId: string) => ({
        id: agentId,
        refresh: vi.fn(async () => {
          const found = agents.get(agentId);
          return found ? { agent: found, project: null } : null;
        }),
        send: vi.fn(async (text: string, sendOpts?: unknown) => {
          sentMessages.push({ agentId, text, options: sendOpts });
        }),
        timeline: {
          append: vi.fn(async (item: unknown) => {
            appendedCards.push({ agentId, item });
            return { seq: appendedCards.length, epoch: "e1" };
          }),
          refetch: vi.fn(async () => ({ entries: [] })),
        },
      })),
    },
    workspaces: {
      ref: vi.fn((workspaceId: string) => ({
        id: workspaceId,
        agents: {
          create: vi.fn(async (createOpts: Record<string, unknown>) => {
            const config = createOpts.config as { provider: string };
            const [prov, mod] = config.provider.split("/");
            if (options?.failCreateForProvider && prov === options.failCreateForProvider) {
              throw new Error(`simulated create failure for ${prov}`);
            }
            const id = `created-${seq++}`;
            if (options?.createHook) {
              await options.createHook(createOpts, id);
            }
            const parent = typeof createOpts.parent === "string" ? createOpts.parent : null;
            const labels: Record<string, string> = {
              ...((createOpts.labels as Record<string, string> | undefined) ?? {}),
              ...(parent ? { [PASEO_PARENT_AGENT_ID_LABEL]: parent } : {}),
            };
            const record: FakeAgentRecord = {
              id,
              workspaceId,
              provider: prov ?? config.provider,
              model: mod ?? "default",
              cwd: "/repo",
              title: (createOpts.title as string | undefined) ?? null,
              status: "running",
              archivedAt: null,
              labels,
            };
            agents.set(id, record);
            createdCalls.push({ options: createOpts, createdId: id });
            return { id };
          }),
        },
      })),
      open: vi.fn(async () => paseo.workspaces.ref("ws-1")),
    },
  };

  return {
    paseo: paseo as unknown as PaseoApi,
    agents,
    sentMessages,
    createdCalls,
    appendedCards,
  };
}

function makeSettings(overrides?: Partial<ContinuitySettings>): ContinuitySettings {
  return {
    ...DEFAULT_SETTINGS,
    enabled: true,
    includeSubagents: true,
    chain: [
      { provider: "codex", model: null, modeId: null, thinkingOptionId: null },
      { provider: "claude", model: null, modeId: null, thinkingOptionId: null },
      { provider: "opencode", model: null, modeId: null, thinkingOptionId: null },
    ],
    ...overrides,
  };
}

const QUOTA_TIMELINE: AgentTimelineItem[] = [
  { type: "user_message", text: "Implement feature X" },
  { type: "error", message: "429 insufficient_quota: You exceeded your current quota" },
];

const QUOTA_OUTCOME = {
  kind: "failed" as const,
  error: { message: "429 insufficient_quota: You exceeded your current quota" },
};

function createPluginHarness(initialSettings: ContinuitySettings) {
  const listeners = new Map<string, Array<(event: unknown, ctx: unknown) => void>>();
  const rpcHandlers = new Map<string, (input: unknown, ctx: unknown) => Promise<unknown>>();
  let settingsListener: ((state: unknown) => void) | null = null;

  const server = {
    registerSettings: vi.fn(() => ({
      read: vi.fn(async () => ({ status: "ready", revision: "1", values: initialSettings })),
      subscribe: vi.fn((fn: (state: unknown) => void) => {
        settingsListener = fn;
        return () => {
          settingsListener = null;
        };
      }),
    })),
    on: vi.fn((name: string, handler: (event: unknown, ctx: unknown) => void) => {
      const list = listeners.get(name) ?? [];
      list.push(handler);
      listeners.set(name, list);
      return () => undefined;
    }),
    before: vi.fn(() => () => undefined),
    handle: vi.fn((contract: { name: string }, handler: (input: unknown, ctx: unknown) => Promise<unknown>) => {
      rpcHandlers.set(contract.name, handler);
    }),
    registerProvider: vi.fn(),
  };

  const cleanup = contribute(server as unknown as PluginServerContext);

  return {
    emit<K extends keyof PluginLifecycleEvents>(name: K, event: PluginLifecycleEvents[K], paseo: PaseoApi) {
      for (const fn of listeners.get(name) ?? []) {
        fn(event, { paseo, signal: new AbortController().signal });
      }
    },
    async callRpc<T>(name: string, input: unknown, paseo: PaseoApi): Promise<T> {
      const handler = rpcHandlers.get(name);
      if (!handler) throw new Error(`Missing RPC handler ${name}`);
      return (await handler(input, { paseo })) as T;
    },
    pushSettings(next: ContinuitySettings) {
      settingsListener?.({ status: "ready", revision: "2", values: next });
    },
    cleanup,
  };
}

async function flushMicrotasks(ticks = 12): Promise<void> {
  for (let i = 0; i < ticks; i += 1) {
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
}

describe("Supervisor / Subagent failover coordination", () => {
  it("fails over a subagent on quota exhaustion, inherits parentAgentId, notifies supervisor, and relays final result", async () => {
    const fake = createFakePaseo({
      initialAgents: [
        {
          id: "sup-1",
          workspaceId: "ws-1",
          provider: "claude",
          model: "claude-sonnet-4-6",
          cwd: "/repo",
          title: "Supervisor",
          status: "running",
          archivedAt: null,
          labels: {},
        },
        {
          id: "sub-b",
          workspaceId: "ws-1",
          provider: "codex",
          model: "gpt-5.4",
          cwd: "/repo",
          title: "Subagent B",
          status: "error",
          archivedAt: null,
          labels: { [PASEO_PARENT_AGENT_ID_LABEL]: "sup-1" },
        },
      ],
    });
    const harness = createPluginHarness(makeSettings());
    await flushMicrotasks();

    harness.emit(
      "agent.turn_ended",
      {
        agent: {
          id: "sub-b",
          workspaceId: "ws-1",
          parentAgentId: "sup-1",
          provider: "codex",
          cwd: "/repo",
          title: "Subagent B",
        },
        turnId: "turn-1",
        outcome: QUOTA_OUTCOME,
        timeline: QUOTA_TIMELINE,
      },
      fake.paseo,
    );
    await flushMicrotasks();

    // 1. Successor created with native `parent` and durable continuity labels.
    expect(fake.createdCalls).toHaveLength(1);
    const createdCall = fake.createdCalls[0]!;
    expect(createdCall.options.parent).toBe("sup-1");
    const createdLabels = createdCall.options.labels as Record<string, string>;
    expect(createdLabels[LABEL.taskId]).toBe("sub-b");
    expect(createdLabels[LABEL.rootParentAgentId]).toBe("sup-1");
    expect(createdLabels[LABEL.supervisorAgentId]).toBe("sup-1");
    const newId = createdCall.createdId;

    // 2. Supervisor notified of started + completed with steer behavior.
    const supMessages = fake.sentMessages.filter((m) => m.agentId === "sup-1");
    expect(supMessages.length).toBe(2);
    expect(supMessages[0]!.text).toContain("subagent.failover.started");
    expect(supMessages[0]!.options).toEqual({ activeTurnBehavior: "steer" });
    expect(supMessages[1]!.text).toContain("subagent.failover.completed");
    expect(supMessages[1]!.text).toContain(`"oldAgentId": "sub-b"`);
    expect(supMessages[1]!.text).toContain(`"newAgentId": "${newId}"`);
    expect(supMessages[1]!.text).toContain(`"taskId": "sub-b"`);

    // 3. Coordination state RPC reflects live task tracking.
    const snap = await harness.callRpc<CoordinationSnapshot>(
      getCoordinationStateRpc.name,
      {},
      fake.paseo,
    );
    expect(snap.tasks).toHaveLength(1);
    expect(snap.tasks[0]).toMatchObject({
      taskId: "sub-b",
      originalAgentId: "sub-b",
      currentAgentId: newId,
      agentHistory: ["sub-b", newId],
      currentSupervisorId: "sup-1",
      status: "running",
      switchCount: 1,
    });

    // 4. Successor finishes -> final result is relayed back to supervisor.
    harness.emit(
      "agent.turn_ended",
      {
        agent: {
          id: newId,
          workspaceId: "ws-1",
          parentAgentId: "sup-1",
          provider: "claude",
          cwd: "/repo",
          title: "Subagent B",
        },
        turnId: "turn-2",
        outcome: { kind: "completed" },
        timeline: [
          { type: "user_message", text: "Handoff prompt" },
          { type: "assistant_message", text: "Feature X implemented and tested." },
        ],
      },
      fake.paseo,
    );
    await flushMicrotasks();

    const afterComplete = fake.sentMessages.filter((m) => m.agentId === "sup-1");
    expect(afterComplete).toHaveLength(3);
    expect(afterComplete[2]!.text).toContain(`task \`sub-b\``);
    expect(afterComplete[2]!.text).toContain("Feature X implemented and tested.");
    harness.cleanup();
  });

  it("preserves task identity across 2+ consecutive failovers and delivers final result to supervisor", async () => {
    const fake = createFakePaseo({
      initialAgents: [
        {
          id: "sup-1",
          workspaceId: "ws-1",
          provider: "claude",
          model: "claude-sonnet-4-6",
          cwd: "/repo",
          title: "Supervisor",
          status: "idle",
          archivedAt: null,
          labels: {},
        },
        {
          id: "sub-1",
          workspaceId: "ws-1",
          provider: "codex",
          model: "gpt-5.4",
          cwd: "/repo",
          title: "Worker",
          status: "error",
          archivedAt: null,
          labels: { [PASEO_PARENT_AGENT_ID_LABEL]: "sup-1" },
        },
      ],
    });
    const settings = makeSettings({ maxSwitchesPerChain: 5, maxConsecutiveFailovers: 3 });
    const state = new ContinuityState();
    const coordinator = new TaskCoordinator();

    // First failover: sub-1 (codex) -> created-1 (claude)
    const first = await runFailover(fake.paseo, settings, state, {
      agent: { id: "sub-1", workspaceId: "ws-1", parentAgentId: "sup-1", provider: "codex", cwd: "/repo", title: "Worker" },
      turnId: "t1",
      timeline: QUOTA_TIMELINE,
      reason: "quota",
      evidence: "quota exceeded",
      coordinator,
    });
    expect(first.status).toBe("created");
    const secondId = first.newAgentId!;

    // Second failover: created-1 (claude) -> created-2 (opencode)
    const second = await runFailover(fake.paseo, settings, state, {
      agent: { id: secondId, workspaceId: "ws-1", parentAgentId: "sup-1", provider: "claude", cwd: "/repo", title: "Worker" },
      turnId: "t2",
      timeline: [
        { type: "user_message", text: "handoff" },
        { type: "user_message", text: "follow up" },
        { type: "error", message: "429 rate limit exceeded" },
      ],
      reason: "rate_limit",
      evidence: "429 rate limit",
      coordinator,
    });
    expect(second.status).toBe("created");
    const thirdId = second.newAgentId!;

    const task = coordinator.getTask("sub-1");
    expect(task).toMatchObject({
      taskId: "sub-1",
      originalAgentId: "sub-1",
      currentAgentId: thirdId,
      agentHistory: ["sub-1", secondId, thirdId],
      switchCount: 2,
      currentSupervisorId: "sup-1",
    });

    // Final completion on third agent still relays to sup-1 with original taskId.
    const delivered = await coordinator.onSubagentTurnEnded(
      fake.paseo,
      {
        agent: { id: thirdId, parentAgentId: "sup-1", provider: "opencode", title: "Worker" },
        turnId: "t3",
        outcome: { kind: "completed" },
        timeline: [{ type: "assistant_message", text: "Finished after 2 failovers." }],
      },
      settings,
    );
    expect(delivered).toBe(true);
    const lastMsg = fake.sentMessages[fake.sentMessages.length - 1]!;
    expect(lastMsg.agentId).toBe("sup-1");
    expect(lastMsg.text).toContain("switches=2");
    expect(lastMsg.text).toContain("Finished after 2 failovers.");
  });

  it("recovers supervisor task tree on supervisor failover and buffers subagent completions arriving mid-switch", async () => {
    const settings = makeSettings();
    const state = new ContinuityState();
    const coordinator = new TaskCoordinator();

    let midSwitchPromise: Promise<boolean> | null = null;
    const fake = createFakePaseo({
      initialAgents: [
        {
          id: "sup-1",
          workspaceId: "ws-1",
          provider: "codex",
          model: "gpt-5.4",
          cwd: "/repo",
          title: "Main Supervisor",
          status: "error",
          archivedAt: null,
          labels: {},
        },
        {
          id: "sub-running",
          workspaceId: "ws-1",
          provider: "claude",
          model: "claude-sonnet-4-6",
          cwd: "/repo",
          title: "Running Subagent",
          status: "running",
          archivedAt: null,
          labels: { [PASEO_PARENT_AGENT_ID_LABEL]: "sup-1" },
        },
        {
          id: "sub-racing",
          workspaceId: "ws-1",
          provider: "opencode",
          model: "qwen-coder",
          cwd: "/repo",
          title: "Racing Subagent",
          status: "running",
          archivedAt: null,
          labels: { [PASEO_PARENT_AGENT_ID_LABEL]: "sup-1" },
        },
      ],
      createHook: async () => {
        // Simulate race: `sub-racing` completes WHILE `sup-1` is mid-failover!
        midSwitchPromise = coordinator.onSubagentTurnEnded(
          fake.paseo,
          {
            agent: {
              id: "sub-racing",
              parentAgentId: "sup-1",
              provider: "opencode",
              title: "Racing Subagent",
            },
            turnId: "race-turn",
            outcome: { kind: "completed" },
            timeline: [{ type: "assistant_message", text: "Race result ready!" }],
          },
          settings,
        );
        await midSwitchPromise;
      },
    });

    const supFailover = await runFailover(fake.paseo, settings, state, {
      agent: {
        id: "sup-1",
        workspaceId: "ws-1",
        parentAgentId: null,
        provider: "codex",
        cwd: "/repo",
        title: "Main Supervisor",
      },
      turnId: "sup-turn",
      timeline: QUOTA_TIMELINE,
      reason: "quota",
      evidence: "quota exceeded",
      coordinator,
    });
    expect(supFailover.status).toBe("created");
    const newSupId = supFailover.newAgentId!;

    // 1. New supervisor prompt includes active subagents & anti-duplicate-dispatch instructions.
    const prompt = fake.createdCalls[0]!.options.prompt as string;
    expect(prompt).toContain("## Active Subagent Tasks (Supervisor Handoff)");
    expect(prompt).toContain("sub-running");
    expect(prompt).toContain("sub-racing");
    expect(prompt).toContain("Do NOT re-dispatch tasks");

    // 2. The racing subagent's result was buffered and flushed to the NEW supervisor, NOT sup-1!
    expect(fake.sentMessages.filter((m) => m.agentId === "sup-1")).toHaveLength(0);
    const newSupMessages = fake.sentMessages.filter((m) => m.agentId === newSupId);
    expect(newSupMessages).toHaveLength(1);
    expect(newSupMessages[0]!.text).toContain("Race result ready!");

    // 3. Subsequent completion of `sub-running` also routes to new supervisor `newSupId`.
    await coordinator.onSubagentTurnEnded(
      fake.paseo,
      {
        agent: {
          id: "sub-running",
          parentAgentId: "sup-1",
          provider: "claude",
          title: "Running Subagent",
        },
        turnId: "late-turn",
        outcome: { kind: "completed" },
        timeline: [{ type: "assistant_message", text: "Late subagent finished!" }],
      },
      settings,
    );
    const updatedSupMessages = fake.sentMessages.filter((m) => m.agentId === newSupId);
    expect(updatedSupMessages).toHaveLength(2);
    expect(updatedSupMessages[1]!.text).toContain("Late subagent finished!");
  });

  it("handles concurrent failovers of multiple subagents without mixing task mappings", async () => {
    const fake = createFakePaseo({
      initialAgents: [
        {
          id: "sup-1",
          workspaceId: "ws-1",
          provider: "claude",
          model: "claude-sonnet-4-6",
          cwd: "/repo",
          title: "Supervisor",
          status: "running",
          archivedAt: null,
          labels: {},
        },
        {
          id: "sub-a",
          workspaceId: "ws-1",
          provider: "codex",
          model: "gpt-5.4",
          cwd: "/repo",
          title: "Task A",
          status: "error",
          archivedAt: null,
          labels: { [PASEO_PARENT_AGENT_ID_LABEL]: "sup-1" },
        },
        {
          id: "sub-b",
          workspaceId: "ws-1",
          provider: "codex",
          model: "gpt-5.4",
          cwd: "/repo",
          title: "Task B",
          status: "error",
          archivedAt: null,
          labels: { [PASEO_PARENT_AGENT_ID_LABEL]: "sup-1" },
        },
      ],
    });
    const settings = makeSettings();
    const state = new ContinuityState();
    const coordinator = new TaskCoordinator();

    const [resA, resB] = await Promise.all([
      runFailover(fake.paseo, settings, state, {
        agent: { id: "sub-a", workspaceId: "ws-1", parentAgentId: "sup-1", provider: "codex", cwd: "/repo", title: "Task A" },
        turnId: "ta",
        timeline: QUOTA_TIMELINE,
        reason: "quota",
        evidence: "quota A",
        coordinator,
      }),
      runFailover(fake.paseo, settings, state, {
        agent: { id: "sub-b", workspaceId: "ws-1", parentAgentId: "sup-1", provider: "codex", cwd: "/repo", title: "Task B" },
        turnId: "tb",
        timeline: QUOTA_TIMELINE,
        reason: "quota",
        evidence: "quota B",
        coordinator,
      }),
    ]);

    expect(resA.status).toBe("created");
    expect(resB.status).toBe("created");
    expect(resA.newAgentId).not.toBe(resB.newAgentId);

    expect(coordinator.getTask("sub-a")?.currentAgentId).toBe(resA.newAgentId);
    expect(coordinator.getTask("sub-b")?.currentAgentId).toBe(resB.newAgentId);
    expect(coordinator.getTaskByAgentId(resA.newAgentId!)?.taskId).toBe("sub-a");
    expect(coordinator.getTaskByAgentId(resB.newAgentId!)?.taskId).toBe("sub-b");
  });

  it("notifies supervisor with subagent.failover.failed when successor creation fails", async () => {
    const fake = createFakePaseo({
      initialAgents: [
        {
          id: "sup-1",
          workspaceId: "ws-1",
          provider: "claude",
          model: "claude-sonnet-4-6",
          cwd: "/repo",
          title: "Supervisor",
          status: "idle",
          archivedAt: null,
          labels: {},
        },
        {
          id: "sub-fail",
          workspaceId: "ws-1",
          provider: "codex",
          model: "gpt-5.4",
          cwd: "/repo",
          title: "Failing Subagent",
          status: "error",
          archivedAt: null,
          labels: { [PASEO_PARENT_AGENT_ID_LABEL]: "sup-1" },
        },
      ],
      failCreateForProvider: "claude",
    });
    const settings = makeSettings();
    const state = new ContinuityState();
    const coordinator = new TaskCoordinator();
    const events: SubagentFailoverEvent[] = [];
    coordinator.onEvent((ev) => events.push(ev));

    const result = await runFailover(fake.paseo, settings, state, {
      agent: { id: "sub-fail", workspaceId: "ws-1", parentAgentId: "sup-1", provider: "codex", cwd: "/repo", title: "Failing Subagent" },
      turnId: "tf",
      timeline: QUOTA_TIMELINE,
      reason: "quota",
      evidence: "quota exceeded",
      coordinator,
    });

    expect(result.status).toBe("failed");
    expect(events.map((e) => e.type)).toEqual(["subagent.failover.started", "subagent.failover.failed"]);
    expect(fake.sentMessages.some((m) => m.agentId === "sup-1" && m.text.includes("subagent.failover.failed"))).toBe(
      true,
    );
  });

  it("prevents duplicate successors on repeated turn_ended events, preserves single-agent failover, and respects includeSubagents=false", async () => {
    // 1. includeSubagents=false -> does NOT auto-failover subagent
    const fakeDisabled = createFakePaseo({
      initialAgents: [
        {
          id: "sub-ignored",
          workspaceId: "ws-1",
          provider: "codex",
          model: "gpt-5.4",
          cwd: "/repo",
          title: "Sub",
          status: "error",
          archivedAt: null,
          labels: { [PASEO_PARENT_AGENT_ID_LABEL]: "sup-1" },
        },
      ],
    });
    const harnessDisabled = createPluginHarness(makeSettings({ includeSubagents: false }));
    await flushMicrotasks();

    harnessDisabled.emit(
      "agent.turn_ended",
      {
        agent: {
          id: "sub-ignored",
          workspaceId: "ws-1",
          parentAgentId: "sup-1",
          provider: "codex",
          cwd: "/repo",
          title: "Sub",
        },
        turnId: "t1",
        outcome: QUOTA_OUTCOME,
        timeline: QUOTA_TIMELINE,
      },
      fakeDisabled.paseo,
    );
    await flushMicrotasks();
    expect(fakeDisabled.createdCalls).toHaveLength(0);
    harnessDisabled.cleanup();

    // 2. Single-agent failover still works cleanly and duplicate events do not create duplicate successors
    const fakeSingle = createFakePaseo({
      initialAgents: [
        {
          id: "solo-1",
          workspaceId: "ws-1",
          provider: "codex",
          model: "gpt-5.4",
          cwd: "/repo",
          title: "Solo Agent",
          status: "error",
          archivedAt: null,
          labels: {},
        },
      ],
    });
    const harnessSingle = createPluginHarness(makeSettings({ includeSubagents: false }));
    await flushMicrotasks();

    const soloEvent: PluginLifecycleEvents["agent.turn_ended"] = {
      agent: {
        id: "solo-1",
        workspaceId: "ws-1",
        parentAgentId: null,
        provider: "codex",
        cwd: "/repo",
        title: "Solo Agent",
      },
      turnId: "t-solo",
      outcome: QUOTA_OUTCOME,
      timeline: QUOTA_TIMELINE,
    };
    // Emit twice concurrently to test duplicate protection.
    harnessSingle.emit("agent.turn_ended", soloEvent, fakeSingle.paseo);
    harnessSingle.emit("agent.turn_ended", soloEvent, fakeSingle.paseo);
    await flushMicrotasks();

    expect(fakeSingle.createdCalls).toHaveLength(1);
    expect(fakeSingle.createdCalls[0]!.options.parent).toBeUndefined();
    expect(fakeSingle.sentMessages).toHaveLength(0);
    harnessSingle.cleanup();
  });

  it("rebuilds supervisor redirects and subagent task lineage from durable labels after plugin restart", async () => {
    const fake = createFakePaseo({
      initialAgents: [
        {
          id: "sup-1",
          workspaceId: "ws-1",
          provider: "codex",
          model: "gpt-5.4",
          cwd: "/repo",
          title: "Supervisor 1",
          status: "error",
          archivedAt: null,
          labels: {},
        },
        {
          id: "sup-2",
          workspaceId: "ws-1",
          provider: "claude",
          model: "claude-sonnet-4-6",
          cwd: "/repo",
          title: "Supervisor 2",
          status: "idle",
          archivedAt: null,
          labels: { [LABEL.previousSupervisorId]: "sup-1" },
        },
        {
          id: "sub-2",
          workspaceId: "ws-1",
          provider: "opencode",
          model: "qwen-coder",
          cwd: "/repo",
          title: "Worker",
          status: "running",
          archivedAt: null,
          labels: {
            [PASEO_PARENT_AGENT_ID_LABEL]: "sup-1",
            [LABEL.taskId]: "sub-1",
            [LABEL.rootParentAgentId]: "sup-1",
            [LABEL.supervisorAgentId]: "sup-1",
            [LABEL.sequence]: "1",
          },
        },
      ],
    });

    // Fresh coordinator simulates a clean restart with empty in-memory maps.
    const restarted = new TaskCoordinator();
    await restarted.ensureHydrated(fake.paseo);

    expect(restarted.resolveSupervisor("sup-1")).toBe("sup-2");
    expect(restarted.getTask("sub-1")).toMatchObject({
      taskId: "sub-1",
      originalAgentId: "sub-1",
      currentAgentId: "sub-2",
      originalParentId: "sup-1",
      currentSupervisorId: "sup-2",
      switchCount: 1,
    });

    // When `sub-2` finishes after restart, its result routes to `sup-2`.
    const delivered = await restarted.onSubagentTurnEnded(
      fake.paseo,
      {
        agent: { id: "sub-2", parentAgentId: "sup-1", provider: "opencode", title: "Worker" },
        turnId: "post-restart-turn",
        outcome: { kind: "completed" },
        timeline: [{ type: "assistant_message", text: "Recovered after restart." }],
      },
      makeSettings(),
    );
    expect(delivered).toBe(true);
    expect(fake.sentMessages).toHaveLength(1);
    expect(fake.sentMessages[0]!.agentId).toBe("sup-2");
    expect(fake.sentMessages[0]!.text).toContain("Recovered after restart.");
  });
});
