import type { PluginServerContext } from "@getpaseo/plugin/server";
import {
  failoverNowRpc,
  getCoordinationStateRpc,
  getSettingsRpc,
  type CoordinationSnapshot,
} from "./shared/contracts";
import { PASEO_PARENT_AGENT_ID_LABEL } from "./shared/labels";
import { DEFAULT_SETTINGS, continuitySettings, type ContinuitySettings } from "./shared/settings";
import { classifyTurn } from "./server/classify";
import { TaskCoordinator } from "./server/coordination";
import { errorText, loadTimeline, runFailover, type FailoverResult } from "./server/failover";
import { ContinuityState } from "./server/state";

export default function contribute(server: PluginServerContext) {
  const settingsHandle = server.registerSettings(continuitySettings);
  const state = new ContinuityState();
  const coordinator = new TaskCoordinator();
  let settings: ContinuitySettings = DEFAULT_SETTINGS;
  let stopped = false;

  void settingsHandle.read().then((current) => {
    if (current.status === "ready") {
      settings = current.values;
    } else {
      console.error("[continuity] settings invalid, using defaults:", current.error);
    }
  });
  const unsubscribe = settingsHandle.subscribe((next) => {
    if (next.status === "ready") {
      settings = next.values;
      state.clearExhaustedChains();
      state.clearHandled();
    }
  });

  server.on("agent.created", (event) => {
    if (stopped || event.agent.parentAgentId === null) return;
    coordinator.registerSubagent({
      id: event.agent.id,
      parentAgentId: event.agent.parentAgentId,
      provider: event.agent.provider,
      title: event.agent.title,
    });
  });

  server.on("agent.archived", (event) => {
    if (stopped) return;
    coordinator.markArchived(event.agent.id);
  });

  server.on("agent.turn_ended", (event, { paseo }) => {
    if (stopped) return;

    const isSubagent = event.agent.parentAgentId !== null;
    const relayIfNeeded = () => {
      if (!isSubagent) return;
      void coordinator.onSubagentTurnEnded(paseo, event, settings).catch((error: unknown) => {
        console.error("[continuity] result relay failed", event.agent.id, errorText(error));
      });
    };

    if (!settings.enabled || settings.chain.length < 2) {
      relayIfNeeded();
      return;
    }
    if (event.outcome.kind === "canceled") {
      relayIfNeeded();
      return;
    }
    if (isSubagent && !settings.includeSubagents) {
      // Preserve default behavior: do not auto-failover subagents when disabled,
      // but still relay results if the subagent's supervisor previously failed over.
      relayIfNeeded();
      return;
    }
    if (state.isHandled(event.agent.id) || state.isInFlight(event.agent.id)) return;

    const classification = classifyTurn(event.outcome, event.timeline, settings.triggers);
    if (!classification) {
      relayIfNeeded();
      return;
    }
    if (!state.startInFlight(event.agent.id)) return;

    console.log("[continuity] failover triggered", {
      agentId: event.agent.id,
      parentAgentId: event.agent.parentAgentId,
      provider: event.agent.provider,
      class: classification.class,
      evidence: classification.evidence,
    });

    // Detached: agent creation can outlast the 30 s hook timeout.
    void runFailover(paseo, settings, state, {
      agent: event.agent,
      turnId: event.turnId,
      timeline: event.timeline,
      reason: classification.class,
      evidence: classification.evidence,
      coordinator,
      ...(classification.resetAt === undefined ? {} : { resetAt: classification.resetAt }),
    })
      .then((result) => {
        if (result.status === "created") {
          state.claim(event.agent.id);
        } else {
          // If failover did not create a successor (exhausted, halted, failed),
          // release so the user can retry once they configure new providers.
          state.release(event.agent.id);
        }
      })
      .catch((error: unknown) => {
        state.release(event.agent.id);
        console.error("[continuity] failover crashed", event.agent.id, errorText(error));
      })
      .finally(() => {
        state.finishInFlight(event.agent.id);
      });
  });

  server.handle(getSettingsRpc, async (): Promise<ContinuitySettings> => {
    return settings;
  });

  server.handle(getCoordinationStateRpc, async (): Promise<CoordinationSnapshot> => {
    return coordinator.snapshot();
  });

  server.handle(failoverNowRpc, async ({ agentId, targetProvider, handoff }, { paseo }): Promise<FailoverResult> => {
    if (settings.chain.length < 2) {
      return { status: "skipped", newAgentId: null, message: "Configure at least two providers in the fallback chain." };
    }
    if (state.isInFlight(agentId)) {
      return { status: "skipped", newAgentId: null, message: "Failover is already in progress for this agent." };
    }
    if (!state.startInFlight(agentId)) {
      return { status: "skipped", newAgentId: null, message: "Failover is already in progress for this agent." };
    }
    try {
      const handle = paseo.agents.ref(agentId);
      const refreshed = await handle.refresh();
      if (!refreshed) {
        return { status: "failed", newAgentId: null, message: `Agent ${agentId} was not found.` };
      }
      const timeline = await loadTimeline(paseo, agentId);
      const agent = refreshed.agent;
      const result = await runFailover(paseo, settings, state, {
        agent: {
          id: agent.id,
          workspaceId: agent.workspaceId ?? null,
          parentAgentId: agent.labels?.[PASEO_PARENT_AGENT_ID_LABEL] ?? null,
          provider: agent.provider,
          cwd: agent.cwd,
          title: agent.title,
        },
        turnId: null,
        timeline,
        reason: "manual",
        evidence: "",
        coordinator,
        ...(targetProvider ? { targetProvider } : {}),
        ...(handoff ? { handoff } : {}),
      });
      if (result.status === "created") {
        state.claim(agentId);
      } else {
        state.release(agentId);
      }
      return result;
    } catch (error) {
      state.release(agentId);
      return { status: "failed", newAgentId: null, message: errorText(error) };
    } finally {
      state.finishInFlight(agentId);
    }
  });

  return () => {
    stopped = true;
    unsubscribe();
  };
}
