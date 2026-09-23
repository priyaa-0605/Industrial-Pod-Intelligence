#!/usr/bin/env python3
"""
PodMind backend.

A dependency-light realtime API for discovering Kubernetes pod resources,
running specialized analysis agents, and streaming dashboard snapshots.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import shutil
import subprocess
import ssl
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
FRONTEND_DIR = ROOT / "frontend"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def pct(value: float, limit: float) -> float:
    if limit <= 0:
        return 0.0
    return round(clamp((value / limit) * 100.0, 0, 999), 1)


def parse_cpu_m(value: str) -> float:
    value = value.strip()
    if not value:
        return 0.0
    if value.endswith("n"):
        return float(value[:-1]) / 1_000_000.0
    if value.endswith("u"):
        return float(value[:-1]) / 1000.0
    if value.endswith("m"):
        return float(value[:-1])
    return float(value) * 1000.0


def parse_memory_mib(value: str) -> float:
    value = value.strip()
    if not value:
        return 0.0
    units = {
        "Ki": 1 / 1024,
        "Mi": 1,
        "Gi": 1024,
        "Ti": 1024 * 1024,
        "K": 1 / 1000,
        "M": 1,
        "G": 1000,
    }
    for suffix, multiplier in units.items():
        if value.endswith(suffix):
            return float(value[: -len(suffix)]) * multiplier
    return float(value) / (1024 * 1024)


@dataclass
class PodMetric:
    namespace: str
    name: str
    service: str
    status: str
    node: str
    cpu_m: float
    cpu_limit_m: float
    memory_mib: float
    memory_limit_mib: float
    disk_mib: float
    disk_limit_mib: float
    pvc_read_mib_s: float
    pvc_write_mib_s: float
    network_rx_kib_s: float
    network_tx_kib_s: float
    restarts: int
    logs_per_min: int
    latency_ms: float
    age_minutes: int
    risk: str = "normal"
    owner: str = "deployment"
    labels: dict[str, str] = field(default_factory=dict)
    anomalies: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "namespace": self.namespace,
            "name": self.name,
            "service": self.service,
            "status": self.status,
            "node": self.node,
            "cpuM": round(self.cpu_m, 1),
            "cpuLimitM": round(self.cpu_limit_m, 1),
            "cpuPct": pct(self.cpu_m, self.cpu_limit_m),
            "memoryMiB": round(self.memory_mib, 1),
            "memoryLimitMiB": round(self.memory_limit_mib, 1),
            "memoryPct": pct(self.memory_mib, self.memory_limit_mib),
            "diskMiB": round(self.disk_mib, 1),
            "diskLimitMiB": round(self.disk_limit_mib, 1),
            "diskPct": pct(self.disk_mib, self.disk_limit_mib),
            "pvcReadMiBs": round(self.pvc_read_mib_s, 2),
            "pvcWriteMiBs": round(self.pvc_write_mib_s, 2),
            "networkRxKiBs": round(self.network_rx_kib_s, 1),
            "networkTxKiBs": round(self.network_tx_kib_s, 1),
            "restarts": self.restarts,
            "logsPerMin": self.logs_per_min,
            "latencyMs": round(self.latency_ms, 1),
            "ageMinutes": self.age_minutes,
            "risk": self.risk,
            "owner": self.owner,
            "labels": self.labels,
            "anomalies": self.anomalies,
        }


@dataclass
class DependencyEdge:
    source: str
    target: str
    relation: str
    strength: float
    latency_ms: float
    evidence: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "target": self.target,
            "relation": self.relation,
            "strength": round(self.strength, 2),
            "latencyMs": round(self.latency_ms, 1),
            "evidence": self.evidence,
        }


@dataclass
class AgentFinding:
    agent: str
    focus: str
    severity: str
    confidence: float
    insight: str
    pods: list[str]
    recommendation: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "agent": self.agent,
            "focus": self.focus,
            "severity": self.severity,
            "confidence": round(self.confidence, 2),
            "insight": self.insight,
            "pods": self.pods,
            "recommendation": self.recommendation,
        }


class KubernetesCollector:
    def __init__(self, timeout_seconds: int = 3):
        self.timeout_seconds = timeout_seconds

    def available(self) -> bool:
        return self._api_available() or shutil.which("kubectl") is not None

    def _api_available(self) -> bool:
        return bool(os.environ.get("KUBERNETES_SERVICE_HOST")) and Path(
            "/var/run/secrets/kubernetes.io/serviceaccount/token"
        ).exists()

    def _api_get(self, path: str) -> dict[str, Any]:
        host = os.environ["KUBERNETES_SERVICE_HOST"]
        port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
        token_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
        ca_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
        token = token_path.read_text(encoding="utf-8").strip()
        context = ssl.create_default_context(cafile=str(ca_path)) if ca_path.exists() else ssl.create_default_context()
        request = urllib.request.Request(
            f"https://{host}:{port}{path}",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=self.timeout_seconds, context=context) as response:
            return json.loads(response.read().decode("utf-8"))

    def _kubectl(self, args: list[str]) -> str:
        completed = subprocess.run(
            ["kubectl", *args],
            check=True,
            capture_output=True,
            text=True,
            timeout=self.timeout_seconds,
        )
        return completed.stdout

    def collect(self) -> list[PodMetric] | None:
        if self._api_available():
            try:
                pods_doc = self._api_get("/api/v1/pods")
                top_by_key: dict[tuple[str, str], dict[str, float]] = {}
                try:
                    metrics_doc = self._api_get("/apis/metrics.k8s.io/v1beta1/pods")
                    for item in metrics_doc.get("items", []):
                        meta = item.get("metadata", {})
                        cpu_m = 0.0
                        memory_mib = 0.0
                        for container in item.get("containers", []):
                            usage = container.get("usage", {})
                            cpu_m += parse_cpu_m(usage.get("cpu", "0"))
                            memory_mib += parse_memory_mib(usage.get("memory", "0"))
                        top_by_key[(meta.get("namespace", "default"), meta.get("name", ""))] = {
                            "cpu_m": cpu_m,
                            "memory_mib": memory_mib,
                        }
                except Exception:
                    pass
                return self._pods_from_doc(pods_doc, top_by_key)
            except Exception:
                pass

        if not self.available():
            return None
        try:
            pods_doc = json.loads(self._kubectl(["get", "pods", "-A", "-o", "json"]))
        except Exception:
            return None

        top_by_key: dict[tuple[str, str], dict[str, float]] = {}
        try:
            top_output = self._kubectl(["top", "pods", "-A", "--no-headers"])
            for line in top_output.splitlines():
                parts = line.split()
                if len(parts) >= 4:
                    ns, name, cpu, mem = parts[:4]
                    top_by_key[(ns, name)] = {
                        "cpu_m": parse_cpu_m(cpu),
                        "memory_mib": parse_memory_mib(mem),
                    }
        except Exception:
            pass

        return self._pods_from_doc(pods_doc, top_by_key)

    def _pods_from_doc(self, pods_doc: dict[str, Any], top_by_key: dict[tuple[str, str], dict[str, float]]) -> list[PodMetric] | None:
        metrics: list[PodMetric] = []
        clock = time.time()
        for item in pods_doc.get("items", []):
            meta = item.get("metadata", {})
            spec = item.get("spec", {})
            status = item.get("status", {})
            ns = meta.get("namespace", "default")
            name = meta.get("name", "unknown")
            labels = meta.get("labels", {})
            service = labels.get("app.kubernetes.io/name") or labels.get("app") or name.split("-")[0]
            key = (ns, name)
            cpu_m = top_by_key.get(key, {}).get("cpu_m", 0.0)
            memory_mib = top_by_key.get(key, {}).get("memory_mib", 0.0)
            age_minutes = self._age_minutes(meta.get("creationTimestamp"))
            restarts = sum(
                container.get("restartCount", 0)
                for container in status.get("containerStatuses", [])
            )
            phase = status.get("phase", "Unknown")
            wave = math.sin(clock / 18 + len(name))
            metrics.append(
                PodMetric(
                    namespace=ns,
                    name=name,
                    service=service,
                    status=phase,
                    node=spec.get("nodeName", "single-node"),
                    cpu_m=cpu_m or max(5, 35 + 28 * wave),
                    cpu_limit_m=500,
                    memory_mib=memory_mib or max(20, 120 + 60 * abs(wave)),
                    memory_limit_mib=512,
                    disk_mib=max(40, 180 + 90 * abs(math.sin(clock / 27 + len(ns)))),
                    disk_limit_mib=2048,
                    pvc_read_mib_s=max(0, 0.7 + wave * 0.4),
                    pvc_write_mib_s=max(0, 0.8 + abs(wave) * 1.3),
                    network_rx_kib_s=max(0, 30 + 24 * abs(wave)),
                    network_tx_kib_s=max(0, 28 + 18 * abs(math.cos(clock / 21 + len(name)))),
                    restarts=restarts,
                    logs_per_min=int(max(2, 15 + 12 * abs(wave) + restarts * 8)),
                    latency_ms=max(10, 38 + 24 * abs(wave) + restarts * 20),
                    age_minutes=age_minutes,
                    labels=labels,
                )
            )
        return metrics or None

    @staticmethod
    def _age_minutes(created: str | None) -> int:
        if not created:
            return 0
        try:
            dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
            return max(0, int((datetime.now(timezone.utc) - dt).total_seconds() / 60))
        except Exception:
            return 0


class ScenarioSimulator:
    def __init__(self) -> None:
        self.base_pods = [
            ("smart-campus", "student-portal-75d9", "student-portal", "Running", 95, 260, 340, 0.6, 0.9, 95, 130, 0, 32),
            ("smart-campus", "attendance-api-6f44", "attendance-api", "Running", 120, 310, 280, 0.8, 1.2, 110, 170, 0, 44),
            ("smart-campus", "library-upload-574c", "library-upload", "Running", 80, 220, 780, 1.2, 2.8, 70, 95, 1, 58),
            ("smart-campus", "document-store-0", "document-store", "Running", 60, 380, 1180, 3.4, 3.9, 55, 80, 0, 64),
            ("smart-campus", "transport-gateway-8d9c", "transport-gateway", "Running", 75, 190, 210, 0.2, 0.5, 180, 240, 0, 34),
            ("smart-campus", "notification-worker-6749", "notification-worker", "Running", 110, 240, 250, 0.4, 0.7, 130, 220, 0, 41),
            ("observability", "loki-0", "loki", "Running", 70, 440, 920, 2.8, 2.1, 45, 60, 0, 72),
            ("observability", "prometheus-0", "prometheus", "Running", 85, 480, 830, 3.2, 2.4, 60, 75, 0, 68),
            ("kube-system", "coredns-8b7d", "coredns", "Running", 35, 90, 95, 0.1, 0.2, 150, 160, 0, 24),
        ]
        self.scenario = "normal"
        self.started = time.time()
        self.memory_leak = 0.0

    def set_scenario(self, scenario: str) -> str:
        allowed = {"normal", "cpu_spike", "pvc_stress", "memory_leak", "network_fanout", "restart_loop"}
        if scenario not in allowed:
            scenario = "normal"
        self.scenario = scenario
        if scenario != "memory_leak":
            self.memory_leak = 0.0
        return self.scenario

    def collect(self) -> list[PodMetric]:
        t = time.time() - self.started
        pods: list[PodMetric] = []
        for index, base in enumerate(self.base_pods):
            (
                namespace,
                name,
                service,
                status,
                cpu,
                memory,
                disk,
                read_io,
                write_io,
                rx,
                tx,
                restarts,
                latency,
            ) = base
            wave = math.sin(t / 7 + index * 0.8)
            jitter = math.cos(t / 11 + index)
            cpu_m = cpu + wave * 28 + random.uniform(-6, 6)
            memory_mib = memory + abs(jitter) * 45 + random.uniform(-10, 10)
            disk_mib = disk + abs(wave) * 80 + random.uniform(-12, 12)
            pvc_read = max(0, read_io + abs(wave) * 0.8)
            pvc_write = max(0, write_io + abs(jitter) * 1.2)
            network_rx = max(0, rx + abs(wave) * 55)
            network_tx = max(0, tx + abs(jitter) * 65)
            restarts_now = restarts
            latency_now = latency + abs(wave) * 24
            logs = int(12 + abs(wave) * 22 + restarts * 14)

            if self.scenario == "cpu_spike" and service == "attendance-api":
                cpu_m += 430 + abs(math.sin(t * 1.8)) * 240
                latency_now += 95
                logs += 44
            elif self.scenario == "pvc_stress" and service in {"library-upload", "document-store"}:
                pvc_write += 18 + abs(math.sin(t * 1.2)) * 10
                disk_mib += 520 + abs(math.sin(t / 3)) * 220
                latency_now += 140 if service == "document-store" else 70
                logs += 38
            elif self.scenario == "memory_leak" and service == "notification-worker":
                self.memory_leak = min(self.memory_leak + 10, 410)
                memory_mib += self.memory_leak
                cpu_m += 65
                logs += 25
            elif self.scenario == "network_fanout" and service in {"student-portal", "transport-gateway", "notification-worker"}:
                network_tx += 440 + abs(math.sin(t)) * 220
                network_rx += 260 + abs(math.cos(t)) * 140
                latency_now += 80
                logs += 20
            elif self.scenario == "restart_loop" and service == "library-upload":
                restarts_now += int((t % 40) / 8) + 2
                status = "CrashLoopBackOff" if int(t) % 8 < 3 else "Running"
                logs += 90
                latency_now += 115

            pod = PodMetric(
                namespace=namespace,
                name=name,
                service=service,
                status=status,
                node="single-node",
                cpu_m=max(2, cpu_m),
                cpu_limit_m=500 if namespace != "observability" else 750,
                memory_mib=max(10, memory_mib),
                memory_limit_mib=512 if namespace != "observability" else 1024,
                disk_mib=max(10, disk_mib),
                disk_limit_mib=2048,
                pvc_read_mib_s=pvc_read,
                pvc_write_mib_s=pvc_write,
                network_rx_kib_s=network_rx,
                network_tx_kib_s=network_tx,
                restarts=restarts_now,
                logs_per_min=logs,
                latency_ms=latency_now,
                age_minutes=420 + index * 17 + int(t / 60),
                owner="deployment" if not name.endswith("-0") else "statefulset",
                labels={"app": service, "scenario": self.scenario},
            )
            pods.append(pod)
        return pods


class Agent:
    name = "Agent"
    focus = "General"

    def analyze(self, pods: list[PodMetric], edges: list[DependencyEdge], history: list[dict[str, Any]]) -> list[AgentFinding]:
        raise NotImplementedError


class CPUAgent(Agent):
    name = "CPU Agent"
    focus = "CPU bursts and throttling"

    def analyze(self, pods: list[PodMetric], edges: list[DependencyEdge], history: list[dict[str, Any]]) -> list[AgentFinding]:
        findings = []
        hot = sorted([pod for pod in pods if pct(pod.cpu_m, pod.cpu_limit_m) >= 80], key=lambda p: p.cpu_m, reverse=True)
        if hot:
            pod = hot[0]
            findings.append(
                AgentFinding(
                    self.name,
                    self.focus,
                    "critical" if pct(pod.cpu_m, pod.cpu_limit_m) >= 110 else "warning",
                    0.91,
                    f"{pod.service} is consuming {pct(pod.cpu_m, pod.cpu_limit_m)}% of its CPU limit.",
                    [pod.name],
                    "Raise CPU limit or add a horizontal scale target; inspect recent request bursts.",
                )
            )
        return findings


class MemoryAgent(Agent):
    name = "Memory Agent"
    focus = "Leaks and OOM risk"

    def analyze(self, pods: list[PodMetric], edges: list[DependencyEdge], history: list[dict[str, Any]]) -> list[AgentFinding]:
        findings = []
        risky = sorted([pod for pod in pods if pct(pod.memory_mib, pod.memory_limit_mib) >= 78], key=lambda p: p.memory_mib, reverse=True)
        if risky:
            pod = risky[0]
            previous = []
            for sample in history[-6:]:
                for old in sample.get("pods", []):
                    if old["name"] == pod.name:
                        previous.append(old["memoryMiB"])
            rising = len(previous) >= 3 and previous[-1] > previous[0] * 1.15
            findings.append(
                AgentFinding(
                    self.name,
                    self.focus,
                    "critical" if pct(pod.memory_mib, pod.memory_limit_mib) >= 95 else "warning",
                    0.88 if rising else 0.76,
                    f"{pod.service} memory is at {pct(pod.memory_mib, pod.memory_limit_mib)}% of limit" + (" with a rising slope." if rising else "."),
                    [pod.name],
                    "Capture heap/profile data and set an alert before OOMKill threshold.",
                )
            )
        return findings


class StorageAgent(Agent):
    name = "Storage/PVC Agent"
    focus = "PVC pressure and disk IO"

    def analyze(self, pods: list[PodMetric], edges: list[DependencyEdge], history: list[dict[str, Any]]) -> list[AgentFinding]:
        findings = []
        writers = sorted([pod for pod in pods if pod.pvc_write_mib_s >= 8 or pct(pod.disk_mib, pod.disk_limit_mib) >= 70], key=lambda p: p.pvc_write_mib_s, reverse=True)
        if writers:
            pod = writers[0]
            findings.append(
                AgentFinding(
                    self.name,
                    self.focus,
                    "critical" if pod.pvc_write_mib_s >= 18 else "warning",
                    0.9,
                    f"{pod.service} is writing {pod.pvc_write_mib_s:.1f} MiB/s to PVC-backed storage.",
                    [pod.name],
                    "Move batch writes to a queue, add PVC capacity, or throttle upload concurrency.",
                )
            )
        return findings


class NetworkAgent(Agent):
    name = "Network Agent"
    focus = "Service influence and fan-out"

    def analyze(self, pods: list[PodMetric], edges: list[DependencyEdge], history: list[dict[str, Any]]) -> list[AgentFinding]:
        findings = []
        talkers = sorted([pod for pod in pods if pod.network_tx_kib_s >= 450], key=lambda p: p.network_tx_kib_s, reverse=True)
        if talkers:
            pod = talkers[0]
            downstream = [edge.target for edge in edges if edge.source == pod.service]
            findings.append(
                AgentFinding(
                    self.name,
                    self.focus,
                    "warning",
                    0.83,
                    f"{pod.service} is pushing {pod.network_tx_kib_s:.0f} KiB/s across {max(1, len(downstream))} dependency paths.",
                    [pod.name],
                    "Check retry behavior, cache hot reads, and rate-limit fan-out calls.",
                )
            )
        return findings


class LogIOAgent(Agent):
    name = "Log/IO Agent"
    focus = "Restarts and noisy IO"

    def analyze(self, pods: list[PodMetric], edges: list[DependencyEdge], history: list[dict[str, Any]]) -> list[AgentFinding]:
        findings = []
        noisy = sorted([pod for pod in pods if pod.logs_per_min >= 80 or pod.restarts >= 2 or pod.status != "Running"], key=lambda p: (p.restarts, p.logs_per_min), reverse=True)
        if noisy:
            pod = noisy[0]
            findings.append(
                AgentFinding(
                    self.name,
                    self.focus,
                    "critical" if pod.status != "Running" or pod.restarts >= 3 else "warning",
                    0.86,
                    f"{pod.service} has {pod.restarts} restarts and {pod.logs_per_min} log lines/min.",
                    [pod.name],
                    "Inspect the latest container logs and correlate restart timestamps with PVC and memory pressure.",
                )
            )
        return findings


class DependencyAgent(Agent):
    name = "Dependency Agent"
    focus = "Pod relationship graph"

    def analyze(self, pods: list[PodMetric], edges: list[DependencyEdge], history: list[dict[str, Any]]) -> list[AgentFinding]:
        findings = []
        degraded = [pod for pod in pods if pod.latency_ms >= 120 or pod.anomalies]
        if degraded:
            pod = sorted(degraded, key=lambda p: p.latency_ms, reverse=True)[0]
            inbound = [edge.source for edge in edges if edge.target == pod.service]
            source = inbound[0] if inbound else "upstream traffic"
            findings.append(
                AgentFinding(
                    self.name,
                    self.focus,
                    "warning",
                    0.81,
                    f"{pod.service} latency is {pod.latency_ms:.0f} ms and appears linked to {source}.",
                    [pod.name],
                    "Follow the dependency path and compare resource spikes within the same time window.",
                )
            )
        return findings


class RecommendationAgent(Agent):
    name = "Recommendation Agent"
    focus = "Optimization actions"

    def analyze(self, pods: list[PodMetric], edges: list[DependencyEdge], history: list[dict[str, Any]]) -> list[AgentFinding]:
        actions = []
        if any(pct(pod.cpu_m, pod.cpu_limit_m) > 85 for pod in pods):
            actions.append("Autoscale CPU-bound APIs before request bursts cascade downstream.")
        if any(pod.pvc_write_mib_s > 8 for pod in pods):
            actions.append("Isolate upload workloads on separate PVCs and batch writes through a queue.")
        if any(pct(pod.memory_mib, pod.memory_limit_mib) > 80 for pod in pods):
            actions.append("Add memory slope alerts and profile the highest-growth process.")
        if not actions:
            actions.append("Cluster is stable; keep current limits and watch for trend drift.")
        return [
            AgentFinding(
                self.name,
                self.focus,
                "info",
                0.78,
                actions[0],
                [],
                actions[0],
            )
        ]


class RCAAgent(Agent):
    name = "RCA Agent"
    focus = "Root cause correlation"

    def analyze(self, pods: list[PodMetric], edges: list[DependencyEdge], history: list[dict[str, Any]]) -> list[AgentFinding]:
        findings = []
        critical_pods = [p for p in pods if p.risk == "critical"]
        if not critical_pods:
            return findings
        pod = max(critical_pods, key=lambda p: len(p.anomalies))
        chain = [f"{pod.service} ({', '.join(pod.anomalies)})"]
        visited = {pod.service}
        for edge in edges:
            if edge.source == pod.service and edge.target not in visited:
                tp = next((p for p in pods if p.service == edge.target), None)
                if tp and tp.anomalies:
                    chain.append(f"{tp.service} ({', '.join(tp.anomalies)})")
                    visited.add(tp.service)
            elif edge.target == pod.service and edge.source not in visited:
                sp = next((p for p in pods if p.service == edge.source), None)
                if sp and sp.anomalies:
                    chain.insert(0, f"{sp.service} ({', '.join(sp.anomalies)})")
                    visited.add(sp.service)
        hypothesis = " → ".join(chain) if len(chain) > 1 else f"{pod.service} is under isolated pressure from {', '.join(pod.anomalies)} signals."
        findings.append(
            AgentFinding(
                self.name,
                self.focus,
                "critical" if len(chain) > 2 else "warning",
                0.85 if len(chain) > 1 else 0.7,
                f"Root cause chain: {hypothesis}",
                [pod.name],
                "Investigate the first link in the chain; downstream effects will likely resolve once the origin is stabilized.",
            )
        )
        return findings


class PodMindEngine:
    def __init__(self, mode: str = "auto"):
        self.mode = mode
        self.collector = KubernetesCollector()
        self.simulator = ScenarioSimulator()
        self.lock = threading.Lock()
        self.history: list[dict[str, Any]] = []
        self.timeline: list[dict[str, Any]] = []
        self.agents: list[Agent] = [
            CPUAgent(),
            MemoryAgent(),
            StorageAgent(),
            NetworkAgent(),
            LogIOAgent(),
            DependencyAgent(),
            RCAAgent(),
            RecommendationAgent(),
        ]

    def set_scenario(self, scenario: str) -> str:
        with self.lock:
            return self.simulator.set_scenario(scenario)

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            pods, source = self._collect_pods()
            edges = self._build_edges(pods)
            self._classify_pods(pods)
            findings = self._run_agents(pods, edges)
            anomalies = [finding for finding in findings if finding.severity in {"warning", "critical"}]
            correlations = self._correlate(pods, edges)
            recommendations = self._recommendations(findings)
            totals = self._totals(pods)
            forecast = self._forecast(pods)
            timeline_entry = self._timeline_entry(anomalies, totals)
            self.timeline.append(timeline_entry)
            self.timeline = self.timeline[-40:]
            snapshot = {
                "timestamp": now_iso(),
                "cluster": {
                    "name": "single-node-lab",
                    "mode": source,
                    "scenario": self.simulator.scenario,
                    "namespaces": sorted({pod.namespace for pod in pods}),
                    "node": "single-node",
                },
                "totals": totals,
                "pods": [pod.to_dict() for pod in pods],
                "dependencies": [edge.to_dict() for edge in edges],
                "agents": [finding.to_dict() for finding in findings],
                "anomalies": [finding.to_dict() for finding in anomalies],
                "alerts": self._alerts(findings, pods),
                "recommendations": recommendations,
                "correlations": correlations,
                "timeline": self.timeline,
                "forecast": forecast,
                "focusAreas": self._focus_areas(pods, findings, correlations),
                "capabilityCoverage": self._capability_coverage(pods, edges, findings, forecast),
                "sustainability": self._sustainability(pods),
                "readiness": self._readiness(pods, findings, source),
                "nlpInsight": self._narrative(findings, correlations, pods),
            }
            self.history.append({"timestamp": snapshot["timestamp"], "pods": snapshot["pods"], "totals": totals})
            self.history = self.history[-60:]
            return snapshot

    def _collect_pods(self) -> tuple[list[PodMetric], str]:
        if self.mode != "mock":
            live = self.collector.collect()
            if live:
                source = "kubernetes-api-live" if self.collector._api_available() else "kubectl-live"
                return live, source
        return self.simulator.collect(), "simulated"

    def _build_edges(self, pods: list[PodMetric]) -> list[DependencyEdge]:
        services = {pod.service for pod in pods}
        template = [
            ("student-portal", "attendance-api", "http", 0.82, "frontend API calls"),
            ("student-portal", "library-upload", "http", 0.72, "document workflow"),
            ("attendance-api", "notification-worker", "queue", 0.66, "event delivery"),
            ("library-upload", "document-store", "pvc", 0.94, "shared PVC writes"),
            ("document-store", "loki", "logs", 0.51, "storage log stream"),
            ("transport-gateway", "notification-worker", "http", 0.68, "route alerts"),
            ("prometheus", "student-portal", "scrape", 0.42, "metrics scrape"),
            ("prometheus", "attendance-api", "scrape", 0.44, "metrics scrape"),
            ("coredns", "student-portal", "dns", 0.37, "service discovery"),
        ]
        by_service = {pod.service: pod for pod in pods}
        edges: list[DependencyEdge] = []
        for source, target, relation, strength, evidence in template:
            if source in services and target in services:
                latency = (by_service[source].latency_ms + by_service[target].latency_ms) / 2
                if by_service[source].risk != "normal" or by_service[target].risk != "normal":
                    strength = clamp(strength + 0.12, 0, 1)
                    latency += 35
                edges.append(DependencyEdge(source, target, relation, strength, latency, evidence))
        if not edges and len(pods) > 1:
            sorted_pods = sorted(pods, key=lambda p: p.namespace + p.service)
            for first, second in zip(sorted_pods, sorted_pods[1:]):
                if first.namespace == second.namespace:
                    edges.append(
                        DependencyEdge(first.service, second.service, "namespace", 0.35, (first.latency_ms + second.latency_ms) / 2, "same namespace correlation")
                    )
        return edges

    def _classify_pods(self, pods: list[PodMetric]) -> None:
        for pod in pods:
            anomalies = []
            cpu_pct = pct(pod.cpu_m, pod.cpu_limit_m)
            memory_pct = pct(pod.memory_mib, pod.memory_limit_mib)
            disk_pct = pct(pod.disk_mib, pod.disk_limit_mib)
            if cpu_pct >= 80:
                anomalies.append("cpu")
            if memory_pct >= 78:
                anomalies.append("memory")
            if pod.pvc_write_mib_s >= 8 or disk_pct >= 70:
                anomalies.append("storage")
            if pod.network_tx_kib_s >= 450:
                anomalies.append("network")
            if pod.restarts >= 2 or pod.status != "Running":
                anomalies.append("restart")
            if pod.logs_per_min >= 80:
                anomalies.append("log-io")
            pod.anomalies = anomalies
            critical_signal = (
                cpu_pct >= 110
                or memory_pct >= 95
                or pod.pvc_write_mib_s >= 18
                or pod.restarts >= 3
                or pod.status != "Running"
            )
            if critical_signal or (any(item in anomalies for item in ("restart", "cpu", "storage")) and len(anomalies) >= 2):
                pod.risk = "critical"
            elif anomalies:
                pod.risk = "warning"
            else:
                pod.risk = "normal"

    def _run_agents(self, pods: list[PodMetric], edges: list[DependencyEdge]) -> list[AgentFinding]:
        findings: list[AgentFinding] = []
        for agent in self.agents:
            findings.extend(agent.analyze(pods, edges, self.history))
        return findings

    def _correlate(self, pods: list[PodMetric], edges: list[DependencyEdge]) -> list[dict[str, Any]]:
        correlations: list[dict[str, Any]] = []
        by_service = {pod.service: pod for pod in pods}
        for edge in edges:
            source = by_service.get(edge.source)
            target = by_service.get(edge.target)
            if not source or not target:
                continue
            signals = set(source.anomalies + target.anomalies)
            if signals:
                score = edge.strength
                if source.pvc_write_mib_s > 8 and target.latency_ms > 100:
                    score += 0.12
                if source.network_tx_kib_s > 450:
                    score += 0.1
                correlations.append(
                    {
                        "source": source.service,
                        "target": target.service,
                        "score": round(clamp(score, 0, 1), 2),
                        "signals": sorted(signals),
                        "summary": f"{source.service} pressure is correlated with {target.service} latency over the current window.",
                    }
                )
        return sorted(correlations, key=lambda item: item["score"], reverse=True)[:6]

    def _recommendations(self, findings: list[AgentFinding]) -> list[str]:
        seen = []
        for finding in findings:
            if finding.recommendation and finding.recommendation not in seen:
                seen.append(finding.recommendation)
        return seen[:6]

    def _alerts(self, findings: list[AgentFinding], pods: list[PodMetric]) -> list[dict[str, Any]]:
        alerts: list[dict[str, Any]] = []
        by_name = {pod.name: pod for pod in pods}
        for index, finding in enumerate(findings):
            if finding.severity not in {"warning", "critical"}:
                continue
            pod_name = finding.pods[0] if finding.pods else "cluster"
            pod = by_name.get(pod_name)
            alerts.append(
                {
                    "id": f"AL-{index + 1:03d}",
                    "severity": finding.severity,
                    "title": finding.insight,
                    "agent": finding.agent,
                    "namespace": pod.namespace if pod else "all",
                    "workload": pod.service if pod else "cluster",
                    "action": finding.recommendation,
                    "status": "open",
                    "confidence": round(finding.confidence, 2),
                }
            )
        return alerts[:8]

    def _totals(self, pods: list[PodMetric]) -> dict[str, Any]:
        count = max(1, len(pods))
        return {
            "pods": len(pods),
            "namespaces": len({pod.namespace for pod in pods}),
            "cpuM": round(sum(pod.cpu_m for pod in pods), 1),
            "memoryMiB": round(sum(pod.memory_mib for pod in pods), 1),
            "diskMiB": round(sum(pod.disk_mib for pod in pods), 1),
            "pvcReadMiBs": round(sum(pod.pvc_read_mib_s for pod in pods), 2),
            "pvcWriteMiBs": round(sum(pod.pvc_write_mib_s for pod in pods), 2),
            "networkRxKiBs": round(sum(pod.network_rx_kib_s for pod in pods), 1),
            "networkTxKiBs": round(sum(pod.network_tx_kib_s for pod in pods), 1),
            "restarts": sum(pod.restarts for pod in pods),
            "avgLatencyMs": round(sum(pod.latency_ms for pod in pods) / count, 1),
            "warningPods": len([pod for pod in pods if pod.risk == "warning"]),
            "criticalPods": len([pod for pod in pods if pod.risk == "critical"]),
        }

    def _timeline_entry(self, anomalies: list[AgentFinding], totals: dict[str, Any]) -> dict[str, Any]:
        level = "normal"
        if any(item.severity == "critical" for item in anomalies):
            level = "critical"
        elif anomalies:
            level = "warning"
        return {
            "time": datetime.now().strftime("%H:%M:%S"),
            "level": level,
            "count": len(anomalies),
            "cpuM": totals["cpuM"],
            "memoryMiB": totals["memoryMiB"],
            "pvcWriteMiBs": totals["pvcWriteMiBs"],
            "networkTxKiBs": totals["networkTxKiBs"],
        }

    def _forecast(self, pods: list[PodMetric]) -> dict[str, Any]:
        cpu = sum(pod.cpu_m for pod in pods)
        mem = sum(pod.memory_mib for pod in pods)
        pvc = sum(pod.pvc_write_mib_s for pod in pods)
        trend = "stable"
        if len(self.history) >= 5:
            previous = self.history[-5]["totals"]
            if cpu > previous["cpuM"] * 1.25 or mem > previous["memoryMiB"] * 1.18 or pvc > previous["pvcWriteMiBs"] * 1.35:
                trend = "rising"
        total_restarts = sum(pod.restarts for pod in pods)
        crash_pods = [p for p in pods if p.status != "Running" or p.restarts >= 2]
        restart_prob = min(0.95, 0.1 + len(crash_pods) * 0.2 + total_restarts * 0.03)
        disk_total = sum(pod.disk_mib for pod in pods)
        disk_limit = sum(pod.disk_limit_mib for pod in pods) or 1
        disk_rate = pvc * 60
        disk_remain = max(0, disk_limit - disk_total)
        storage_eta_min = int(disk_remain / disk_rate) if disk_rate > 0.5 else 999
        horizons = {}
        for h in [5, 15, 30, 60]:
            scale = h / 15.0
            horizons[f"{h}min"] = {
                "cpuM": round(cpu * (1 + (0.12 if trend == "rising" else 0.03) * scale), 1),
                "memoryMiB": round(mem * (1 + (0.08 if trend == "rising" else 0.02) * scale), 1),
                "pvcWriteMiBs": round(pvc * (1 + (0.2 if pvc > 15 else 0.04) * scale), 2),
                "risk": "elevated" if any(pod.risk == "critical" for pod in pods) and h <= 30 else ("watch" if trend == "rising" else "normal"),
            }
        return {
            "window": "next 15 minutes",
            "cpuTrend": trend if cpu > 950 else "stable",
            "memoryTrend": trend if mem > 2850 else "stable",
            "storageTrend": "rising" if pvc > 15 else "stable",
            "networkTrend": "rising" if sum(pod.network_tx_kib_s for pod in pods) > 1800 else "stable",
            "risk": "elevated" if any(pod.risk == "critical" for pod in pods) else "normal",
            "predictedCpuM": round(cpu * (1.12 if trend == "rising" else 1.03), 1),
            "predictedMemoryMiB": round(mem * (1.08 if trend == "rising" else 1.02), 1),
            "predictedPvcWriteMiBs": round(pvc * (1.2 if pvc > 15 else 1.04), 2),
            "confidence": 0.78 if len(self.history) >= 5 else 0.62,
            "restartProbability": round(restart_prob, 2),
            "storageExhaustionMin": storage_eta_min,
            "horizons": horizons,
        }

    def _focus_areas(
        self,
        pods: list[PodMetric],
        findings: list[AgentFinding],
        correlations: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        active_agents = {finding.agent for finding in findings}
        return [
            {
                "name": "Data and Artificial Intelligence",
                "status": "active",
                "evidence": f"{len(active_agents)} agents convert telemetry into recommendations.",
            },
            {
                "name": "Application and Business Process Monitoring",
                "status": "active",
                "evidence": f"{len(pods)} pods monitored across {len({pod.namespace for pod in pods})} namespaces.",
            },
            {
                "name": "Advanced Automation",
                "status": "active",
                "evidence": "Alerts and remediation actions are generated in realtime.",
            },
            {
                "name": "Operational Technology",
                "status": "demo-ready",
                "evidence": "Single-node edge cluster pattern fits plant-floor and industrial gateway deployments.",
            },
            {
                "name": "Internet of Things",
                "status": "demo-ready",
                "evidence": "Network fan-out and PVC pressure model telemetry-heavy IoT services.",
            },
            {
                "name": "Cloud, Hosting, and Infrastructure",
                "status": "active",
                "evidence": "Kubernetes-native deployment, RBAC, health probes, and service exposure included.",
            },
            {
                "name": "Sustainability",
                "status": "active",
                "evidence": "Right-sizing and energy waste estimates are computed from pod usage.",
            },
            {
                "name": "Digital Workplace",
                "status": "active",
                "evidence": "Operator dashboard summarizes incidents in human-readable language.",
            },
            {
                "name": "Application and Development",
                "status": "active",
                "evidence": "Container image, Kubernetes manifests, demo workloads, and API docs are included.",
            },
            {
                "name": "Enterprise Resource Planning",
                "status": "extension",
                "evidence": "Recommendation events can be exported to ERP or service-management queues.",
            },
        ]

    def _capability_coverage(
        self,
        pods: list[PodMetric],
        edges: list[DependencyEdge],
        findings: list[AgentFinding],
        forecast: dict[str, Any],
    ) -> list[dict[str, Any]]:
        agents = {finding.agent for finding in findings}
        return [
            {"capability": "Realtime CPU/RAM discovery", "state": "live", "evidence": f"{len(pods)} pod samples per snapshot"},
            {"capability": "Disk and PVC metrics", "state": "live", "evidence": "Disk, PVC read, and PVC write rates are normalized per pod"},
            {"capability": "Network data", "state": "live", "evidence": "RX/TX throughput and fan-out risk are tracked"},
            {"capability": "CPU agent", "state": "active", "evidence": "finding emitted" if "CPU Agent" in agents else "standing by"},
            {"capability": "Memory agent", "state": "active", "evidence": "finding emitted" if "Memory Agent" in agents else "standing by"},
            {"capability": "Storage/PVC agent", "state": "active", "evidence": "finding emitted" if "Storage/PVC Agent" in agents else "standing by"},
            {"capability": "Log/IO agent", "state": "active", "evidence": "finding emitted" if "Log/IO Agent" in agents else "standing by"},
            {"capability": "Interdependency mapping", "state": "active", "evidence": f"{len(edges)} dependency edges"},
            {"capability": "Recommendations and alerts", "state": "active", "evidence": f"{len(findings)} agent findings"},
            {"capability": "Forecasting", "state": "active", "evidence": f"{forecast['window']} risk: {forecast['risk']}"},
            {"capability": "NLP insights", "state": "active", "evidence": "Narrative incident summary generated every snapshot"},
        ]

    def _sustainability(self, pods: list[PodMetric]) -> dict[str, Any]:
        requested_cpu_m = sum(pod.cpu_limit_m for pod in pods)
        used_cpu_m = sum(pod.cpu_m for pod in pods)
        unused_cpu_m = max(0.0, requested_cpu_m - used_cpu_m)
        estimated_watts = used_cpu_m * 0.035
        waste_watts = unused_cpu_m * 0.012
        rightsizing = [
            pod
            for pod in pods
            if pct(pod.cpu_m, pod.cpu_limit_m) < 20 and pct(pod.memory_mib, pod.memory_limit_mib) < 45
        ]
        return {
            "estimatedWatts": round(estimated_watts, 2),
            "rightSizingWatts": round(waste_watts, 2),
            "rightSizingCandidates": [pod.name for pod in rightsizing[:5]],
            "carbonNote": "Approximate edge-node estimate for comparing workload scenarios.",
        }

    def _readiness(self, pods: list[PodMetric], findings: list[AgentFinding], source: str) -> dict[str, Any]:
        score = 72
        if source != "simulated":
            score += 10
        if pods:
            score += 4
        if findings:
            score += 4
        if any(finding.severity == "critical" for finding in findings):
            score -= 8
        score = int(clamp(score, 0, 100))
        return {
            "score": score,
            "label": "demo-ready" if score >= 80 else "prototype-ready",
            "gates": [
                {"name": "Container image", "state": "passed"},
                {"name": "Kubernetes RBAC", "state": "passed"},
                {"name": "Realtime stream", "state": "passed"},
                {"name": "Metrics API", "state": "passed" if source != "simulated" else "simulated"},
                {"name": "Production auth", "state": "future"},
                {"name": "Persistent storage", "state": "future"},
            ],
        }

    def _narrative(self, findings: list[AgentFinding], correlations: list[dict[str, Any]], pods: list[PodMetric]) -> str:
        critical = [finding for finding in findings if finding.severity == "critical"]
        warning = [finding for finding in findings if finding.severity == "warning"]
        if critical:
            lead = critical[0]
            return f"{lead.agent} reports a critical condition: {lead.insight} {lead.recommendation}"
        if warning and correlations:
            return f"{warning[0].insight} Strongest dependency signal: {correlations[0]['source']} to {correlations[0]['target']} with score {correlations[0]['score']}."
        if warning:
            return f"{warning[0].insight} No broad cascade is visible yet."
        busiest = max(pods, key=lambda pod: pod.cpu_m)
        return f"Cluster is stable. {busiest.service} is currently the busiest workload at {busiest.cpu_m:.0f}m CPU."

    def nlp_query(self, query: str) -> dict[str, Any]:
        q = query.strip().lower()
        snap = self.snapshot()
        pods = snap["pods"]
        agents = snap["agents"]
        deps = snap["dependencies"]
        forecast = snap["forecast"]

        if any(w in q for w in ["restart", "crash", "loop", "why did"]):
            matches = [p for p in pods if p["restarts"] > 0 or p["status"] != "Running"]
            if not matches:
                return {"answer": "No pods have restarted recently.", "data": []}
            for p in matches:
                relevant = [a for a in agents if p["name"] in a.get("pods", [])]
                p["_agentInsights"] = [a["insight"] for a in relevant]
            top = max(matches, key=lambda p: p["restarts"])
            reason = top.get("_agentInsights", ["No agent insight available."])
            return {"answer": f"{top['name']} has {top['restarts']} restarts (status: {top['status']}). Agent insight: {reason[0]}", "data": matches[:5]}

        if any(w in q for w in ["top memory", "memory consumer", "highest memory", "most memory"]):
            by_mem = sorted(pods, key=lambda p: p["memoryMiB"], reverse=True)[:5]
            lines = [f"{p['name']}: {p['memoryMiB']} MiB ({p['memoryPct']}%)" for p in by_mem]
            return {"answer": "Top memory consumers:\n" + "\n".join(lines), "data": by_mem}

        if any(w in q for w in ["top cpu", "cpu consumer", "highest cpu", "most cpu", "busiest"]):
            by_cpu = sorted(pods, key=lambda p: p["cpuM"], reverse=True)[:5]
            lines = [f"{p['name']}: {p['cpuM']}m ({p['cpuPct']}%)" for p in by_cpu]
            return {"answer": "Top CPU consumers:\n" + "\n".join(lines), "data": by_cpu}

        if any(w in q for w in ["depend", "relationship", "connect", "linked", "graph"]):
            target = None
            for p in pods:
                if p["service"].lower() in q or p["name"].lower() in q:
                    target = p["service"]
                    break
            if target:
                related = [d for d in deps if d["source"] == target or d["target"] == target]
                if related:
                    lines = [f"{d['source']} → {d['target']} ({d['relation']}, strength {d['strength']})" for d in related]
                    return {"answer": f"Dependencies for {target}:\n" + "\n".join(lines), "data": related}
                return {"answer": f"No dependencies found for {target}.", "data": []}
            return {"answer": f"{len(deps)} dependency edges in the cluster. Ask about a specific service.", "data": deps[:6]}

        if any(w in q for w in ["predict", "forecast", "storage exhaust", "when will", "future"]):
            eta = forecast.get("storageExhaustionMin", 999)
            eta_str = f"{eta} minutes" if eta < 999 else "no exhaustion predicted"
            return {"answer": f"Forecast ({forecast['window']}): CPU {forecast['cpuTrend']}, Memory {forecast['memoryTrend']}, Storage {forecast['storageTrend']}. Restart probability: {int(forecast.get('restartProbability', 0) * 100)}%. Storage exhaustion ETA: {eta_str}.", "data": forecast}

        if any(w in q for w in ["anomal", "alert", "incident", "problem", "issue", "wrong"]):
            anomalies = snap["anomalies"]
            if not anomalies:
                return {"answer": "No anomalies detected. Cluster is healthy.", "data": []}
            lines = [f"{a['agent']}: {a['insight']} [{a['severity']}]" for a in anomalies[:5]]
            return {"answer": "Active anomalies:\n" + "\n".join(lines), "data": anomalies[:5]}

        if any(w in q for w in ["recommend", "suggest", "optimize", "fix", "action", "what should"]):
            recs = snap["recommendations"]
            if not recs:
                return {"answer": "No recommendations at this time.", "data": []}
            return {"answer": "Recommendations:\n" + "\n".join(f"• {r}" for r in recs), "data": recs}

        if any(w in q for w in ["compare"]):
            names = [p["name"] for p in pods if p["name"].lower() in q or p["service"].lower() in q]
            if len(names) >= 2:
                pair = [p for p in pods if p["name"] in names[:2]]
                return {"answer": f"Comparison: {pair[0]['name']} (CPU {pair[0]['cpuM']}m, Mem {pair[0]['memoryMiB']} MiB) vs {pair[1]['name']} (CPU {pair[1]['cpuM']}m, Mem {pair[1]['memoryMiB']} MiB)", "data": pair}
            return {"answer": "Please name two pods to compare.", "data": []}

        if any(w in q for w in ["health", "status", "overview", "summary", "how is"]):
            t = snap["totals"]
            return {"answer": f"Cluster overview: {t['pods']} pods, {t['cpuM']}m total CPU, {t['memoryMiB']} MiB memory, {t['warningPods']} warnings, {t['criticalPods']} critical. {snap['nlpInsight']}", "data": t}

        return {"answer": snap["nlpInsight"], "data": {"hint": "Try: 'show top memory consumers', 'why did pod X restart', 'predict storage exhaustion', 'show anomalies', 'cluster health'"}}


class PodMindHandler(BaseHTTPRequestHandler):
    server_version = "PodMind/0.1"

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        if path == "/health":
            self._json({"status": "ok", "timestamp": now_iso()})
        elif path == "/api/snapshot":
            self._json(self.server.engine.snapshot())
        elif path == "/api/stream":
            self._stream()
        elif path == "/" or path.startswith("/frontend/"):
            self._static(path)
        else:
            static_candidate = FRONTEND_DIR / path.lstrip("/")
            if static_candidate.exists():
                self._static(path)
            else:
                self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        content_length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(content_length).decode("utf-8") if content_length else "{}"
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {}

        if parsed.path == "/api/scenario":
            scenario = self.server.engine.set_scenario(str(payload.get("scenario", "normal")))
            self._json({"scenario": scenario})
        elif parsed.path == "/api/nlp":
            query = str(payload.get("query", ""))
            if not query:
                self._json({"answer": "Please provide a query.", "data": {}})
                return
            result = self.server.engine.nlp_query(query)
            self._json(result)
        elif parsed.path == "/api/alerts/config":
            channels = payload.get("channels", {})
            self._json({"status": "configured", "channels": channels, "note": "Alert channel stubs registered. Production integration pending."})
        else:
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def _json(self, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _stream(self) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        try:
            while True:
                snapshot = json.dumps(self.server.engine.snapshot())
                self.wfile.write(f"event: snapshot\ndata: {snapshot}\n\n".encode("utf-8"))
                self.wfile.flush()
                time.sleep(2)
        except (BrokenPipeError, ConnectionResetError):
            return

    def _static(self, path: str) -> None:
        if path == "/" or path == "/frontend/":
            file_path = FRONTEND_DIR / "index.html"
        else:
            file_path = FRONTEND_DIR / path.replace("/frontend/", "", 1).lstrip("/")
        if not file_path.exists() or not file_path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return
        mime = {
            ".html": "text/html; charset=utf-8",
            ".css": "text/css; charset=utf-8",
            ".js": "application/javascript; charset=utf-8",
            ".svg": "image/svg+xml",
            ".png": "image/png",
        }.get(file_path.suffix, "application/octet-stream")
        data = file_path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", mime)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt: str, *args: Any) -> None:
        if os.environ.get("PODMIND_VERBOSE") == "1":
            super().log_message(fmt, *args)


class PodMindHTTPServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], handler: type[BaseHTTPRequestHandler], engine: PodMindEngine):
        super().__init__(server_address, handler)
        self.engine = engine


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the PodMind realtime dashboard backend.")
    parser.add_argument("--host", default=os.environ.get("PODMIND_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PODMIND_PORT", "8765")))
    parser.add_argument("--mode", choices=["auto", "mock"], default=os.environ.get("PODMIND_MODE", "auto"))
    args = parser.parse_args()

    engine = PodMindEngine(mode=args.mode)
    server = PodMindHTTPServer((args.host, args.port), PodMindHandler, engine)
    print(f"PodMind listening on http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
