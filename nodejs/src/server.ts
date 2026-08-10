import fs from "node:fs";
import path from "node:path";
import { spawn, type ChildProcessByStdio } from "node:child_process";
import type { Readable } from "node:stream";

import { StateletClient, type StateletClientOptions } from "./client";

const DEFAULT_START_TIMEOUT_MS = 15000;
const DEFAULT_STOP_TIMEOUT_MS = 5000;

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

type ManagedChildProcess = ChildProcessByStdio<null, Readable, Readable>;

function onceExit(
  child: ManagedChildProcess
): Promise<{ code: number | null; signal: NodeJS.Signals | null }> {
  return new Promise((resolve) => {
    child.once("exit", (code, signal) => resolve({ code, signal }));
  });
}

function isProcessRunning(child: ManagedChildProcess): boolean {
  return child.exitCode === null && child.signalCode === null;
}

function ensureString(value: unknown, fieldName: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new TypeError(`${fieldName} must be a non-empty string`);
  }
  return value;
}

function inferRepoRoot(): string {
  return path.resolve(__dirname, "..", "..", "..");
}

function inferBinaryPath(repoRoot: string, binaryName: string): string {
  return path.join(repoRoot, "target", "debug", binaryName);
}

function normalizeSpawnEnv(env?: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  return {
    ...process.env,
    ...env,
  };
}

export interface StartStandaloneOptions {
  repoRoot?: string;
  binaryPath?: string;
  dbPath?: string;
  grpcAddr?: string;
  env?: NodeJS.ProcessEnv;
  startTimeoutMs?: number;
  clientOptions?: StateletClientOptions;
}

interface StateletServerInternalOptions {
  mode: "standalone";
  binaryPath: string;
  dbPath: string;
  grpcAddr: string;
  process: ManagedChildProcess;
  readyClientOptions?: StateletClientOptions;
}

export class StateletServer {
  readonly mode: "standalone";
  readonly binaryPath: string;
  readonly dbPath: string;
  readonly grpcAddr: string;
  readonly process: ManagedChildProcess;
  stdout = "";
  stderr = "";

  private readonly readyClientOptions: StateletClientOptions;
  private readonly waitForExitPromise: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;

  private constructor(options: StateletServerInternalOptions) {
    this.mode = options.mode;
    this.binaryPath = options.binaryPath;
    this.dbPath = options.dbPath;
    this.grpcAddr = options.grpcAddr;
    this.process = options.process;
    this.readyClientOptions = options.readyClientOptions || {};
    this.waitForExitPromise = onceExit(this.process);

    this.process.stdout.on("data", (chunk: Buffer) => {
      this.stdout += chunk.toString("utf8");
    });

    this.process.stderr.on("data", (chunk: Buffer) => {
      this.stderr += chunk.toString("utf8");
    });
  }

  static async startStandalone(options: StartStandaloneOptions = {}): Promise<StateletServer> {
    const repoRoot = options.repoRoot || inferRepoRoot();
    const binaryPath = options.binaryPath || inferBinaryPath(repoRoot, "raft_engine");
    const dbPath = ensureString(options.dbPath || path.join(repoRoot, ".statelet-nodejs"), "dbPath");
    const grpcAddr = ensureString(options.grpcAddr || "127.0.0.1:7379", "grpcAddr");
    const startTimeoutMs = options.startTimeoutMs ?? DEFAULT_START_TIMEOUT_MS;
    const childEnv = normalizeSpawnEnv(options.env);

    if (!fs.existsSync(binaryPath)) {
      throw new Error(
        `Statelet binary not found at ${binaryPath}. Build it first with: cargo build --features data-node --bin raft_engine`
      );
    }

    const child = spawn(binaryPath, [dbPath, grpcAddr], {
      cwd: repoRoot,
      env: childEnv,
      stdio: ["ignore", "pipe", "pipe"],
    });

    const server = new StateletServer({
      mode: "standalone",
      binaryPath,
      dbPath,
      grpcAddr,
      process: child,
      readyClientOptions: options.clientOptions,
    });

    await server.waitUntilReady(startTimeoutMs);
    return server;
  }

  createClient(clientOptions: StateletClientOptions = {}): StateletClient {
    return new StateletClient(this.grpcAddr, clientOptions);
  }

  async waitUntilReady(timeoutMs: number = DEFAULT_START_TIMEOUT_MS): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    let lastError: Error | null = null;

    while (Date.now() < deadline) {
      if (!isProcessRunning(this.process)) {
        const exit = await this.waitForExitPromise;
        throw new Error(
          `Statelet server exited before becoming ready (code=${exit.code}, signal=${exit.signal}). stderr:\n${this.stderr}`
        );
      }

      const client = this.createClient(this.readyClientOptions);
      try {
        await client.waitForReady(1000);
        await client.ping();
        client.close();
        return;
      } catch (error) {
        lastError = error as Error;
        client.close();
        await delay(200);
      }
    }

    throw new Error(
      `Timed out waiting for Statelet server at ${this.grpcAddr}. Last error: ${lastError ? lastError.message : "unknown"}`
    );
  }

  async stop(timeoutMs: number = DEFAULT_STOP_TIMEOUT_MS): Promise<{ code: number | null; signal: NodeJS.Signals | null }> {
    if (!isProcessRunning(this.process)) {
      return this.waitForExitPromise;
    }

    this.process.kill("SIGTERM");

    const timeout = new Promise<null>((resolve) => {
      setTimeout(() => resolve(null), timeoutMs);
    });

    const exit = await Promise.race([this.waitForExitPromise, timeout]);
    if (exit) {
      return exit;
    }

    this.process.kill("SIGKILL");
    return this.waitForExitPromise;
  }

  kill(signal: NodeJS.Signals = "SIGKILL"): void {
    if (isProcessRunning(this.process)) {
      this.process.kill(signal);
    }
  }
}
