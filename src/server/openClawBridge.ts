import { spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { createReadStream, existsSync } from 'node:fs'
import { appendFile, copyFile, mkdir, readdir, readFile, stat, unlink, writeFile } from 'node:fs/promises'
import { pathToFileURL } from 'node:url'
import { basename, dirname, extname, isAbsolute, join } from 'node:path'
import { createInterface } from 'node:readline'
import { tmpdir } from 'node:os'
import express, { type RequestHandler } from 'express'
import { resolveAppServerRuntimeConfig } from './appServerRuntimeConfig.js'

const DEFAULT_GATEWAY_URL = 'ws://127.0.0.1:18789'
const DEFAULT_OPENCLAW_ENTRY = 'C:\\Users\\JD\\AppData\\Roaming\\npm\\node_modules\\openclaw\\openclaw.mjs'
const OPENCLAW_ID_PREFIX = 'openclaw::'
const DEFAULT_CODEX_FAST_TIMEOUT_MS = 900_000

type OpenClawSessionRow = Record<string, unknown> & {
  key?: string
  sessionId?: string
  displayName?: string
  label?: string
  updatedAt?: number
  sessionFile?: string
}

type OpenClawAttachment = {
  label?: string
  path?: string
  fsPath?: string
}

type OpenClawCommandRow = {
  name: string
  description: string
  source: string
}

type OpenClawModelRow = {
  id: string
  name: string
  isDefault: boolean
}

type OpenClawAgentRow = {
  id: string
  name: string
  isDefault: boolean
  primaryModel: string
}

const OPENCLAW_METADATA_CACHE_TTL_MS = 10 * 60 * 1000
let cachedModels: { rows: OpenClawModelRow[]; atMs: number } | null = null
let cachedCommands: { rows: OpenClawCommandRow[]; atMs: number } | null = null
let modelsRefreshPromise: Promise<OpenClawModelRow[]> | null = null
let commandsRefreshPromise: Promise<OpenClawCommandRow[]> | null = null
let gatewayCallRuntimePromise: Promise<((opts: Record<string, unknown>) => Promise<Record<string, unknown>>) | null> | null = null
let gatewayClientClassPromise: Promise<(new (options: Record<string, unknown>) => {
  start: () => void
  stop: () => void
  request: (method: string, params?: Record<string, unknown>, options?: { expectFinal?: boolean; timeoutMs?: number | null }) => Promise<Record<string, unknown>>
}) | null> | null = null

function readString(value: unknown): string {
  return typeof value === 'string' ? value.trim() : ''
}

function readNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}

function normalizeOpenClawModelOverride(model: string): string {
  const normalized = model.trim()
  if (normalized.toLowerCase().startsWith('openai-codex/gpt-')) {
    return `codex/${normalized.slice('openai-codex/'.length)}`
  }
  return normalized
}

function encodeThreadId(sessionKey: string): string {
  return `${OPENCLAW_ID_PREFIX}${encodeURIComponent(sessionKey)}`
}

function decodeThreadId(threadId: string): string {
  return threadId.startsWith(OPENCLAW_ID_PREFIX)
    ? decodeURIComponent(threadId.slice(OPENCLAW_ID_PREFIX.length))
    : threadId
}

function readRouteParam(value: string | string[] | undefined): string {
  if (typeof value === 'string') return value
  if (Array.isArray(value)) return value[0] ?? ''
  return ''
}

function agentIdFromSessionKey(sessionKey: string): string {
  const match = /^agent:([^:]+):/.exec(sessionKey)
  return match?.[1] ?? ''
}

function toIso(value: unknown): string {
  const ms = readNumber(value)
  if (!ms) return new Date().toISOString()
  return new Date(ms).toISOString()
}

function runOpenClaw(args: string[], timeoutMs = 120_000): Promise<string> {
  return new Promise((resolve, reject) => {
    const entry = process.env.OPENCLAW_ENTRY || DEFAULT_OPENCLAW_ENTRY
    const command = process.env.OPENCLAW_NODE || process.execPath
    const child = spawn(command, [entry, ...args], {
      windowsHide: true,
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    let stdout = ''
    let stderr = ''
    const timer = setTimeout(() => {
      child.kill()
      reject(new Error(`openclaw ${args.join(' ')} timed out after ${timeoutMs}ms`))
    }, timeoutMs)

    child.stdout.on('data', (chunk) => {
      stdout += String(chunk)
    })
    child.stderr.on('data', (chunk) => {
      stderr += String(chunk)
    })
    child.on('error', (error) => {
      clearTimeout(timer)
      reject(error)
    })
    child.on('close', (code) => {
      clearTimeout(timer)
      if (code === 0) {
        resolve(stdout)
        return
      }
      reject(new Error((stderr || stdout || `openclaw exited with code ${code}`).trim()))
    })
  })
}

function runOpenClawStreaming(
  args: string[],
  onOutput: (chunk: string, stream: 'stdout' | 'stderr') => void,
  timeoutMs = 120_000,
): Promise<string> {
  return new Promise((resolve, reject) => {
    const entry = process.env.OPENCLAW_ENTRY || DEFAULT_OPENCLAW_ENTRY
    const command = process.env.OPENCLAW_NODE || process.execPath
    const startedAtMs = Date.now()
    let firstStdoutAtMs = 0
    let firstStderrAtMs = 0
    const child = spawn(command, [entry, ...args], {
      windowsHide: true,
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    let stdout = ''
    let stderr = ''
    const timer = setTimeout(() => {
      child.kill()
      reject(new Error(`openclaw ${args.join(' ')} timed out after ${timeoutMs}ms`))
    }, timeoutMs)

    child.stdout.on('data', (chunk) => {
      const text = String(chunk)
      if (!firstStdoutAtMs) firstStdoutAtMs = Date.now()
      stdout += text
      onOutput(text, 'stdout')
    })
    child.stderr.on('data', (chunk) => {
      const text = String(chunk)
      if (!firstStderrAtMs) firstStderrAtMs = Date.now()
      stderr += text
      onOutput(text, 'stderr')
    })
    child.on('error', (error) => {
      clearTimeout(timer)
      reject(error)
    })
    child.on('close', (code) => {
      clearTimeout(timer)
      const completedAtMs = Date.now()
      const safeArgs = args.map((arg, index) => {
        const previous = args[index - 1]
        return previous === '--message' ? `[message:${arg.length} chars]` : arg
      })
      console.info('[openclaw-stream]', JSON.stringify({
        code,
        durationMs: completedAtMs - startedAtMs,
        firstStdoutMs: firstStdoutAtMs ? firstStdoutAtMs - startedAtMs : null,
        firstStderrMs: firstStderrAtMs ? firstStderrAtMs - startedAtMs : null,
        args: safeArgs,
      }))
      if (code === 0) {
        resolve(stdout)
        return
      }
      reject(new Error((stderr || stdout || `openclaw exited with code ${code}`).trim()))
    })
  })
}

function parseJsonObject(output: string): Record<string, unknown> {
  const trimmed = output.trim()
  if (!trimmed) return {}
  try {
    return JSON.parse(trimmed) as Record<string, unknown>
  } catch {
    const start = trimmed.indexOf('{')
    const end = trimmed.lastIndexOf('}')
    if (start >= 0 && end > start) {
      return JSON.parse(trimmed.slice(start, end + 1)) as Record<string, unknown>
    }
    throw new Error('OpenClaw returned non-JSON output')
  }
}

async function gatewayCall(method: string, params: Record<string, unknown> = {}, timeoutMs = 30_000): Promise<Record<string, unknown>> {
  const gatewayUrl = process.env.OPENCLAW_GATEWAY_URL || DEFAULT_GATEWAY_URL
  const password = process.env.OPENCLAW_GATEWAY_PASSWORD?.trim()
  if (!password) throw new Error('OPENCLAW_GATEWAY_PASSWORD is required for OpenClaw gateway calls')
  const output = await runOpenClaw([
    'gateway',
    'call',
    method,
    '--json',
    '--url',
    gatewayUrl,
    '--password',
    password,
    '--params',
    JSON.stringify(params),
  ], timeoutMs)
  return parseJsonObject(output)
}

async function loadGatewayCallRuntime(): Promise<((opts: Record<string, unknown>) => Promise<Record<string, unknown>>) | null> {
  if (gatewayCallRuntimePromise) return gatewayCallRuntimePromise
  gatewayCallRuntimePromise = (async () => {
    try {
      const entry = process.env.OPENCLAW_ENTRY || DEFAULT_OPENCLAW_ENTRY
      const distDir = join(dirname(entry), 'dist')
      const files = await readdir(distDir)
      const callFiles = files.filter((name) => /^call-[\w-]+\.js$/u.test(name))
      for (const callFile of callFiles) {
        const mod = await import(pathToFileURL(join(distDir, callFile)).href) as {
          i?: (opts: Record<string, unknown>) => Promise<Record<string, unknown>>
        }
        if (typeof mod.i === 'function') return mod.i
      }
      return null
    } catch (error) {
      console.warn('[openclaw-gateway-runtime] unavailable', error instanceof Error ? error.message : error)
      return null
    }
  })()
  return gatewayCallRuntimePromise
}

async function directGatewayCall(method: string, params: Record<string, unknown>, options: {
  expectFinal?: boolean
  timeoutMs?: number
} = {}): Promise<Record<string, unknown>> {
  const callGateway = await loadGatewayCallRuntime()
  if (!callGateway) throw new Error('OpenClaw gateway runtime is unavailable')
  return await callGateway({
    method,
    params,
    expectFinal: options.expectFinal === true,
    timeoutMs: options.timeoutMs ?? 30_000,
    clientName: 'gateway-client',
    clientDisplayName: 'codexclaw',
    mode: 'backend',
    scopes: [
      'operator.admin',
      'operator.read',
      'operator.write',
      'operator.approvals',
      'operator.pairing',
      'talk.secrets',
    ],
  })
}

async function getOpenClawGatewayPassword(): Promise<string> {
  const openClawHome = getOpenClawHome()
  if (!openClawHome) return ''
  const config = await readJsonFile(join(openClawHome, 'openclaw.json'))
  return readString(((config?.gateway as Record<string, unknown> | undefined)?.auth as Record<string, unknown> | undefined)?.password)
}

async function loadGatewayClientClass(): Promise<(new (options: Record<string, unknown>) => {
  start: () => void
  stop: () => void
  request: (method: string, params?: Record<string, unknown>, options?: { expectFinal?: boolean; timeoutMs?: number | null }) => Promise<Record<string, unknown>>
}) | null> {
  if (gatewayClientClassPromise) return gatewayClientClassPromise
  gatewayClientClassPromise = (async () => {
    try {
      const entry = process.env.OPENCLAW_ENTRY || DEFAULT_OPENCLAW_ENTRY
      const distDir = join(dirname(entry), 'dist')
      const files = await readdir(distDir)
      const clientFiles = files.filter((name) => /^client-[\w-]+\.js$/u.test(name))
      const clientModules: Record<string, unknown>[] = []
      for (const clientFile of clientFiles) {
        if (clientFile.includes('bootstrap') || clientFile.includes('info') || clientFile.includes('readiness') || clientFile.includes('factory')) continue
        try {
          clientModules.push(await import(pathToFileURL(join(distDir, clientFile)).href) as Record<string, unknown>)
        } catch {
          continue
        }
      }
      for (const candidates of [
        clientModules.map((clientModule) => clientModule.n),
        clientModules.map((clientModule) => clientModule.GatewayClient),
      ]) {
        const GatewayClient = candidates.find((candidate) => {
          if (typeof candidate !== 'function') return false
          const prototype = (candidate as { prototype?: Record<string, unknown> }).prototype
          return Boolean(
            prototype
            && typeof prototype.start === 'function'
            && typeof prototype.stop === 'function'
            && typeof prototype.request === 'function',
          )
        })
        if (GatewayClient) {
          return GatewayClient as new (options: Record<string, unknown>) => {
            start: () => void
            stop: () => void
            request: (method: string, params?: Record<string, unknown>, options?: { expectFinal?: boolean; timeoutMs?: number | null }) => Promise<Record<string, unknown>>
          }
        }
      }
      throw new Error('OpenClaw GatewayClient export missing')
    } catch (error) {
      console.warn('[openclaw-gateway-runtime] client unavailable', error instanceof Error ? error.message : error)
      return null
    }
  })()
  return gatewayClientClassPromise
}

async function transientGatewayCall(method: string, params: Record<string, unknown>, options: {
  expectFinal?: boolean
  timeoutMs?: number | null
} = {}): Promise<Record<string, unknown>> {
  const GatewayClient = await loadGatewayClientClass()
  if (!GatewayClient) throw new Error('OpenClaw gateway client is unavailable')
  const password = await getOpenClawGatewayPassword()
  const client = await new Promise<{
    request: (method: string, params?: Record<string, unknown>, options?: { expectFinal?: boolean; timeoutMs?: number | null }) => Promise<Record<string, unknown>>
    stop: () => void
  }>((resolve, reject) => {
    const gatewayClient = new GatewayClient({
      url: 'ws://127.0.0.1:18789',
      password,
      deviceIdentity: null,
      clientName: 'gateway-client',
      clientDisplayName: 'codexclaw-dispatch',
      mode: 'backend',
      role: 'operator',
      scopes: ['operator.admin'],
      onHelloOk: () => resolve(gatewayClient),
      onConnectError: (error: unknown) => reject(error instanceof Error ? error : new Error(String(error))),
    })
    gatewayClient.start()
  })
  try {
    return await client.request(method, params, {
      expectFinal: options.expectFinal === true,
      timeoutMs: options.timeoutMs ?? 30_000,
    })
  } finally {
    client.stop()
  }
}

async function runOpenClawGatewayAgent(params: Record<string, unknown>, timeoutMs = 30_000): Promise<Record<string, unknown>> {
  const entry = process.env.OPENCLAW_ENTRY || DEFAULT_OPENCLAW_ENTRY
  const script = `
    import { pathToFileURL } from 'node:url';
    import { dirname, join } from 'node:path';
    import { readdir } from 'node:fs/promises';
    const params = JSON.parse(process.argv[1]);
    const distDir = join(dirname(${JSON.stringify(entry)}), 'dist');
    const callFiles = (await readdir(distDir)).filter((name) => /^call-[\\w-]+\\.js$/u.test(name));
    let callGateway = null;
    for (const callFile of callFiles) {
      const mod = await import(pathToFileURL(join(distDir, callFile)).href);
      if (typeof mod.i === 'function') {
        callGateway = mod.i;
        break;
      }
    }
    if (typeof callGateway !== 'function') throw new Error('OpenClaw call runtime export missing');
    const result = await callGateway({
      method: 'agent',
      params,
      expectFinal: false,
      timeoutMs: ${timeoutMs},
      clientName: 'gateway-client',
      clientDisplayName: 'codexclaw-helper',
      mode: 'backend',
      scopes: ['operator.admin','operator.read','operator.write','operator.approvals','operator.pairing','talk.secrets'],
    });
    console.log(JSON.stringify(result ?? {}));
  `
  const output = await new Promise<string>((resolve, reject) => {
    const child = spawn(process.execPath, ['--input-type=module', '-e', script, JSON.stringify(params)], {
      windowsHide: true,
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    let stdout = ''
    let stderr = ''
    const timer = setTimeout(() => {
      child.kill()
      reject(new Error(`OpenClaw gateway helper timed out after ${timeoutMs}ms`))
    }, timeoutMs + 5_000)
    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString()
    })
    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString()
    })
    child.on('error', (error) => {
      clearTimeout(timer)
      reject(error)
    })
    child.on('close', (code) => {
      clearTimeout(timer)
      if (code === 0) {
        resolve(stdout)
        return
      }
      reject(new Error((stderr || stdout || `OpenClaw gateway helper exited with code ${code}`).trim()))
    })
  })
  return parseJsonObject(output)
}

function getOpenClawHome(): string {
  const configured = process.env.OPENCLAW_HOME?.trim()
  if (configured) return configured
  const home = process.env.USERPROFILE || process.env.HOME || ''
  return home ? join(home, '.openclaw') : ''
}

async function getOpenClawWorkspace(agentId: string): Promise<string> {
  const openClawHome = getOpenClawHome()
  const fallback = openClawHome ? join(openClawHome, 'workspace') : ''
  const config = openClawHome ? await readJsonFile(join(openClawHome, 'openclaw.json')) : null
  const agents = config?.agents && typeof config.agents === 'object' && !Array.isArray(config.agents)
    ? config.agents as Record<string, unknown>
    : {}
  const defaults = agents.defaults && typeof agents.defaults === 'object' && !Array.isArray(agents.defaults)
    ? agents.defaults as Record<string, unknown>
    : {}
  const defaultWorkspace = readString(defaults.workspace) || fallback
  const list = Array.isArray(agents.list) ? agents.list : []
  const agent = list
    .filter((entry): entry is Record<string, unknown> => Boolean(entry && typeof entry === 'object' && !Array.isArray(entry)))
    .find((entry) => readString(entry.id) === agentId)
  return readString(agent?.workspace) || defaultWorkspace
}

function isLikelyLocalPath(value: string): boolean {
  if (!value) return false
  if (value.startsWith('file://')) return true
  return isAbsolute(value)
}

function normalizeSourcePath(value: string): string {
  if (!value.startsWith('file://')) return value
  try {
    return decodeURIComponent(value.replace(/^file:\/\//u, ''))
  } catch {
    return value.replace(/^file:\/\//u, '')
  }
}

function safeAttachmentName(label: string, sourcePath: string): string {
  const candidate = basename(label || sourcePath).replace(/[<>:"/\\|?*\x00-\x1F]/g, '_').trim()
  return candidate || `attachment-${Date.now()}`
}

async function stageAttachment(sourcePath: string, label: string, workspace: string): Promise<OpenClawAttachment> {
  const normalizedSource = normalizeSourcePath(sourcePath)
  if (!isLikelyLocalPath(normalizedSource)) {
    return { label, path: sourcePath, fsPath: sourcePath }
  }
  const sourceStats = await stat(normalizedSource)
  if (!sourceStats.isFile()) {
    throw new Error(`Attachment is not a file: ${normalizedSource}`)
  }
  if (!workspace) {
    throw new Error('OpenClaw workspace is not configured')
  }
  const uploadDir = join(workspace, 'codexclaw-uploads', new Date().toISOString().replace(/[:.]/g, '-'))
  await mkdir(uploadDir, { recursive: true })
  const fileName = safeAttachmentName(label, normalizedSource)
  const stagedPath = join(uploadDir, fileName)
  await copyFile(normalizedSource, stagedPath)
  return { label: fileName, path: stagedPath, fsPath: stagedPath }
}

async function readJsonFile(path: string): Promise<Record<string, unknown> | null> {
  try {
    const raw = await readFile(path, 'utf8')
    const parsed = JSON.parse(raw.replace(/^\uFEFF/u, '')) as unknown
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed)
      ? parsed as Record<string, unknown>
      : null
  } catch {
    return null
  }
}

function parseSessionStore(value: Record<string, unknown>, agentId: string): OpenClawSessionRow[] {
  const rows: OpenClawSessionRow[] = []
  for (const [key, entry] of Object.entries(value)) {
    if (!entry || typeof entry !== 'object' || Array.isArray(entry)) continue
    rows.push({
      ...(entry as OpenClawSessionRow),
      key,
      agentId,
    })
  }
  return rows
}

function readConfiguredAgentIds(config: Record<string, unknown> | null): Set<string> {
  const configuredAgentIds = new Set<string>()
  const agents = config?.agents && typeof config.agents === 'object' && !Array.isArray(config.agents)
    ? config.agents as Record<string, unknown>
    : {}
  const agentList = Array.isArray(agents.list) ? agents.list : []
  for (const entry of agentList) {
    if (!entry || typeof entry !== 'object' || Array.isArray(entry)) continue
    const id = readString((entry as Record<string, unknown>).id)
    if (id) configuredAgentIds.add(id)
  }

  const fallbackAgents = ['ari-super', 'ari-ops', 'family-finance', 'pat-super', 'pat-ops']
  for (const agentId of fallbackAgents) {
    if (existsSync(join(getOpenClawHome(), 'agents', agentId, 'sessions', 'sessions.json'))) {
      configuredAgentIds.add(agentId)
    }
  }
  return configuredAgentIds
}

async function listConfiguredAgents(): Promise<OpenClawAgentRow[]> {
  const openClawHome = getOpenClawHome()
  const config = openClawHome ? await readJsonFile(join(openClawHome, 'openclaw.json')) : null
  const agents = config?.agents && typeof config.agents === 'object' && !Array.isArray(config.agents)
    ? config.agents as Record<string, unknown>
    : {}
  const agentList = Array.isArray(agents.list) ? agents.list : []
  const rows = agentList.flatMap((entry): OpenClawAgentRow[] => {
    if (!entry || typeof entry !== 'object' || Array.isArray(entry)) return []
    const row = entry as Record<string, unknown>
    const id = readString(row.id)
    if (!id) return []
    return [{
      id,
      name: readString(row.name) || id,
      isDefault: row.default === true,
      primaryModel: readString((row.model as Record<string, unknown> | undefined)?.primary),
    }]
  })
  return rows.sort((first, second) => {
    if (first.isDefault !== second.isDefault) return first.isDefault ? -1 : 1
    return first.name.localeCompare(second.name)
  })
}

async function resolveConfiguredAgentId(rawAgentId: unknown): Promise<string> {
  const agents = await listConfiguredAgents()
  const requested = readString(rawAgentId)
  if (requested && agents.some((agent) => agent.id === requested)) return requested
  const envDefault = readString(process.env.OPENCLAW_DEFAULT_AGENT)
  if (envDefault && agents.some((agent) => agent.id === envDefault)) return envDefault
  return agents.find((agent) => agent.isDefault)?.id || agents[0]?.id || envDefault || 'ari-super'
}

async function listLocalSessions(limit = 100): Promise<OpenClawSessionRow[]> {
  const openClawHome = getOpenClawHome()
  if (!openClawHome) return []

  const config = await readJsonFile(join(openClawHome, 'openclaw.json'))
  const configuredAgentIds = readConfiguredAgentIds(config)

  const sessions: OpenClawSessionRow[] = []
  for (const agentId of configuredAgentIds) {
    const storePath = join(openClawHome, 'agents', agentId, 'sessions', 'sessions.json')
    const store = await readJsonFile(storePath)
    if (!store) continue
    sessions.push(...parseSessionStore(store, agentId))
  }

  sessions.sort((first, second) => (readNumber(second.updatedAt) ?? 0) - (readNumber(first.updatedAt) ?? 0))
  return sessions.slice(0, Math.max(1, limit))
}

async function listSessions(limit = 100): Promise<OpenClawSessionRow[]> {
  const localSessions = await listLocalSessions(limit)
  if (localSessions.length > 0 || process.env.OPENCLAW_DISABLE_GATEWAY_SESSION_LIST !== '0') {
    return localSessions
  }

  const payload = await gatewayCall('sessions.list', { limit }, 45_000)
  const sessions = Array.isArray(payload.sessions) ? payload.sessions : []
  return Promise.all(
    sessions
      .filter((row): row is OpenClawSessionRow => Boolean(row && typeof row === 'object'))
      .map((row) => enrichSession(row)),
  )
}

async function findLocalSession(sessionKey: string): Promise<OpenClawSessionRow | null> {
  const agentId = agentIdFromSessionKey(sessionKey)
  const openClawHome = getOpenClawHome()
  if (!agentId || !openClawHome) return null
  const storePath = join(openClawHome, 'agents', agentId, 'sessions', 'sessions.json')
  const store = await readJsonFile(storePath)
  const entry = store?.[sessionKey]
  if (!entry || typeof entry !== 'object' || Array.isArray(entry)) return null
  return {
    ...(entry as OpenClawSessionRow),
    key: sessionKey,
    agentId,
  }
}

async function findSession(sessionKey: string): Promise<OpenClawSessionRow | null> {
  const localSession = await findLocalSession(sessionKey)
  if (localSession) return localSession
  const sessions = await listSessions(250)
  return sessions.find((session) => readString(session.key) === sessionKey) ?? null
}

async function renameLocalSession(sessionKey: string, displayName: string): Promise<OpenClawSessionRow> {
  const agentId = agentIdFromSessionKey(sessionKey)
  if (!agentId) throw new Error('OpenClaw session does not include an agent id')
  const openClawHome = getOpenClawHome()
  if (!openClawHome) throw new Error('OpenClaw home is not configured')
  const storePath = join(openClawHome, 'agents', agentId, 'sessions', 'sessions.json')
  const store = await readJsonFile(storePath)
  const existing = store?.[sessionKey]
  if (!existing || typeof existing !== 'object' || Array.isArray(existing)) {
    throw new Error('OpenClaw session not found')
  }
  const nextEntry = {
    ...(existing as OpenClawSessionRow),
    displayName,
  }
  const nextStore = {
    ...store,
    [sessionKey]: nextEntry,
  }
  await writeFile(storePath, `${JSON.stringify(nextStore, null, 2)}\n`, 'utf8')
  return {
    ...nextEntry,
    key: sessionKey,
    agentId,
  }
}

async function enrichSession(session: OpenClawSessionRow): Promise<OpenClawSessionRow> {
  if (readString(session.sessionFile)) return session
  const key = readString(session.key)
  const agentId = agentIdFromSessionKey(key)
  if (!key || !agentId) return session
  const openClawHome = getOpenClawHome()
  if (!openClawHome) return session
  const storePath = join(openClawHome, 'agents', agentId, 'sessions', 'sessions.json')
  try {
    const raw = await readFile(storePath, 'utf8')
    const parsed = JSON.parse(raw) as Record<string, OpenClawSessionRow>
    return {
      ...session,
      ...(parsed[key] ?? {}),
      key,
    }
  } catch {
    return session
  }
}

function contentToText(content: unknown): string {
  if (typeof content === 'string') return content
  if (!Array.isArray(content)) return ''
  const parts: string[] = []
  for (const item of content) {
    if (!item || typeof item !== 'object') continue
    const row = item as Record<string, unknown>
    const type = readString(row.type)
    if (type === 'text') {
      const text = readString(row.text)
      if (text) parts.push(text)
    }
  }
  return parts.join('\n\n')
}

function stripOpenClawUserPrefix(text: string): string {
  return text.replace(/^\[[^\]]+\]\s*/, '').trim()
}

function normalizeHistoryEntry(row: Record<string, unknown>, index: number): Record<string, unknown> | null {
  const id = readString(row.id) || `openclaw-message-${index}`
  const message = row.message && typeof row.message === 'object' ? row.message as Record<string, unknown> : null
  const timestamp = readString(row.timestamp) || new Date().toISOString()
  const type = readString(row.type)
  if (message) {
    const role = readString(message.role)
    if (role === 'user' || role === 'assistant') {
      const text = contentToText(message.content)
      if (
        role === 'user'
        && (
          text.includes('<<<BEGIN_OPENCLAW_INTERNAL_CONTEXT>>>')
          || text.startsWith('[Inter-session message]')
        )
      ) {
        return null
      }
      const toolCalls = Array.isArray(message.content)
        ? message.content
          .filter((item) => item && typeof item === 'object' && readString((item as Record<string, unknown>).type) === 'toolCall')
          .map((item) => item as Record<string, unknown>)
        : []
      if (text) {
        return {
          id,
          role,
          text: role === 'user' ? stripOpenClawUserPrefix(text) : text,
          timestamp,
          messageType: role === 'assistant' ? 'agentMessage' : undefined,
        }
      }
      if (toolCalls.length > 0) {
        const details = toolCalls.map((call) => {
          const name = readString(call.name) || 'tool'
          const args = call.arguments && typeof call.arguments === 'object'
            ? JSON.stringify(call.arguments, null, 2)
            : ''
          return args ? `${name}\n${args}` : name
        }).join('\n\n')
        return {
          id,
          role: 'system',
          text: details,
          timestamp,
          messageType: 'commandExecution',
          commandExecution: {
            command: toolCalls.map((call) => readString(call.name) || 'tool').join(', '),
            cwd: null,
            status: 'completed',
            aggregatedOutput: details,
            exitCode: null,
          },
        }
      }
    }
    if (role === 'toolResult') {
      const toolName = readString(message.toolName) || 'tool'
      const text = contentToText(message.content)
      const status = readString((message.details as Record<string, unknown> | undefined)?.status)
      return {
        id,
        role: 'system',
        text: text || `${toolName} ${status || 'completed'}`,
        timestamp,
        messageType: 'commandExecution',
        commandExecution: {
          command: toolName,
          cwd: null,
          status: status === 'running' ? 'inProgress' : status === 'failed' ? 'failed' : 'completed',
          aggregatedOutput: text,
          exitCode: null,
        },
      }
    }
  }
  if (type === 'custom_message') {
    if (row.display === false) return null
    const content = readString(row.content)
    if (!content) return null
    return { id, role: 'system', text: content, timestamp, messageType: 'system' }
  }
  return null
}

async function readSessionMessages(sessionFile: string): Promise<Record<string, unknown>[]> {
  if (!sessionFile || !existsSync(sessionFile)) return []
  const rows: Record<string, unknown>[] = []
  const stream = createReadStream(sessionFile, { encoding: 'utf8' })
  const rl = createInterface({ input: stream, crlfDelay: Infinity })
  let index = 0
  for await (const line of rl) {
    const trimmed = line.trim()
    if (!trimmed) continue
    try {
      const parsed = JSON.parse(trimmed) as Record<string, unknown>
      const normalized = normalizeHistoryEntry(parsed, index)
      if (normalized) rows.push(normalized)
      index += 1
    } catch {
      index += 1
    }
  }
  return rows
}

function countAssistantMessages(messages: Record<string, unknown>[]): number {
  return messages.filter((message) => readString(message.role) === 'assistant').length
}

function hasAssistantMessageSince(messages: Record<string, unknown>[], sinceMs: number): boolean {
  return messages.some((message) => {
    if (readString(message.role) !== 'assistant') return false
    const timestampMs = Date.parse(readString(message.timestamp))
    return Number.isFinite(timestampMs) && timestampMs >= sinceMs
  })
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function readPositiveIntegerEnv(name: string, fallback: number): number {
  const parsed = Number.parseInt(process.env[name] ?? '', 10)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback
}

async function waitForAssistantReply(
  sessionKey: string,
  baselineAssistantCount: number,
  timeoutMs: number,
  knownSessionFile = '',
  turnStartedAtMs = Date.now(),
): Promise<void> {
  const startedAtMs = Date.now()
  while (Date.now() - startedAtMs < timeoutMs) {
    const currentSessionFile = readString((await findLocalSession(sessionKey))?.sessionFile)
    const sessionFiles = Array.from(new Set([knownSessionFile, currentSessionFile].filter(Boolean)))
    for (const sessionFile of sessionFiles) {
      const messages = await readSessionMessages(sessionFile)
      if (sessionFile === knownSessionFile && countAssistantMessages(messages) > baselineAssistantCount) return
      if (hasAssistantMessageSince(messages, turnStartedAtMs - 5_000)) return
    }
    await sleep(250)
  }
  throw new Error(`OpenClaw turn timed out after ${timeoutMs}ms`)
}

async function waitForCompletedSessionSnapshot(
  sessionKey: string,
  fallbackSession: OpenClawSessionRow,
  timeoutMs: number,
): Promise<{ session: OpenClawSessionRow; messages: Record<string, unknown>[] }> {
  const startedAtMs = Date.now()
  let latestSession = await findSession(sessionKey) ?? fallbackSession
  let latestMessages = await readSessionMessages(readString(latestSession.sessionFile))
  while (Date.now() - startedAtMs < timeoutMs) {
    latestSession = await findSession(sessionKey) ?? latestSession
    latestMessages = await readSessionMessages(readString(latestSession.sessionFile))
    if (latestSession.hasActiveRun !== true && readString(latestSession.status) !== 'running') {
      return { session: latestSession, messages: latestMessages }
    }
    await sleep(250)
  }
  return { session: latestSession, messages: latestMessages }
}

function toThread(session: OpenClawSessionRow): Record<string, unknown> {
  const key = readString(session.key)
  const title = readString(session.displayName) || readString(session.label) || key.split(':').slice(-1)[0] || 'OpenClaw session'
  const updatedAtIso = toIso(session.updatedAt)
  return {
    id: encodeThreadId(key),
    title,
    projectName: 'OpenClaw',
    cwd: 'OpenClaw',
    hasWorktree: false,
    createdAtIso: toIso(session.startedAt ?? session.sessionStartedAt ?? session.updatedAt),
    updatedAtIso,
    preview: readString(session.model) || readString(session.status) || key,
    unread: false,
    inProgress: session.hasActiveRun === true || readString(session.status) === 'running',
  }
}

function toCompletedThread(session: OpenClawSessionRow): Record<string, unknown> {
  return {
    ...toThread(session),
    inProgress: false,
  }
}

function appendRunOptions(args: string[], options: { model?: string; thinking?: string; fastMode?: boolean } = {}): string[] {
  const model = normalizeOpenClawModelOverride(readString(options.model))
  const thinking = readString(options.thinking)
  if (model && isPermittedOpenClawModelOverride(model)) args.push('--model', model)
  if (thinking) args.push('--thinking', thinking)
  return args
}

function buildSendArgs(session: OpenClawSessionRow, text: string, options: { model?: string; thinking?: string; fastMode?: boolean } = {}): string[] {
  const sessionId = readString(session.sessionId)
  const sessionKey = readString(session.key)
  const agentId = agentIdFromSessionKey(sessionKey)
  if (!sessionId) throw new Error('OpenClaw session is missing sessionId')
  if (!agentId) throw new Error('OpenClaw session is missing agent id')
  const args = ['agent', '--json', '--session-id', sessionId, '--message', text]
  args.splice(1, 0, '--agent', agentId)
  return appendRunOptions(args, options)
}

function normalizeCodexExecModel(model: string): string {
  const normalized = normalizeOpenClawModelOverride(model)
  if (normalized.startsWith('openai-codex/')) return normalized.slice('openai-codex/'.length)
  if (normalized.startsWith('codex/')) return normalized.slice('codex/'.length)
  if (normalized.startsWith('openai/')) return normalized.slice('openai/'.length)
  return normalized || 'gpt-5.4-mini'
}

function extractAttachmentPathFromPromptLine(line: string): string {
  const localPathMatch = /^Local path:\s*(.+)$/iu.exec(line.trim())
  if (localPathMatch) return localPathMatch[1]?.trim() ?? ''

  const headingMatch = /^##\s+.+?:\s*(.+)$/u.exec(line.trim())
  return headingMatch?.[1]?.trim() ?? ''
}

function extractImageAttachmentPathsFromPrompt(text: string): string[] {
  const imageExtensions = new Set(['.avif', '.bmp', '.gif', '.jpg', '.jpeg', '.png', '.webp'])
  const paths = new Set<string>()
  for (const line of text.split(/\r?\n/u)) {
    const candidate = normalizeSourcePath(extractAttachmentPathFromPromptLine(line))
    if (!candidate || !isLikelyLocalPath(candidate)) continue
    if (!imageExtensions.has(extname(candidate).toLowerCase())) continue
    if (!existsSync(candidate)) continue
    paths.add(candidate)
  }
  return [...paths]
}

function encodeOpenClawActivity(label: string, details: string[] = []): string {
  return `__codexclaw_activity__:${JSON.stringify({ label, details })}`
}

function summarizeCodexExecItem(item: Record<string, unknown>): { label: string; details: string[] } | null {
  const type = readString(item.type)
  const name = readString(item.name) || readString(item.command) || readString(item.tool_name)
  const text = readString(item.text) || readString(item.summary) || readString(item.result)

  if (type.includes('tool') || type.includes('function') || name) {
    return {
      label: type.includes('output') || type.includes('result') ? 'OpenClaw tool returned' : 'Running OpenClaw tool',
      details: [name || text || type || 'tool'].filter(Boolean).slice(0, 2),
    }
  }

  if (type.includes('reasoning')) {
    return {
      label: 'OpenClaw is reasoning',
      details: [text || 'Thinking through the request'],
    }
  }

  if (type.includes('agent_message') || type.includes('message')) {
    return {
      label: 'OpenClaw is drafting',
      details: text ? [text.slice(0, 240)] : ['Preparing response'],
    }
  }

  return type ? { label: 'OpenClaw is working', details: [type] } : null
}

function summarizeCodexExecJsonLine(line: string): { label: string; details: string[] } | null {
  try {
    const row = JSON.parse(line) as Record<string, unknown>
    const type = readString(row.type)
    if (type === 'thread.started') {
      return { label: 'OpenClaw fast mode started', details: ['Created Codex execution thread'] }
    }
    if (type === 'turn.started') {
      return { label: 'OpenClaw is thinking', details: ['Model run started'] }
    }
    if (type === 'turn.completed') {
      const usage = row.usage && typeof row.usage === 'object' && !Array.isArray(row.usage)
        ? row.usage as Record<string, unknown>
        : {}
      const inputTokens = readNumber(usage.input_tokens)
      const outputTokens = readNumber(usage.output_tokens)
      const details = inputTokens !== null || outputTokens !== null
        ? [`${inputTokens ?? 0} input tokens, ${outputTokens ?? 0} output tokens`]
        : ['Final response ready']
      return { label: 'OpenClaw finished thinking', details }
    }
    if (type === 'item.started' || type === 'item.completed') {
      const item = row.item && typeof row.item === 'object' && !Array.isArray(row.item)
        ? row.item as Record<string, unknown>
        : {}
      return summarizeCodexExecItem(item)
    }
    return type ? { label: 'OpenClaw is working', details: [type] } : null
  } catch {
    return null
  }
}

function runCodexExecFastReply(
  session: OpenClawSessionRow,
  text: string,
  onOutput: (chunk: string, stream: 'stdout' | 'stderr') => void,
  options: { model?: string; thinking?: string; fastMode?: boolean } = {},
): Promise<string> {
  return new Promise((resolve, reject) => {
    const outputPath = join(tmpdir(), `codexclaw-fast-${randomUUID()}.txt`)
    const model = normalizeCodexExecModel(readString(options.model))
    const runtimeConfig = resolveAppServerRuntimeConfig()
    const timeoutMs = readPositiveIntegerEnv('OPENCLAW_CODEX_FAST_TIMEOUT_MS', DEFAULT_CODEX_FAST_TIMEOUT_MS)
    const workspaceDir = readString(session.workspaceDir) || readString(session.cwd) || join(getOpenClawHome(), 'workspace-family-finance')
    const codexCmd = process.platform === 'win32'
      ? join(process.env.APPDATA || '', 'npm', 'codex.cmd')
      : 'codex'
    const imageArgs = extractImageAttachmentPathsFromPrompt(text).flatMap((path) => ['--image', path])
    const args = [
      'exec',
      '-c',
      `approval_policy="${runtimeConfig.approvalPolicy}"`,
      '-c',
      `sandbox_mode="${runtimeConfig.sandboxMode}"`,
      '--ephemeral',
      '--skip-git-repo-check',
      '--cd',
      workspaceDir,
      '--model',
      model,
      '--json',
      ...imageArgs,
      '--output-last-message',
      outputPath,
      '-',
    ]
    const startedAtMs = Date.now()
    const child = process.platform === 'win32'
      ? spawn('cmd.exe', ['/d', '/s', '/c', codexCmd, ...args], {
        windowsHide: true,
        stdio: ['pipe', 'pipe', 'pipe'],
      })
      : spawn(codexCmd, args, {
      windowsHide: true,
      stdio: ['pipe', 'pipe', 'pipe'],
    })
    let stderr = ''
    let stdoutBuffer = ''
    child.stdin?.end(text)
    const timer = setTimeout(() => {
      child.kill()
      reject(new Error(`Codex fast mode timed out after ${Math.round(timeoutMs / 1000)}s`))
    }, timeoutMs)
    child.stdout.on('data', (chunk) => {
      stdoutBuffer += String(chunk)
      let newlineIndex = stdoutBuffer.indexOf('\n')
      while (newlineIndex >= 0) {
        const line = stdoutBuffer.slice(0, newlineIndex).trim()
        stdoutBuffer = stdoutBuffer.slice(newlineIndex + 1)
        const activity = line ? summarizeCodexExecJsonLine(line) : null
        if (activity) {
          onOutput(encodeOpenClawActivity(activity.label, activity.details), 'stdout')
        }
        newlineIndex = stdoutBuffer.indexOf('\n')
      }
    })
    child.stderr.on('data', (chunk) => {
      stderr += String(chunk)
      const detail = String(chunk).trim()
      if (detail) onOutput(detail, 'stderr')
    })
    child.on('error', (error) => {
      clearTimeout(timer)
      reject(error)
    })
    child.on('close', async (code) => {
      clearTimeout(timer)
      try {
        const finalLine = stdoutBuffer.trim()
        const finalActivity = finalLine ? summarizeCodexExecJsonLine(finalLine) : null
        if (finalActivity) {
          onOutput(encodeOpenClawActivity(finalActivity.label, finalActivity.details), 'stdout')
        }
        const reply = existsSync(outputPath) ? (await readFile(outputPath, 'utf8')).trim() : ''
        await unlink(outputPath).catch(() => {})
        console.info('[openclaw-fast-codex]', JSON.stringify({
          code,
          durationMs: Date.now() - startedAtMs,
          model,
          sandboxMode: runtimeConfig.sandboxMode,
          approvalPolicy: runtimeConfig.approvalPolicy,
        }))
        if (code !== 0) throw new Error((stderr || `codex exited with code ${code}`).trim())
        resolve(reply || 'OK')
      } catch (error) {
        reject(error)
      }
    })
  })
}

async function persistFastReplyToSession(session: OpenClawSessionRow, text: string, reply: string): Promise<void> {
  const sessionKey = readString(session.key)
  const agentId = agentIdFromSessionKey(sessionKey)
  const openClawHome = getOpenClawHome()
  if (!sessionKey || !agentId || !openClawHome) return
  const storePath = join(openClawHome, 'agents', agentId, 'sessions', 'sessions.json')
  const store = await readJsonFile(storePath) ?? {}
  const now = Date.now()
  const entry = store[sessionKey] && typeof store[sessionKey] === 'object' && !Array.isArray(store[sessionKey])
    ? store[sessionKey] as OpenClawSessionRow
    : session
  const sessionId = readString(entry.sessionId) || readString(session.sessionId) || randomUUID()
  const sessionFile = readString(entry.sessionFile) || readString(session.sessionFile) || join(openClawHome, 'agents', agentId, 'sessions', `${sessionId}.jsonl`)
  await mkdir(dirname(sessionFile), { recursive: true })
  const userId = randomUUID()
  const assistantId = randomUUID()
  const timestamp = new Date(now).toISOString()
  const rows = [
    {
      type: 'message',
      id: userId,
      parentId: null,
      timestamp,
      message: {
        role: 'user',
        content: text,
        timestamp: now,
      },
    },
    {
      type: 'message',
      id: assistantId,
      parentId: userId,
      timestamp: new Date().toISOString(),
      message: {
        role: 'assistant',
        content: [{ type: 'text', text: reply }],
        provider: 'codex',
        model: 'fast-codex-exec',
        stopReason: 'stop',
        timestamp: now,
      },
    },
  ]
  await appendFile(sessionFile, `${rows.map((row) => JSON.stringify(row)).join('\n')}\n`, 'utf8')
  const nextStore = {
    ...store,
    [sessionKey]: {
      ...entry,
      sessionId,
      sessionFile,
      updatedAt: now,
      status: 'done',
      fastMode: true,
    },
  }
  await writeFile(storePath, `${JSON.stringify(nextStore, null, 2)}\n`, 'utf8')
}

async function sendToSessionStreaming(
  session: OpenClawSessionRow,
  text: string,
  onOutput: (chunk: string, stream: 'stdout' | 'stderr') => void,
  options: { model?: string; thinking?: string; fastMode?: boolean } = {},
): Promise<void> {
  const sessionId = readString(session.sessionId)
  const sessionKey = readString(session.key)
  const agentId = agentIdFromSessionKey(sessionKey)
  if (!sessionId) throw new Error('OpenClaw session is missing sessionId')
  if (!sessionKey) throw new Error('OpenClaw session is missing session key')
  if (!agentId) throw new Error('OpenClaw session is missing agent id')
  const beforeMessages = await readSessionMessages(readString(session.sessionFile))
  const baselineAssistantCount = countAssistantMessages(beforeMessages)
  const model = normalizeOpenClawModelOverride(readString(options.model))
  const thinking = readString(options.thinking)
  const fastMode = options.fastMode === true
  const runId = randomUUID()
  const turnStartedAtMs = Date.now()
  if (fastMode) {
    const reply = await runCodexExecFastReply(session, text, onOutput, options)
    await persistFastReplyToSession(session, text, reply)
    onOutput('Codex fast mode reply received', 'stdout')
    return
  }
  try {
    const gatewayParams = {
      agentId,
      sessionId,
      sessionKey,
      idempotencyKey: runId,
      message: text,
      deliver: false,
      channel: 'webchat',
      ...(fastMode ? {
        modelRun: true,
        promptMode: 'none',
        bootstrapContextMode: 'lightweight',
        cleanupBundleMcpOnRunEnd: true,
      } : {}),
      ...(model && isPermittedOpenClawModelOverride(model) ? parseModelOverride(model) : {}),
      ...(thinking ? { thinking } : {}),
    }
    await transientGatewayCall('agent', gatewayParams, { expectFinal: false, timeoutMs: 5_000 })
    onOutput('OpenClaw accepted the turn', 'stdout')
    await waitForAssistantReply(sessionKey, baselineAssistantCount, 900_000, readString(session.sessionFile), turnStartedAtMs)
    onOutput('OpenClaw reply received', 'stdout')
    return
  } catch (error) {
    onOutput(`Gateway path failed, falling back to OpenClaw CLI: ${error instanceof Error ? error.message : 'unknown error'}`, 'stderr')
  }
  await runOpenClawStreaming(buildSendArgs(session, text, options), onOutput, 900_000)
}

async function startSession(
  text: string,
  options: { model?: string; thinking?: string; fastMode?: boolean } = {},
  rawAgentId: unknown = '',
): Promise<OpenClawSessionRow> {
  const sessionId = randomUUID()
  const agentId = await resolveConfiguredAgentId(rawAgentId)
  const sessionKey = `agent:${agentId}:explicit:${sessionId}`
  const model = normalizeOpenClawModelOverride(readString(options.model))
  const thinking = readString(options.thinking)
  const createResult = await directGatewayCall('sessions.create', {
    agentId,
    key: sessionKey,
    ...(model && isPermittedOpenClawModelOverride(model) ? { model } : {}),
  }, { expectFinal: false, timeoutMs: 30_000 })
  const resolvedSessionKey = readString(createResult.key) || sessionKey
  let patchedEntry: Record<string, unknown> | null = null
  if (options.fastMode === true || thinking) {
    const patchResult = await directGatewayCall('sessions.patch', {
      key: resolvedSessionKey,
      ...(options.fastMode === true ? { fastMode: true } : {}),
      ...(thinking ? { thinkingLevel: thinking } : {}),
    }, { expectFinal: false, timeoutMs: 30_000 })
    patchedEntry = patchResult.entry && typeof patchResult.entry === 'object'
      ? patchResult.entry as Record<string, unknown>
      : null
  }
  const createdEntry = createResult.entry && typeof createResult.entry === 'object'
    ? createResult.entry as Record<string, unknown>
    : null
  const session: OpenClawSessionRow = {
    ...(createdEntry ?? {}),
    ...(patchedEntry ?? {}),
    key: resolvedSessionKey,
    sessionId: readString(patchedEntry?.sessionId) || readString(createdEntry?.sessionId) || sessionId,
    displayName: readString(patchedEntry?.displayName) || readString(createdEntry?.displayName) || text.slice(0, 80),
    updatedAt: readNumber(patchedEntry?.updatedAt) ?? readNumber(createdEntry?.updatedAt) ?? Date.now(),
    ...(options.fastMode === true ? { fastMode: true } : {}),
    ...(thinking ? { thinkingLevel: thinking } : {}),
  }
  await sendToSessionStreaming(session, text, () => {}, options)
  return await findSession(resolvedSessionKey) ?? session
}

function asyncHandler(handler: RequestHandler): RequestHandler {
  return (req, res, next) => {
    Promise.resolve(handler(req, res, next)).catch(next)
  }
}

function writeSse(res: express.Response, event: string, data: unknown): void {
  res.write(`event: ${event}\n`)
  res.write(`data: ${JSON.stringify(data)}\n\n`)
}

function readEncodedOpenClawActivity(chunk: string): { label: string; details: string[] } | null {
  const prefix = '__codexclaw_activity__:'
  if (!chunk.startsWith(prefix)) return null
  try {
    const parsed = JSON.parse(chunk.slice(prefix.length)) as Record<string, unknown>
    const label = readString(parsed.label) || 'OpenClaw is working'
    const details = Array.isArray(parsed.details)
      ? parsed.details.map((detail) => readString(detail)).filter(Boolean).slice(0, 5)
      : []
    return { label, details }
  } catch {
    return null
  }
}

function readOpenClawRunOptions(body: unknown): { model?: string; thinking?: string; fastMode?: boolean } {
  const row = body && typeof body === 'object' && !Array.isArray(body)
    ? body as Record<string, unknown>
    : {}
  const model = readString(row.model)
  const thinking = readString(row.thinking)
  const fastMode = row.fastMode !== false
  return {
    ...(model ? { model } : {}),
    ...(thinking ? { thinking } : {}),
    ...(fastMode ? { fastMode } : {}),
  }
}

function normalizeThinking(value: string): string {
  return value === 'none' ? 'off' : value
}

function isPermittedOpenClawModelOverride(model: string): boolean {
  const normalized = normalizeOpenClawModelOverride(model).toLowerCase()
  return normalized.startsWith('openai/')
    || normalized.startsWith('openai-codex/')
    || normalized.startsWith('codex/')
}

function parseModelOverride(model: string): Record<string, string> {
  const normalized = normalizeOpenClawModelOverride(model)
  const slashIndex = normalized.indexOf('/')
  if (slashIndex <= 0 || slashIndex >= normalized.length - 1) return { model: normalized }
  return {
    provider: normalized.slice(0, slashIndex),
    model: normalized.slice(slashIndex + 1),
  }
}

function isFreshMetadataCache(atMs: number): boolean {
  return Date.now() - atMs < OPENCLAW_METADATA_CACHE_TTL_MS
}

async function readConfiguredModels(): Promise<OpenClawModelRow[]> {
  const output = await runOpenClaw(['models', 'list', '--json'], 20_000)
  const parsed = parseJsonObject(output)
  const models = Array.isArray(parsed.models) ? parsed.models : []
  return models.flatMap((item) => {
    const row = item && typeof item === 'object' && !Array.isArray(item)
      ? item as Record<string, unknown>
      : null
    if (!row) return []
    const id = readString(row.key)
    if (!id) return []
    const name = readString(row.name) || id
    const tags = Array.isArray(row.tags) ? row.tags : []
    return [{
      id,
      name,
      isDefault: tags.some((tag) => tag === 'default'),
    }]
  })
}

async function listConfiguredModels(): Promise<OpenClawModelRow[]> {
  if (cachedModels && isFreshMetadataCache(cachedModels.atMs)) return cachedModels.rows
  if (modelsRefreshPromise) return modelsRefreshPromise

  modelsRefreshPromise = readConfiguredModels()
    .then((rows) => {
      cachedModels = { rows, atMs: Date.now() }
      return rows
    })
    .catch((error) => {
      if (cachedModels) return cachedModels.rows
      throw error
    })
    .finally(() => {
      modelsRefreshPromise = null
    })
  return modelsRefreshPromise
}

async function readVisibleCommands(): Promise<OpenClawCommandRow[]> {
  const output = await runOpenClaw(['skills', 'list', '--json'], 20_000)
  const parsed = parseJsonObject(output)
  const skills = Array.isArray(parsed.skills) ? parsed.skills : []
  const commands = skills.flatMap((item) => {
    const row = item && typeof item === 'object' && !Array.isArray(item)
      ? item as Record<string, unknown>
      : null
    if (!row) return []
    if (row.commandVisible !== true || row.userInvocable !== true) return []
    const name = readString(row.name).replace(/^\/+/u, '')
    if (!name) return []
    return [{
      name,
      description: readString(row.description),
      source: readString(row.source),
    }]
  })
  const unique = new Map<string, OpenClawCommandRow>()
  unique.set('commands', {
    name: 'commands',
    description: 'Show available OpenClaw commands',
    source: 'native',
  })
  for (const command of commands) {
    if (!unique.has(command.name)) unique.set(command.name, command)
  }
  return [...unique.values()].sort((a, b) => {
    if (a.name === 'commands') return -1
    if (b.name === 'commands') return 1
    return a.name.localeCompare(b.name)
  })
}

async function listVisibleCommands(): Promise<OpenClawCommandRow[]> {
  if (cachedCommands && isFreshMetadataCache(cachedCommands.atMs)) return cachedCommands.rows
  if (commandsRefreshPromise) return commandsRefreshPromise

  commandsRefreshPromise = readVisibleCommands()
    .then((rows) => {
      cachedCommands = { rows, atMs: Date.now() }
      return rows
    })
    .catch((error) => {
      if (cachedCommands) return cachedCommands.rows
      throw error
    })
    .finally(() => {
      commandsRefreshPromise = null
    })
  return commandsRefreshPromise
}

export function createOpenClawBridgeMiddleware(): RequestHandler {
  const router = express.Router()

  router.get('/health', asyncHandler(async (_req, res) => {
    const sessions = await listLocalSessions(1)
    res.json({
      ok: true,
      mode: 'local-session-store',
      sessionStoreAvailable: sessions.length > 0,
      gatewayRequiredForSend: true,
    })
  }))

  router.post('/stage-attachments', asyncHandler(async (req, res) => {
    const rawThreadId = readString(req.body?.threadId)
    const rawAttachments: unknown[] = Array.isArray(req.body?.attachments) ? req.body.attachments : []
    const attachments = rawAttachments
      .filter((entry): entry is OpenClawAttachment => Boolean(entry && typeof entry === 'object' && !Array.isArray(entry)))
      .map((entry) => ({
        label: readString(entry.label),
        path: readString(entry.path),
        fsPath: readString(entry.fsPath),
      }))
      .filter((entry) => readString(entry.fsPath || entry.path))

    if (attachments.length === 0) {
      res.json({ attachments: [] })
      return
    }

    let agentId = await resolveConfiguredAgentId(req.body?.agentId)
    if (rawThreadId) {
      const sessionKey = decodeThreadId(rawThreadId)
      agentId = agentIdFromSessionKey(sessionKey) || agentId
    }

    const workspace = await getOpenClawWorkspace(agentId)
    const staged: OpenClawAttachment[] = []
    for (const attachment of attachments) {
      const sourcePath = readString(attachment.fsPath) || readString(attachment.path)
      const label = readString(attachment.label) || basename(sourcePath)
      staged.push(await stageAttachment(sourcePath, label, workspace))
    }

    res.json({ attachments: staged, workspace, agentId })
  }))

  router.get('/threads', asyncHandler(async (_req, res) => {
    const sessions = await listSessions(100)
    res.json({
      groups: [
        {
          projectName: 'OpenClaw',
          threads: sessions.map(toThread),
        },
      ],
      nextCursor: null,
    })
  }))

  router.get('/models', asyncHandler(async (_req, res) => {
    res.json({ models: await listConfiguredModels() })
  }))

  router.get('/agents', asyncHandler(async (_req, res) => {
    res.json({ agents: await listConfiguredAgents() })
  }))

  router.get('/commands', asyncHandler(async (_req, res) => {
    res.json({ commands: await listVisibleCommands() })
  }))

  router.get('/threads/:threadId', asyncHandler(async (req, res) => {
    const sessionKey = decodeThreadId(readRouteParam(req.params.threadId))
    const session = await findSession(sessionKey)
    if (!session) {
      res.status(404).json({ error: 'OpenClaw session not found' })
      return
    }
    const messages = await readSessionMessages(readString(session.sessionFile))
    res.json({
      thread: toThread(session),
      messages,
      inProgress: session.hasActiveRun === true || readString(session.status) === 'running',
      activeTurnId: '',
      turnIndexByTurnId: {},
    })
  }))

  router.patch('/threads/:threadId', asyncHandler(async (req, res) => {
    const sessionKey = decodeThreadId(readRouteParam(req.params.threadId))
    const displayName = readString(req.body?.name ?? req.body?.displayName)
    if (!displayName) {
      res.status(400).json({ error: 'name is required' })
      return
    }
    const session = await renameLocalSession(sessionKey, displayName)
    res.json({ thread: toThread(session) })
  }))

  router.post('/threads', asyncHandler(async (req, res) => {
    const text = readString(req.body?.text)
    if (!text) {
      res.status(400).json({ error: 'text is required' })
      return
    }
    const options = readOpenClawRunOptions(req.body)
    if (options.thinking) options.thinking = normalizeThinking(options.thinking)
    const session = await startSession(text, options, req.body?.agentId)
    res.json({ thread: toThread(session), messages: await readSessionMessages(readString(session.sessionFile)) })
  }))

  router.post('/threads/:threadId/turns', asyncHandler(async (req, res) => {
    const text = readString(req.body?.text)
    if (!text) {
      res.status(400).json({ error: 'text is required' })
      return
    }
    const sessionKey = decodeThreadId(readRouteParam(req.params.threadId))
    const session = await findSession(sessionKey)
    if (!session) {
      res.status(404).json({ error: 'OpenClaw session not found' })
      return
    }
    const options = readOpenClawRunOptions(req.body)
    if (options.thinking) options.thinking = normalizeThinking(options.thinking)
    await sendToSessionStreaming(session, text, () => {}, options)
    const { session: refreshed, messages } = await waitForCompletedSessionSnapshot(sessionKey, session, 5_000)
    res.json({ thread: toCompletedThread(refreshed), messages })
  }))

  router.post('/threads/:threadId/turns/stream', asyncHandler(async (req, res) => {
    const text = readString(req.body?.text)
    if (!text) {
      res.status(400).json({ error: 'text is required' })
      return
    }
    const sessionKey = decodeThreadId(readRouteParam(req.params.threadId))
    const session = await findSession(sessionKey)
    if (!session) {
      res.status(404).json({ error: 'OpenClaw session not found' })
      return
    }

    res.statusCode = 200
    res.setHeader('Content-Type', 'text/event-stream; charset=utf-8')
    res.setHeader('Cache-Control', 'no-cache, no-transform')
    res.setHeader('Connection', 'keep-alive')
    res.flushHeaders?.()

    let closed = false
    req.on('close', () => {
      closed = true
    })

    let lastSnapshot = ''
    const turnStartedAtMs = Date.now()
    let firstActivityAtMs = 0
    let firstSnapshotAtMs = 0
    const sendSnapshot = async (event: 'snapshot' | 'done' = 'snapshot') => {
      const { session: refreshed, messages } = event === 'done'
        ? await waitForCompletedSessionSnapshot(sessionKey, session, 5_000)
        : { session: await findSession(sessionKey) ?? session, messages: [] as Record<string, unknown>[] }
      const resolvedMessages = event === 'done'
        ? messages
        : await readSessionMessages(readString(refreshed.sessionFile))
      const payload = {
        thread: event === 'done' ? toCompletedThread(refreshed) : toThread(refreshed),
        messages: resolvedMessages,
      }
      const serialized = JSON.stringify(payload)
      if (event !== 'done' && serialized === lastSnapshot) return
      lastSnapshot = serialized
      if (!firstSnapshotAtMs) firstSnapshotAtMs = Date.now()
      writeSse(res, event, payload)
    }

    writeSse(res, 'activity', {
      label: 'OpenClaw is working',
      details: ['Waiting for tool calls and responses'],
    })
    await sendSnapshot()

    const interval = setInterval(() => {
      if (closed) return
      void sendSnapshot().catch((error) => {
        writeSse(res, 'error', { error: error instanceof Error ? error.message : 'OpenClaw stream failed' })
      })
    }, 750)

    try {
      const options = readOpenClawRunOptions(req.body)
      if (options.thinking) options.thinking = normalizeThinking(options.thinking)
      await sendToSessionStreaming(session, text, (chunk, stream) => {
        if (closed) return
        if (!firstActivityAtMs) firstActivityAtMs = Date.now()
        const encodedActivity = readEncodedOpenClawActivity(chunk)
        if (encodedActivity) {
          writeSse(res, 'activity', encodedActivity)
          return
        }
        const detail = chunk.trim().slice(0, 500)
        if (!detail) return
        writeSse(res, 'activity', {
          label: stream === 'stderr' ? 'OpenClaw status' : 'OpenClaw is working',
          details: [detail],
        })
      }, options)
      clearInterval(interval)
      await sendSnapshot('done')
      console.info('[openclaw-turn]', JSON.stringify({
        sessionKey,
        model: readString(options.model),
        thinking: readString(options.thinking),
        fastMode: options.fastMode === true,
        durationMs: Date.now() - turnStartedAtMs,
        firstActivityMs: firstActivityAtMs ? firstActivityAtMs - turnStartedAtMs : null,
        firstSnapshotMs: firstSnapshotAtMs ? firstSnapshotAtMs - turnStartedAtMs : null,
      }))
    } catch (error) {
      clearInterval(interval)
      writeSse(res, 'error', { error: error instanceof Error ? error.message : 'OpenClaw request failed' })
    } finally {
      res.end()
    }
  }))

  router.get('/session-file', asyncHandler(async (req, res) => {
    const path = readString(req.query.path)
    if (!path) {
      res.status(400).json({ error: 'path is required' })
      return
    }
    res.type('text/plain').send(await readFile(path, 'utf8'))
  }))

  return router
}
