import { spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { createReadStream, existsSync } from 'node:fs'
import { copyFile, mkdir, readFile, stat } from 'node:fs/promises'
import { basename, isAbsolute, join } from 'node:path'
import { createInterface } from 'node:readline'
import express, { type RequestHandler } from 'express'

const DEFAULT_GATEWAY_URL = 'ws://127.0.0.1:18789'
const DEFAULT_OPENCLAW_ENTRY = 'C:\\Users\\JD\\AppData\\Roaming\\npm\\node_modules\\openclaw\\openclaw.mjs'
const OPENCLAW_ID_PREFIX = 'openclaw::'

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

const OPENCLAW_METADATA_CACHE_TTL_MS = 10 * 60 * 1000
let cachedModels: { rows: OpenClawModelRow[]; atMs: number } | null = null
let cachedCommands: { rows: OpenClawCommandRow[]; atMs: number } | null = null
let modelsRefreshPromise: Promise<OpenClawModelRow[]> | null = null
let commandsRefreshPromise: Promise<OpenClawCommandRow[]> | null = null

function readString(value: unknown): string {
  return typeof value === 'string' ? value.trim() : ''
}

function readNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null
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
      stdout += text
      onOutput(text, 'stdout')
    })
    child.stderr.on('data', (chunk) => {
      const text = String(chunk)
      stderr += text
      onOutput(text, 'stderr')
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
    const parsed = JSON.parse(raw) as unknown
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

async function findSession(sessionKey: string): Promise<OpenClawSessionRow | null> {
  const sessions = await listSessions(250)
  return sessions.find((session) => readString(session.key) === sessionKey) ?? null
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

async function sendToSession(session: OpenClawSessionRow, text: string, options: { model?: string; thinking?: string } = {}): Promise<void> {
  const sessionId = readString(session.sessionId)
  const sessionKey = readString(session.key)
  const agentId = agentIdFromSessionKey(sessionKey)
  if (!sessionId) throw new Error('OpenClaw session is missing sessionId')
  const args = ['agent', '--json', '--session-id', sessionId, '--message', text]
  if (agentId) args.splice(1, 0, '--agent', agentId)
  await runOpenClaw(appendRunOptions(args, options), 900_000)
}

function appendRunOptions(args: string[], options: { model?: string; thinking?: string } = {}): string[] {
  const model = readString(options.model)
  const thinking = readString(options.thinking)
  if (model && process.env.OPENCLAW_ALLOW_MODEL_OVERRIDE === '1') args.push('--model', model)
  if (thinking) args.push('--thinking', thinking)
  return args
}

function buildSendArgs(session: OpenClawSessionRow, text: string, options: { model?: string; thinking?: string } = {}): string[] {
  const sessionId = readString(session.sessionId)
  const sessionKey = readString(session.key)
  const agentId = agentIdFromSessionKey(sessionKey)
  if (!sessionId) throw new Error('OpenClaw session is missing sessionId')
  const args = ['agent', '--json', '--session-id', sessionId, '--message', text]
  if (agentId) args.splice(1, 0, '--agent', agentId)
  return appendRunOptions(args, options)
}

async function sendToSessionStreaming(
  session: OpenClawSessionRow,
  text: string,
  onOutput: (chunk: string, stream: 'stdout' | 'stderr') => void,
  options: { model?: string; thinking?: string } = {},
): Promise<void> {
  await runOpenClawStreaming(buildSendArgs(session, text, options), onOutput, 900_000)
}

async function startSession(text: string, options: { model?: string; thinking?: string } = {}): Promise<OpenClawSessionRow> {
  const sessionId = randomUUID()
  await runOpenClaw(appendRunOptions([
    'agent',
    '--json',
    '--agent',
    process.env.OPENCLAW_DEFAULT_AGENT || 'ari-super',
    '--session-id',
    sessionId,
    '--message',
    text,
  ], options), 900_000)
  const sessions = await listSessions(250)
  return sessions.find((session) => readString(session.sessionId) === sessionId)
    ?? {
      key: `agent:${process.env.OPENCLAW_DEFAULT_AGENT || 'ari-super'}:${sessionId}`,
      sessionId,
      displayName: text.slice(0, 80),
      updatedAt: Date.now(),
    }
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

function readOpenClawRunOptions(body: unknown): { model?: string; thinking?: string } {
  const row = body && typeof body === 'object' && !Array.isArray(body)
    ? body as Record<string, unknown>
    : {}
  const model = readString(row.model)
  const thinking = readString(row.thinking)
  return {
    ...(model ? { model } : {}),
    ...(thinking ? { thinking } : {}),
  }
}

function normalizeThinking(value: string): string {
  return value === 'none' ? 'off' : value
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

    let agentId = process.env.OPENCLAW_DEFAULT_AGENT || 'ari-super'
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

  router.post('/threads', asyncHandler(async (req, res) => {
    const text = readString(req.body?.text)
    if (!text) {
      res.status(400).json({ error: 'text is required' })
      return
    }
    const options = readOpenClawRunOptions(req.body)
    if (options.thinking) options.thinking = normalizeThinking(options.thinking)
    const session = await startSession(text, options)
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
    await sendToSession(session, text, options)
    const refreshed = await findSession(sessionKey) ?? session
    res.json({ thread: toThread(refreshed), messages: await readSessionMessages(readString(refreshed.sessionFile)) })
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
    const sendSnapshot = async (event: 'snapshot' | 'done' = 'snapshot') => {
      const refreshed = await findSession(sessionKey) ?? session
      const messages = await readSessionMessages(readString(refreshed.sessionFile))
      const payload = { thread: toThread(refreshed), messages }
      const serialized = JSON.stringify(payload)
      if (event !== 'done' && serialized === lastSnapshot) return
      lastSnapshot = serialized
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
        const detail = chunk.trim().slice(0, 500)
        if (!detail) return
        writeSse(res, 'activity', {
          label: stream === 'stderr' ? 'OpenClaw status' : 'OpenClaw is working',
          details: [detail],
        })
      }, options)
      clearInterval(interval)
      await sendSnapshot('done')
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
