import type { UiMessage, UiProjectGroup, UiThread } from '../types/codex'
import type { FileAttachmentParam } from './codexGateway'

export const OPENCLAW_THREAD_ID_PREFIX = 'openclaw::'

type SkillParam = { name: string; path: string }

export type OpenClawCommandInfo = {
  name: string
  description: string
  source?: string
}

export function isOpenClawThreadId(threadId: string): boolean {
  return threadId.startsWith(OPENCLAW_THREAD_ID_PREFIX)
}

async function fetchOpenClawJson<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, init)
  const payload = await response.json().catch(() => ({})) as { error?: string }
  if (!response.ok) {
    throw new Error(payload.error || `OpenClaw request failed with ${response.status}`)
  }
  return payload as T
}

function normalizeThread(value: unknown): UiThread | null {
  if (!value || typeof value !== 'object') return null
  const row = value as Record<string, unknown>
  const id = typeof row.id === 'string' ? row.id : ''
  if (!id) return null
  return {
    id,
    title: typeof row.title === 'string' && row.title.trim() ? row.title : 'OpenClaw session',
    projectName: typeof row.projectName === 'string' && row.projectName.trim() ? row.projectName : 'OpenClaw',
    cwd: typeof row.cwd === 'string' ? row.cwd : 'OpenClaw',
    hasWorktree: row.hasWorktree === true,
    createdAtIso: typeof row.createdAtIso === 'string' ? row.createdAtIso : new Date().toISOString(),
    updatedAtIso: typeof row.updatedAtIso === 'string' ? row.updatedAtIso : new Date().toISOString(),
    preview: typeof row.preview === 'string' ? row.preview : '',
    unread: row.unread === true,
    inProgress: row.inProgress === true,
  }
}

function normalizeMessage(value: unknown): UiMessage | null {
  if (!value || typeof value !== 'object') return null
  const row = value as Record<string, unknown>
  const id = typeof row.id === 'string' ? row.id : ''
  const role = row.role === 'user' || row.role === 'assistant' || row.role === 'system' ? row.role : null
  if (!id || !role) return null
  const message: UiMessage = {
    id,
    role,
    text: typeof row.text === 'string' ? row.text : '',
    messageType: typeof row.messageType === 'string' ? row.messageType : undefined,
    rawPayload: typeof row.rawPayload === 'string' ? row.rawPayload : undefined,
  }
  const commandExecution = row.commandExecution && typeof row.commandExecution === 'object'
    ? row.commandExecution as Record<string, unknown>
    : null
  if (commandExecution) {
    const status = commandExecution.status === 'inProgress'
      || commandExecution.status === 'completed'
      || commandExecution.status === 'failed'
      || commandExecution.status === 'declined'
      || commandExecution.status === 'interrupted'
      ? commandExecution.status
      : 'completed'
    message.commandExecution = {
      command: typeof commandExecution.command === 'string' ? commandExecution.command : 'tool',
      cwd: typeof commandExecution.cwd === 'string' ? commandExecution.cwd : null,
      status,
      aggregatedOutput: typeof commandExecution.aggregatedOutput === 'string' ? commandExecution.aggregatedOutput : message.text,
      exitCode: typeof commandExecution.exitCode === 'number' ? commandExecution.exitCode : null,
    }
  }
  return message
}

function fileNameFromPath(pathValue: string): string {
  const normalized = pathValue.replace(/\\/g, '/')
  const segments = normalized.split('/').filter(Boolean)
  return segments.at(-1) ?? normalized
}

function normalizeSkillMarkdownPath(skillPath: string): string {
  const trimmed = skillPath.trim().replace(/[\\/]+$/u, '')
  if (!trimmed) return ''
  const deduped = trimmed.replace(/([\\/])SKILL\.md[\\/]SKILL\.md$/u, '$1SKILL.md')
  return /[\\/]SKILL\.md$/u.test(deduped) ? deduped : `${deduped}/SKILL.md`
}

function extractLocalImagePathFromUrl(value: string): string | null {
  if (!value) return null
  try {
    const parsed = new URL(value, 'http://localhost')
    if (parsed.pathname !== '/codex-local-image') return null
    const path = parsed.searchParams.get('path')?.trim() ?? ''
    return path.length > 0 ? path : null
  } catch {
    return null
  }
}

async function stageOpenClawAttachments(
  threadId: string | null,
  attachments: FileAttachmentParam[],
): Promise<FileAttachmentParam[]> {
  if (attachments.length === 0) return []
  const payload = await fetchOpenClawJson<{ attachments?: unknown }>('/codex-api/openclaw/stage-attachments', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ threadId, attachments }),
  })
  return Array.isArray(payload.attachments)
    ? payload.attachments
      .map((entry) => {
        const row = entry && typeof entry === 'object' ? entry as Record<string, unknown> : {}
        const path = typeof row.path === 'string' ? row.path : ''
        if (!path) return null
        return {
          label: typeof row.label === 'string' && row.label.trim() ? row.label : fileNameFromPath(path),
          path,
          fsPath: typeof row.fsPath === 'string' && row.fsPath.trim() ? row.fsPath : path,
        }
      })
      .filter((entry): entry is FileAttachmentParam => entry !== null)
    : attachments
}

async function buildOpenClawTextWithAttachments(
  text: string,
  imageUrls: string[] = [],
  fileAttachments: FileAttachmentParam[] = [],
  skills: SkillParam[] = [],
  threadId: string | null = null,
): Promise<string> {
  const attachments: FileAttachmentParam[] = [...fileAttachments]
  for (const imageUrl of imageUrls) {
    const trimmed = imageUrl.trim()
    if (!trimmed) continue
    const localPath = extractLocalImagePathFromUrl(trimmed)
    if (localPath) {
      attachments.push({
        label: fileNameFromPath(localPath),
        path: localPath,
        fsPath: localPath,
      })
      continue
    }
    attachments.push({
      label: 'Image URL',
      path: trimmed,
      fsPath: trimmed,
    })
  }

  const deduped = attachments.filter((entry, index) =>
    attachments.findIndex((candidate) => candidate.fsPath === entry.fsPath) === index)
  const trimmedText = text.trim()
  const staged = await stageOpenClawAttachments(threadId, deduped)

  let prefix = ''
  if (skills.length > 0) {
    prefix += '# Skills selected by the user:\n'
    for (const skill of skills) {
      prefix += `\n## ${skill.name}\nSkill path: ${normalizeSkillMarkdownPath(skill.path)}\n`
    }
    prefix += '\nPlease read and follow the selected skill instructions where relevant.\n\n'
  }
  if (staged.length > 0) {
    prefix += '# Files/images attached by the user:\n'
    for (const attachment of staged) {
      prefix += `\n## ${attachment.label}\nLocal path: ${attachment.path}\n`
    }
    prefix += '\nPlease inspect the local file path(s) above with your file or image tools.\n\n'
  }
  if (!prefix) return trimmedText
  const request = trimmedText || 'Please use the selected skill(s).'
  return `${prefix}## My request for OpenClaw:\n\n${request}\n`
}

export async function getOpenClawThreadGroupsPage(): Promise<{ groups: UiProjectGroup[]; nextCursor: null }> {
  const payload = await fetchOpenClawJson<{ groups?: unknown }>('/codex-api/openclaw/threads')
  const groups = Array.isArray(payload.groups)
    ? payload.groups.map((group) => {
      const row = group && typeof group === 'object' ? group as Record<string, unknown> : {}
      return {
        projectName: typeof row.projectName === 'string' ? row.projectName : 'OpenClaw',
        threads: Array.isArray(row.threads)
          ? row.threads.map(normalizeThread).filter((thread): thread is UiThread => thread !== null)
          : [],
      }
    })
    : []
  return { groups, nextCursor: null }
}

export async function getOpenClawModelIds(): Promise<string[]> {
  const payload = await fetchOpenClawJson<{ models?: unknown }>('/codex-api/openclaw/models')
  if (!Array.isArray(payload.models)) return []
  const rows = payload.models
    .map((item) => item && typeof item === 'object' ? item as Record<string, unknown> : null)
    .filter((item): item is Record<string, unknown> => item !== null)
  const defaultModels = rows
    .filter((item) => item.isDefault === true)
    .map((item) => typeof item.id === 'string' ? item.id.trim() : '')
    .filter(Boolean)
  const otherModels = rows
    .filter((item) => item.isDefault !== true)
    .map((item) => typeof item.id === 'string' ? item.id.trim() : '')
    .filter(Boolean)
  return [...defaultModels, ...otherModels].filter((item, index, values) => values.indexOf(item) === index)
}

export async function getOpenClawCommands(): Promise<OpenClawCommandInfo[]> {
  const payload = await fetchOpenClawJson<{ commands?: unknown }>('/codex-api/openclaw/commands')
  if (!Array.isArray(payload.commands)) return []
  return payload.commands
    .map((item) => item && typeof item === 'object' ? item as Record<string, unknown> : null)
    .filter((item): item is Record<string, unknown> => item !== null)
    .map((item) => ({
      name: typeof item.name === 'string' ? item.name.trim().replace(/^\/+/u, '') : '',
      description: typeof item.description === 'string' ? item.description.trim() : '',
      source: typeof item.source === 'string' ? item.source.trim() : undefined,
    }))
    .filter((item, index, values) =>
      item.name.length > 0 && values.findIndex((candidate) => candidate.name === item.name) === index)
}

export async function getOpenClawThreadDetail(threadId: string): Promise<{
  messages: UiMessage[]
  inProgress: boolean
  activeTurnId: string
  turnIndexByTurnId: Record<string, number>
}> {
  const payload = await fetchOpenClawJson<{
    messages?: unknown
    inProgress?: boolean
    activeTurnId?: string
    turnIndexByTurnId?: Record<string, number>
  }>(`/codex-api/openclaw/threads/${encodeURIComponent(threadId)}`)
  return {
    messages: Array.isArray(payload.messages)
      ? payload.messages.map(normalizeMessage).filter((message): message is UiMessage => message !== null)
      : [],
    inProgress: payload.inProgress === true,
    activeTurnId: typeof payload.activeTurnId === 'string' ? payload.activeTurnId : '',
    turnIndexByTurnId: payload.turnIndexByTurnId ?? {},
  }
}

export async function startOpenClawThread(
  text: string,
  imageUrls: string[] = [],
  fileAttachments: FileAttachmentParam[] = [],
  skills: SkillParam[] = [],
  model = '',
  thinking = '',
): Promise<{ thread: UiThread; messages: UiMessage[] }> {
  const finalText = await buildOpenClawTextWithAttachments(text, imageUrls, fileAttachments, skills)
  const payload = await fetchOpenClawJson<{ thread?: unknown; messages?: unknown }>('/codex-api/openclaw/threads', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ text: finalText, model, thinking }),
  })
  const thread = normalizeThread(payload.thread)
  if (!thread) throw new Error('OpenClaw did not return a thread')
  return {
    thread,
    messages: Array.isArray(payload.messages)
      ? payload.messages.map(normalizeMessage).filter((message): message is UiMessage => message !== null)
      : [],
  }
}

export async function startOpenClawThreadTurn(
  threadId: string,
  text: string,
  imageUrls: string[] = [],
  fileAttachments: FileAttachmentParam[] = [],
  skills: SkillParam[] = [],
  model = '',
  thinking = '',
): Promise<{
  thread: UiThread
  messages: UiMessage[]
}> {
  const finalText = await buildOpenClawTextWithAttachments(text, imageUrls, fileAttachments, skills, threadId)
  const payload = await fetchOpenClawJson<{ thread?: unknown; messages?: unknown }>(
    `/codex-api/openclaw/threads/${encodeURIComponent(threadId)}/turns`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: finalText, model, thinking }),
    },
  )
  const thread = normalizeThread(payload.thread)
  if (!thread) throw new Error('OpenClaw did not return a thread')
  return {
    thread,
    messages: Array.isArray(payload.messages)
      ? payload.messages.map(normalizeMessage).filter((message): message is UiMessage => message !== null)
      : [],
  }
}

function parseSseEvent(rawEvent: string): { event: string; data: unknown } | null {
  const lines = rawEvent.split(/\r?\n/)
  let event = 'message'
  const dataLines: string[] = []
  for (const line of lines) {
    if (line.startsWith('event:')) {
      event = line.slice('event:'.length).trim()
    } else if (line.startsWith('data:')) {
      dataLines.push(line.slice('data:'.length).trimStart())
    }
  }
  if (dataLines.length === 0) return null
  try {
    return { event, data: JSON.parse(dataLines.join('\n')) }
  } catch {
    return null
  }
}

function normalizeTurnPayload(value: unknown): { thread: UiThread; messages: UiMessage[] } | null {
  if (!value || typeof value !== 'object') return null
  const row = value as Record<string, unknown>
  const thread = normalizeThread(row.thread)
  if (!thread) return null
  return {
    thread,
    messages: Array.isArray(row.messages)
      ? row.messages.map(normalizeMessage).filter((message): message is UiMessage => message !== null)
      : [],
  }
}

export async function startOpenClawThreadTurnStream(
  threadId: string,
  text: string,
  imageUrls: string[] = [],
  fileAttachments: FileAttachmentParam[] = [],
  skills: SkillParam[] = [],
  model = '',
  thinking = '',
  handlers: {
    onSnapshot?: (payload: { thread: UiThread; messages: UiMessage[] }) => void
    onActivity?: (activity: { label: string; details: string[] }) => void
  } = {},
): Promise<{ thread: UiThread; messages: UiMessage[] }> {
  const finalText = await buildOpenClawTextWithAttachments(text, imageUrls, fileAttachments, skills, threadId)
  const response = await fetch(`/codex-api/openclaw/threads/${encodeURIComponent(threadId)}/turns/stream`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ text: finalText, model, thinking }),
  })
  if (!response.ok) {
    const payload = await response.json().catch(() => ({})) as { error?: string }
    throw new Error(payload.error || `OpenClaw request failed with ${response.status}`)
  }
  if (!response.body) throw new Error('OpenClaw did not return a stream')

  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  let latest: { thread: UiThread; messages: UiMessage[] } | null = null

  const consumeEvent = (rawEvent: string) => {
    const parsed = parseSseEvent(rawEvent)
    if (!parsed) return
    if (parsed.event === 'snapshot' || parsed.event === 'done') {
      const payload = normalizeTurnPayload(parsed.data)
      if (!payload) return
      latest = payload
      handlers.onSnapshot?.(payload)
      return
    }
    if (parsed.event === 'activity' && parsed.data && typeof parsed.data === 'object') {
      const row = parsed.data as Record<string, unknown>
      const label = typeof row.label === 'string' ? row.label : 'OpenClaw is working'
      const details = Array.isArray(row.details)
        ? row.details.filter((detail): detail is string => typeof detail === 'string')
        : []
      handlers.onActivity?.({ label, details })
      return
    }
    if (parsed.event === 'error' && parsed.data && typeof parsed.data === 'object') {
      const message = typeof (parsed.data as Record<string, unknown>).error === 'string'
        ? String((parsed.data as Record<string, unknown>).error)
        : 'OpenClaw stream failed'
      throw new Error(message)
    }
  }

  while (true) {
    const { value, done } = await reader.read()
    buffer += decoder.decode(value ?? new Uint8Array(), { stream: !done })
    let boundary = buffer.indexOf('\n\n')
    while (boundary >= 0) {
      const rawEvent = buffer.slice(0, boundary)
      buffer = buffer.slice(boundary + 2)
      consumeEvent(rawEvent)
      boundary = buffer.indexOf('\n\n')
    }
    if (done) break
  }
  if (buffer.trim()) consumeEvent(buffer)
  if (!latest) throw new Error('OpenClaw did not return a final thread snapshot')
  return latest
}
