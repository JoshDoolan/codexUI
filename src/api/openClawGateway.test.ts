import { afterEach, describe, expect, it, vi } from 'vitest'
import { startOpenClawThread, startOpenClawThreadTurn } from './openClawGateway'

type FetchRequest = {
  url: string
  body: Record<string, unknown>
}

function mockOpenClawFetch(): FetchRequest[] {
  const requests: FetchRequest[] = []

  vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input)
    const body = typeof init?.body === 'string'
      ? JSON.parse(init.body) as Record<string, unknown>
      : {}
    requests.push({ url, body })

    if (url === '/codex-api/openclaw/stage-attachments') {
      const attachments = Array.isArray(body.attachments)
        ? body.attachments.map((entry) => {
          const row = entry as Record<string, unknown>
          const label = typeof row.label === 'string' ? row.label : 'attachment'
          return {
            label,
            path: `C:\\OpenClaw\\workspace\\codexclaw-uploads\\${label}`,
            fsPath: `C:\\OpenClaw\\workspace\\codexclaw-uploads\\${label}`,
          }
        })
        : []
      return new Response(JSON.stringify({ attachments }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    return new Response(JSON.stringify({
      thread: {
        id: 'openclaw::agent%3Afamily-finance%3Asession-1',
        title: 'OpenClaw session',
        projectName: 'OpenClaw',
        cwd: 'OpenClaw',
      },
      messages: [],
    }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    })
  }))

  return requests
}

describe('OpenClaw attachment payloads', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('keeps an image caption as the OpenClaw request while staging the image path', async () => {
    const requests = mockOpenClawFetch()

    await startOpenClawThread(
      'please inspect this chart',
      ['/codex-local-image?path=C%3A%5CUsers%5CJD%5CAppData%5CLocal%5CTemp%5Ccodex-web-uploads%5Cf-1%5Cchart.png'],
      [],
      [],
      'openai-codex/gpt-5.5',
      'medium',
      'family-finance',
      true,
    )

    expect(requests[0]).toMatchObject({
      url: '/codex-api/openclaw/stage-attachments',
      body: {
        threadId: null,
        agentId: 'family-finance',
      },
    })
    expect(requests[0].body.attachments).toEqual([{
      label: 'chart.png',
      path: 'C:\\Users\\JD\\AppData\\Local\\Temp\\codex-web-uploads\\f-1\\chart.png',
      fsPath: 'C:\\Users\\JD\\AppData\\Local\\Temp\\codex-web-uploads\\f-1\\chart.png',
    }])

    const sentText = requests[1].body.text
    expect(requests[1].url).toBe('/codex-api/openclaw/threads')
    expect(sentText).toContain('# Files mentioned by the user:')
    expect(sentText).toContain('## chart.png: C:\\OpenClaw\\workspace\\codexclaw-uploads\\chart.png')
    expect(sentText).toContain('## My request for OpenClaw:\n\nplease inspect this chart\n')
  })

  it('stages file attachments against existing OpenClaw threads before sending the caption', async () => {
    const requests = mockOpenClawFetch()

    await startOpenClawThreadTurn(
      'openclaw::agent%3Afamily-finance%3Asession-1',
      'summarise this file',
      [],
      [{ label: 'notes.txt', path: 'C:\\Users\\JD\\Desktop\\notes.txt', fsPath: 'C:\\Users\\JD\\Desktop\\notes.txt' }],
      [],
      'openai-codex/gpt-5.5',
      'medium',
      true,
    )

    expect(requests[0]).toMatchObject({
      url: '/codex-api/openclaw/stage-attachments',
      body: {
        threadId: 'openclaw::agent%3Afamily-finance%3Asession-1',
      },
    })

    const sentText = requests[1].body.text
    expect(requests[1].url).toBe('/codex-api/openclaw/threads/openclaw%3A%3Aagent%253Afamily-finance%253Asession-1/turns')
    expect(sentText).toContain('# Files mentioned by the user:')
    expect(sentText).toContain('## notes.txt: C:\\OpenClaw\\workspace\\codexclaw-uploads\\notes.txt')
    expect(sentText).toContain('## My request for OpenClaw:\n\nsummarise this file\n')
  })
})
