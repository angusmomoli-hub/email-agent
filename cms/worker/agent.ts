/**
 * Person C — AI Agent worker（執行側）
 *
 * 獨立 process。loop：登入 B 個 API → claim task → 按 skill 做 → 寫返 Mongo
 * → complete。同 NestJS API 分開行（enqueue 模式嘅「工人」）。
 *
 * ⚠️ 而家每個 handler 係【可清走嘅 demo 邏輯】(source/_agent_demo 標記)，
 *    真引擎要 port：S1/enrich → gmaps_browser.py + CUA；analyze/draft → MiniMax。
 *    換 handler 內部即可，loop / claim / complete 唔使動。
 *
 * 跑：npm run worker          （一直行）
 *     WORKER_MAX_IDLE=3 npm run worker   （連續 3 次冇 task 就停，測試用）
 */
import { MongoClient, ObjectId, Db } from 'mongodb';
import { randomBytes } from 'crypto';
import { execFileSync } from 'child_process';
import * as nodemailer from 'nodemailer';
import { ImapFlow } from 'imapflow';
import { simpleParser } from 'mailparser';
import {
  BRAND_NAME,
  BRAND_TAGLINE,
  BRAND_SOLUTIONS,
  BRAND_CONTEXT_BLOCK,
  BRAND_SIGNATURE,
  BRAND_TONE_GUIDE,
} from './brand';

/**
 * 叫 Hermes agent 做嘢（佢有 MiniMax + stealth browser + skills）。
 * --yolo = 非互動自動批准工具（開 browser 唔卡）。
 */
function callHermes(prompt: string, timeoutMs = 240_000): string {
  return execFileSync('hermes', ['-z', prompt, '--yolo'], {
    encoding: 'utf8',
    timeout: timeoutMs,
    maxBuffer: 16 * 1024 * 1024,
  });
}
/** 由 Hermes output 抽第一個 JSON object */
function extractJson(s: string): any {
  const m = s.match(/\{[\s\S]*\}/);
  if (!m) throw new Error('Hermes 冇回 JSON: ' + s.slice(0, 200));
  return JSON.parse(m[0]);
}
/** 由 Hermes output 抽 JSON array（支援截斷修復） */
function extractJsonArray(s: string): any[] {
  // 先試完整 match
  const m = s.match(/\[[\s\S]*\]/);
  if (m) {
    try { return JSON.parse(m[0]); } catch { /* 落去修復 */ }
  }
  // 搵開頭 [ 但冇結尾 ]（截斷）→ 修復
  const start = s.indexOf('[');
  if (start === -1) throw new Error('Hermes 冇回 JSON array: ' + s.slice(0, 300));
  let raw = s.slice(start);
  // 移除最後一個不完整嘅 object（搵最後一個 },）
  const lastComplete = raw.lastIndexOf('},');
  if (lastComplete > 0) {
    raw = raw.slice(0, lastComplete + 1) + ']';
  } else {
    const lastObj = raw.lastIndexOf('}');
    if (lastObj > 0) raw = raw.slice(0, lastObj + 1) + ']';
  }
  try {
    return JSON.parse(raw);
  } catch {
    throw new Error('Hermes JSON 截斷修復失敗: ' + s.slice(0, 300));
  }
}
/**
 * 叫 Hermes 並攞 JSON。parse 唔到（佢「講」而唔係回 JSON）就再叫一次強硬版。
 */
function hermesJson(
  prompt: string,
  opts: { array?: boolean; timeout?: number } = {},
): any {
  const pick = (s: string) =>
    opts.array ? extractJsonArray(s) : extractJson(s);
  try {
    return pick(callHermes(prompt, opts.timeout));
  } catch {
    const strict =
      prompt +
      `\n\nIMPORTANT: Your ENTIRE reply must be ONLY the raw JSON` +
      (opts.array ? ' array (start with [ end with ])' : ' object (start with { end with })') +
      `. No explanation, no markdown, no words before or after.`;
    return pick(callHermes(strict, opts.timeout));
  }
}

const API = process.env.API_URL || 'http://localhost:4000/api';
const MONGO =
  process.env.MONGODB_URI ||
  'mongodb://leadteam:leadteam2026@localhost:27017/lead_scraper';
const EMAIL = process.env.AGENT_EMAIL || 'admin@test.com';
const PASS = process.env.AGENT_PASS || '123456';
const AGENT_ID = process.env.AGENT_ID || 'WORKER-1';
const SKILL = process.env.AGENT_SKILL || ''; // 空 = 任何 skill
const POLL_MS = Number(process.env.POLL_MS || 2000);
const MAX_IDLE = Number(process.env.WORKER_MAX_IDLE || 0); // 0 = 永遠

let token = '';
const log = (...a: unknown[]) => console.log(`[agent ${AGENT_ID}]`, ...a);
const nowIso = () => new Date().toISOString();

async function login(): Promise<void> {
  const r = await fetch(`${API}/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email: EMAIL, password: PASS }),
  });
  const j: any = await r.json();
  token = j?.data?.access_token;
  if (!token) throw new Error('登入失敗');
  log('已登入');
}

async function api(path: string, method = 'GET', body?: unknown): Promise<any> {
  const r = await fetch(`${API}${path}`, {
    method,
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token}`,
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (r.status === 401) {
    await login();
    return api(path, method, body);
  }
  return r.json();
}

const claim = () =>
  api('/tasks/claim', 'POST', {
    agent_id: AGENT_ID,
    ...(SKILL ? { skill_id: SKILL } : {}),
  }).then((j) => j?.data ?? null);
const complete = (id: string, result: unknown) =>
  api(`/tasks/${id}/complete`, 'POST', { result });
const failTask = (id: string, error: string) =>
  api(`/tasks/${id}/fail`, 'POST', { error });

// ───────── handlers（demo 引擎，逐個換真）─────────
async function handle(task: any, db: Db): Promise<unknown> {
  const p = task.params || {};
  const skill = task.skill_id;
  if (p.mode === 'reply_check') return doReplyCheck(p, db); // cron 派嘅，冇 lead
  if (skill === 'S1') return doSearch(p, db);
  if (skill === 'S2' && (p.mode === 'enrich' || p.pipeline_stage === 'enrich')) return doEnrich(p, db);
  if (skill === 'S2') return doAnalyze(p, db);
  if (skill === 'S3') return doDraft(p, db);
  if (skill === 'S4') return doSend(p, db);
  return { note: `no handler for ${skill}` };
}

/** 簡單分類回覆（keyword heuristic，參考舊 reply_analyzer；快、唔使 LLM）*/
function classifyReply(subject: string, body: string): string {
  const t = `${subject} ${body}`.toLowerCase();
  if (/(out of office|auto-?reply|automatic reply|放假|自動回覆|不在)/.test(t)) return 'auto_reply';
  if (/(not interested|no thank|unsubscribe|remove me|唔需要|唔感興趣|冇興趣|拒絕)/.test(t)) return 'not_interested';
  if (/(meeting|zoom|call|schedule|available|開會|會議|時間|傾下|約|見面)/.test(t)) return 'meeting';
  if (/(interested|tell me more|sounds good|有興趣|想了解|報價|quote|詳情)/.test(t)) return 'interested';
  return 'question';
}

/**
 * 定時回覆檢查（inbound 閉環）：直接連 IMAP 讀 inbox → 對返已聯絡 lead
 * → 分類 → 寫返 lead 的 _reply_* 欄。
 * ⚠️ 用返 SMTP 同一組 creds（Gmail App Password 同時支援 SMTP 發 + IMAP 讀）。
 *    未配 SMTP_USER/PASS → graceful 回 0，唔 crash。
 */
async function doReplyCheck(_p: any, db: Db) {
  const leads = await db
    .collection('leads')
    .find({ status: 'contacted', email: { $nin: ['', null] }, _replied: { $ne: true } })
    .limit(100)
    .toArray();
  if (!leads.length) return { checked: 0, note: '冇待查回覆嘅 lead' };

  const user = process.env.SMTP_USER;
  const pass = process.env.SMTP_PASS;
  if (!user || !pass) {
    return { checked: leads.length, note: 'IMAP 未配（要 SMTP_USER/SMTP_PASS）' };
  }

  const byEmail = new Map(leads.map((l) => [String(l.email).toLowerCase(), l]));
  const client = new ImapFlow({
    host: process.env.IMAP_HOST || 'imap.gmail.com',
    port: Number(process.env.IMAP_PORT || 993),
    secure: true,
    auth: { user, pass },
    logger: false,
  });

  let scanned = 0;
  let updated = 0;
  try {
    await client.connect();
    const lock = await client.getMailboxLock('INBOX');
    try {
      const since = new Date(Date.now() - 14 * 864e5); // 近 14 日
      for await (const msg of client.fetch({ since }, { source: true })) {
        scanned++;
        const parsed = await simpleParser(msg.source as Buffer);
        const from = (parsed.from?.value?.[0]?.address || '').toLowerCase();
        const lead = byEmail.get(from);
        if (!lead) continue;
        const category = classifyReply(parsed.subject || '', parsed.text || '');
        await db.collection('leads').updateOne(
          { _id: lead._id },
          {
            $set: {
              _replied: true,
              _reply_category: category,
              _reply_summary: (parsed.subject || '').slice(0, 120),
              _reply_at: nowIso(),
            },
          },
        );
        byEmail.delete(from); // 一個 lead 只更新一次
        updated++;
      }
    } finally {
      lock.release();
    }
    await client.logout();
  } catch (e: any) {
    return { checked: leads.length, replies: 0, note: 'IMAP 讀取失敗：' + (e?.message ?? e) };
  }
  return { checked: leads.length, scanned, replies: updated };
}

/** S1 搜尋 → 叫 Hermes 開 stealth browser 搵 Google Maps，回真公司 */
async function doSearch(p: any, db: Db) {
  const n = Math.min(Number(p.target_count) || 3, 10);
  // 用最快嘅工具（web search API 好過駕駛 Maps UI），單次 call 避免 double timeout
  const prompt = `Find up to ${n} real "${p.keyword}" businesses in ${p.location}.
Use the fastest method available (web search tool preferred; browser only if needed).
For each: name, address, phone, website. Use "" for unknown. Be quick.

Reply with ONLY a raw JSON array — start with [ end with ]. No commentary.
[{"name":"","address":"","phone":"","website":""}]`;
  const arr = extractJsonArray(callHermes(prompt, 420_000));
  const ids: string[] = [];
  for (const r of arr.slice(0, n)) {
    if (!r?.name) continue;
    const res = await db.collection('leads').insertOne({
      lead_id: randomBytes(8).toString('hex'),
      company_name: r.name,
      address: r.address,
      phone: r.phone,
      website: r.website,
      source: 'google_maps',
      _via: 'hermes',
      search_query: `${p.keyword} ${p.location}`,
      status: null,
      _status: 'unverified',
      _imported_at: nowIso(),
    });
    ids.push(res.insertedId.toString());
  }
  return { created_leads: ids.length, lead_object_ids: ids };
}

/**
 * 從 URL 抽 domain（e.g. "https://www.example.com/about" → "example.com"）
 */
function extractDomain(url: string): string {
  try {
    const host = new URL(url.startsWith('http') ? url : `https://${url}`).hostname;
    return host.replace(/^www\./, '');
  } catch { return ''; }
}

/** 爬蟲結果 */
interface ScrapeResult {
  emails: string[];
  phones: string[];
  description: string;   // 公司簡介
  services: string[];     // 服務/產品列表
  pageTexts: string[];    // 各頁面嘅純文字（供 AI 分析用）
}

/**
 * 從 HTML 移除 script/style/nav 等無用標籤，返回可讀純文字（截斷至 maxLen）
 */
function htmlToText(html: string, maxLen = 3000): string {
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?<\/style>/gi, '')
    .replace(/<nav[\s\S]*?<\/nav>/gi, '')
    .replace(/<header[\s\S]*?<\/header>/gi, '')
    .replace(/<footer[\s\S]*?<\/footer>/gi, '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, maxLen);
}

/**
 * 從 HTML 抽 meta description
 */
function extractMetaDescription(html: string): string {
  const m = html.match(/<meta[^>]+name=["']description["'][^>]+content=["']([^"']+)["']/i)
    || html.match(/<meta[^>]+content=["']([^"']+)["'][^>]+name=["']description["']/i)
    || html.match(/<meta[^>]+property=["']og:description["'][^>]+content=["']([^"']+)["']/i);
  return m ? m[1].trim() : '';
}

/**
 * 第一層：用 Node fetch 直接爬官網 HTML。
 * 抽 email、phone、公司簡介、服務/產品。
 */
async function scrapeWebsite(url: string): Promise<ScrapeResult> {
  const emails: string[] = [];
  const phones: string[] = [];
  const pageTexts: string[] = [];
  let description = '';
  const services: string[] = [];

  const base = url.replace(/\/+$/, '');
  const pagesToTry = [
    { url: base, type: 'home' },
    { url: base + '/about', type: 'about' },
    { url: base + '/about-us', type: 'about' },
    { url: base + '/services', type: 'services' },
    { url: base + '/products', type: 'services' },
    { url: base + '/what-we-do', type: 'services' },
    { url: base + '/contact', type: 'contact' },
    { url: base + '/contact-us', type: 'contact' },
  ];

  for (const page of pagesToTry) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 10_000);
      const res = await fetch(page.url, {
        signal: controller.signal,
        headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36' },
        redirect: 'follow',
      });
      clearTimeout(timeout);
      if (!res.ok) continue;
      const html = await res.text();
      const text = htmlToText(html);
      pageTexts.push(`[${page.type}] ${text}`);

      // ── 抽 email ──
      const mailtoMatches = html.match(/mailto:([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})/gi) || [];
      for (const m of mailtoMatches) {
        emails.push(m.replace(/^mailto:/i, '').toLowerCase());
      }
      const emailPattern = /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/g;
      const pageEmails = html.match(emailPattern) || [];
      for (const e of pageEmails) {
        const lower = e.toLowerCase();
        if (!lower.endsWith('.png') && !lower.endsWith('.jpg') && !lower.endsWith('.svg') &&
            !lower.endsWith('.gif') && !lower.endsWith('.webp') && !lower.endsWith('.woff') &&
            !lower.includes('example.com') && !lower.includes('sentry.io') &&
            !lower.includes('webpack') && !lower.includes('schema.org')) {
          emails.push(lower);
        }
      }

      // ── 抽電話 ──
      const phonePattern = /(?:\+?\d{1,4}[\s\-.]?)?\(?\d{2,4}\)?[\s\-.]?\d{3,4}[\s\-.]?\d{3,4}/g;
      const pagePhones = html.match(phonePattern) || [];
      for (const ph of pagePhones) {
        const digits = ph.replace(/\D/g, '');
        if (digits.length >= 8 && digits.length <= 15) phones.push(ph.trim());
      }

      // ── 抽公司簡介（首頁 / about 頁嘅 meta description 或首段文字） ──
      if (!description && (page.type === 'home' || page.type === 'about')) {
        const meta = extractMetaDescription(html);
        if (meta) {
          description = meta;
        } else if (text.length > 50) {
          // 用首 300 字做簡介
          description = text.slice(0, 300).trim();
        }
      }

      // ── 抽服務/產品（services / products 頁面嘅列表項） ──
      if (page.type === 'services') {
        // 嘗試抽 <li> 內容或 <h2>/<h3> 標題作為服務名
        const headings = html.match(/<(?:h[23]|li)[^>]*>([^<]{3,80})<\/(?:h[23]|li)>/gi) || [];
        for (const h of headings.slice(0, 15)) {
          const clean = h.replace(/<[^>]+>/g, '').trim();
          if (clean.length >= 3 && clean.length <= 80) services.push(clean);
        }
      }
    } catch { /* 忽略 fetch 失敗嘅頁面 */ }
  }

  return {
    emails: [...new Set(emails)],
    phones: [...new Set(phones)],
    description,
    services: [...new Set(services)].slice(0, 10),
    pageTexts,
  };
}

/** S2 enrich → 三層策略：爬蟲 → Hermes browser → domain fallback */
async function doEnrich(p: any, db: Db) {
  const _id = new ObjectId(p.lead_object_id);
  const lead = await db.collection('leads').findOne({ _id });
  if (!lead) throw new Error('lead 唔存在');

  let foundEmail = '';
  let foundPhone = '';
  let foundAddress = '';
  let via = '';

  let scraped: ScrapeResult | null = null;

  // ── 第一層：Node fetch 爬蟲（快、靜默） ──
  if (lead.website) {
    log(`enrich 第一層：爬 ${lead.website}`);
    scraped = await scrapeWebsite(
      lead.website.startsWith('http') ? lead.website : `https://${lead.website}`
    );
    if (scraped.emails.length > 0) foundEmail = scraped.emails[0];
    if (scraped.phones.length > 0 && !lead.phone) foundPhone = scraped.phones[0];
    if (foundEmail) via = 'scraper';
    log(`  爬蟲結果: emails=${scraped.emails.length}, phones=${scraped.phones.length}, desc=${scraped.description.length > 0}, services=${scraped.services.length}`);
  }

  // ── 第二層：Hermes stealth browser（如果爬蟲搵唔到 email） ──
  if (!foundEmail) {
    log('enrich 第二層：叫 Hermes browser');
    const target = lead.website
      ? `Use your stealth browser to visit ${lead.website}`
      : `Use your stealth browser to search the web for "${lead.company_name}" and find its site`;
    const prompt = `${target} (company: ${lead.company_name}).
You MUST find the company's contact email. Try these steps in order:
1. Check the page source for mailto: links
2. Visit /contact, /contact-us, /about, /about-us pages
3. Look in the page footer
4. Check for email addresses in any visible text
5. If there's a contact form, look for a displayed email near it
Also find phone and address if visible.
Output ONLY JSON, empty string if not found:
{"email":"","phone":"","address":""}`;
    try {
      const c = hermesJson(prompt, { timeout: 300_000 });
      if (c.email && !foundEmail) { foundEmail = c.email; via = 'hermes'; }
      if (c.phone && !foundPhone) foundPhone = c.phone;
      if (c.address) foundAddress = c.address;
    } catch (e: any) {
      log('  Hermes enrich 失敗:', e?.message);
    }
  }

  // ── 第三層：Domain fallback（用常見 prefix 推測 email） ──
  if (!foundEmail && lead.website) {
    const domain = extractDomain(lead.website);
    if (domain) {
      // 試常見 email prefix
      const guesses = [`info@${domain}`, `contact@${domain}`, `hello@${domain}`, `enquiry@${domain}`];
      foundEmail = guesses[0]; // 用 info@ 做 fallback
      via = 'domain_guess';
      log(`  第三層 fallback: ${foundEmail}`);
    }
  }

  const set: any = { _website_researched: true, _enrich_via: via };
  if (foundEmail) set.email = foundEmail;
  if (foundPhone) set.phone = foundPhone;
  if (foundAddress && !lead.address) set.address = foundAddress;

  // 寫入爬蟲抽到嘅公司簡介 + 服務（供後續 S2 analyze 用）
  if (scraped) {
    if (scraped.description) set.website_description = scraped.description;
    if (scraped.services.length > 0) set._scraped_services = scraped.services;
    if (scraped.emails.length > 1) set.extra_emails = scraped.emails.slice(1, 5);
    if (scraped.phones.length > 1) set.extra_phones = scraped.phones.slice(1, 5);
    // 儲存頁面文字摘要（截斷至 5000 字）供 AI 分析
    if (scraped.pageTexts.length > 0) {
      set._scraped_text = scraped.pageTexts.join('\n\n').slice(0, 5000);
    }
  }

  await db.collection('leads').updateOne({ _id }, { $set: set });
  return {
    enriched: true,
    via,
    found: { email: foundEmail, phone: foundPhone, address: foundAddress },
    scraped_desc: !!scraped?.description,
    scraped_services: scraped?.services.length ?? 0,
  };
}

/** S2 分析 → 有爬蟲資料就直接 LLM 分析，冇就叫 Hermes 開瀏覽器 → 回寫 _collab_* + analyses */
async function doAnalyze(p: any, db: Db) {
  const _id = new ObjectId(p.lead_object_id);
  const lead = await db.collection('leads').findOne({ _id });
  if (!lead) throw new Error('lead 唔存在');

  let out: string;
  let via: string;

  if (lead._scraped_text) {
    // 已有爬蟲資料 → 直接用 LLM 分析，唔使開瀏覽器
    via = 'hermes-llm';
    const desc = lead.website_description || '';
    const svcs = (lead._scraped_services || []).join(', ');
    const text = (lead._scraped_text || '').slice(0, 4000);
    const prompt = `You are a B2B research assistant representing ${BRAND_NAME} (${BRAND_TAGLINE}),
a Hong Kong digital agency.
${BRAND_CONTEXT_BLOCK}

Company to research: ${lead.company_name}
Website: ${lead.website || 'N/A'}
Company description: ${desc}
Their services: ${svcs}
Website content (scraped):
${text}

Based on the above information, propose a SPECIFIC collaboration angle —
i.e. WHICH of ${BRAND_NAME}'s services (STRATEGIZE / DESIGN / CODE / MARKET, or any
of: ${BRAND_SOLUTIONS.join(' / ')}) maps best to this lead's pain points, and why.
${BRAND_TONE_GUIDE}
Output ONLY one JSON object, no other text:
{"primary":"主要合作方向","pitch":"一句 pitch (港式繁中 + EN mix OK)","reason":"理由","services":["服務1","服務2"]}`;
    out = callHermes(prompt);
  } else {
    // 冇爬蟲資料 → 用 Hermes 瀏覽器去睇官網
    via = 'hermes-browser';
    const site = lead.website
      ? `Use your browser to visit the company website: ${lead.website}`
      : `Use your browser to search the web for this company and find its website`;
    const prompt = `You are a B2B research assistant representing ${BRAND_NAME} (${BRAND_TAGLINE}),
a Hong Kong digital agency.
${BRAND_CONTEXT_BLOCK}

Company to research: ${lead.company_name}
${site}
Use your stealth browser normally to avoid bot detection. Read what the company does,
then propose a SPECIFIC collaboration angle — i.e. WHICH of ${BRAND_NAME}'s services
(STRATEGIZE / DESIGN / CODE / MARKET, or any of: ${BRAND_SOLUTIONS.join(' / ')})
maps best to this lead's pain points, and why.
${BRAND_TONE_GUIDE}
Output ONLY one JSON object, no other text:
{"primary":"主要合作方向","pitch":"一句 pitch (港式繁中 + EN mix OK)","reason":"理由","services":["服務1","服務2"]}`;
    out = callHermes(prompt);
  }

  const c = extractJson(out);

  await db.collection('leads').updateOne(
    { _id },
    {
      $set: {
        _has_analysis: true,
        _collab_primary: c.primary,
        _collab_pitch: c.pitch,
        _collab_reason: c.reason,
        _collab_services: c.services,
        _collab_generated_at: nowIso(),
        _analyzed_at: nowIso(),
      },
    },
  );
  await db.collection('analyses').insertOne({
    lead_id: lead.lead_id,
    analysis_type: 'full',
    ai_summary: c.pitch,
    _via: via,
    _analyzed_at: nowIso(),
  });
  return { analyzed: true, via, primary: c.primary };
}

/** S3 草稿 → 叫 Hermes(LLM) 寫 outreach email → email_draft + email_queue */
async function doDraft(p: any, db: Db) {
  const _id = new ObjectId(p.lead_object_id);
  const lead = await db.collection('leads').findOne({ _id });
  if (!lead) throw new Error('lead 唔存在');
  const prompt = `Write a short, warm B2B outreach email in 繁體中文 (港式英文夾雜 OK) from ${BRAND_NAME} (${BRAND_TAGLINE})
— a Hong Kong digital agency — to the company「${lead.company_name}」.

${BRAND_CONTEXT_BLOCK}

Collaboration angle: ${lead._collab_primary || ''} — ${lead._collab_pitch || ''}.
${BRAND_TONE_GUIDE}

Output ONLY JSON with subject + body. Body MUST end with this signature block:

${BRAND_SIGNATURE}

{"subject":"","body":""}`;
  const c = hermesJson(prompt);
  await db.collection('leads').updateOne(
    { _id },
    { $set: { email_draft: c.body, _has_email_draft: true } },
  );
  await db.collection('email_queue').insertOne({
    email_id: randomBytes(4).toString('hex'),
    lead_id: lead.lead_id,
    company_name: lead.company_name,
    to_email: lead.email,
    subject: c.subject,
    body: c.body,
    status: 'pending',
    _via: 'hermes',
    created_at: nowIso(),
  });
  return { drafted: true, via: 'hermes', subject: c.subject };
}

/**
 * S4 發送 —— ⚠️ 預設【唔真發 email】（發冷 email 俾真公司係不可逆）。
 * 預設只標 email_queue = approved（等人手 / 開關確認）。
 * set ENABLE_REAL_SEND=true 先會叫 Hermes 真發 + 標 contacted。
 */
async function doSend(p: any, db: Db) {
  const _id = new ObjectId(p.lead_object_id);
  const lead = await db.collection('leads').findOne({ _id });
  if (!lead) throw new Error('lead 唔存在');
  const eq = await db
    .collection('email_queue')
    .findOne({ lead_id: lead.lead_id, status: 'pending' });
  if (!eq) return { sent: false, note: '冇待發 email' };

  // 未開真發 → 只標 approved，唔寄
  if (process.env.ENABLE_REAL_SEND !== 'true') {
    await db
      .collection('email_queue')
      .updateMany(
        { lead_id: lead.lead_id, status: 'pending' },
        { $set: { status: 'approved' } },
      );
    return { sent: false, note: 'real send 停用（ENABLE_REAL_SEND=true 先發）' };
  }

  // 真發：經 SMTP（nodemailer），一律寄去 TEST_RECIPIENT
  if (!process.env.SMTP_HOST) {
    return { sent: false, note: 'SMTP 未配（.env 填 SMTP_HOST/USER/PASS）' };
  }
  const transporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: Number(process.env.SMTP_PORT || 587),
    secure: Number(process.env.SMTP_PORT) === 465,
    auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS },
  });
  // 收件人：SEND_OVERRIDE 有設（測試安全）→ 一律寄去嗰度；冇設 → lead.email（真發）。
  // creds（SMTP_USER/PASS）同收件人全部由跑 worker 嗰位喺 .env 設，唔 hardcode 落 code。
  const override = process.env.SEND_OVERRIDE;
  const to = override || lead.email;
  if (!to) return { sent: false, note: '冇收件人（lead 冇 email，又冇 SEND_OVERRIDE）' };
  try {
    await transporter.sendMail({
      from: process.env.SMTP_FROM || process.env.SMTP_USER,
      to,
      subject: override
        ? `[TEST→${lead.email || '?'}] ${eq.subject ?? ''}` // 測試模式標明原收件人
        : eq.subject ?? '',
      text: eq.body ?? '',
    });
  } catch (e: any) {
    // check 成功先標 sent（修「假 sent」bug）
    await db
      .collection('email_queue')
      .updateOne(
        { _id: eq._id },
        { $set: { status: 'failed', error: { send_error: e?.message ?? String(e) } } },
      );
    return { sent: false, error: e?.message ?? String(e) };
  }
  await db
    .collection('email_queue')
    .updateOne({ _id: eq._id }, { $set: { status: 'sent', sent_at: nowIso() } });
  await db
    .collection('leads')
    .updateOne({ _id }, { $set: { status: 'contacted', _email_sent: true } });
  return { sent: true, to };
}

// ───────── main loop ─────────
/** 登入重試（B API 一時連唔到都唔好死，backoff 重試）*/
async function loginWithRetry(): Promise<void> {
  for (let attempt = 1; ; attempt++) {
    try {
      await login();
      return;
    } catch (e: any) {
      const wait = Math.min(30_000, 2_000 * attempt);
      log(`登入失敗（第 ${attempt} 次）：${e?.message ?? e} —— ${wait / 1000}s 後重試`);
      await sleep(wait);
    }
  }
}

async function main() {
  log(`啟動 → API=${API} skill=${SKILL || 'any'}`);
  await loginWithRetry();
  const client = new MongoClient(MONGO);
  await client.connect();
  const db = client.db();

  let idle = 0;
  let running = true;
  process.on('SIGINT', () => {
    log('收到 SIGINT，停緊…');
    running = false;
  });

  while (running) {
    let task: any = null;
    try {
      task = await claim();
    } catch (e: any) {
      log('claim error:', e?.message ?? e);
      await sleep(POLL_MS);
      continue;
    }
    if (!task) {
      idle++;
      if (MAX_IDLE && idle >= MAX_IDLE) {
        log(`連續 ${idle} 次冇 task，停止`);
        break;
      }
      await sleep(POLL_MS);
      continue;
    }
    idle = 0;
    log(`接到 ${task.task_id} (${task.skill_id}) ${task.title ?? ''}`);
    try {
      const result = await handle(task, db);
      await complete(task.task_id, result);
      log(`✓ 完成 ${task.task_id}`, JSON.stringify(result));
    } catch (e: any) {
      log(`✗ ${task.task_id} 失敗:`, e?.message ?? e);
      // ⚠️ failTask 失敗唔可以 propagate 出去，否則會 process.exit(1)。
      // 內部再 try/catch，將 "fail 失敗" 變 warn log，繼續 loop
      // (DB 入面個 task 仲 stay 在 running，stale-minutes cron 之後會 requeue)。
      try {
        await failTask(task.task_id, e?.message ?? String(e));
      } catch (e2: any) {
        log(`⚠ failTask 都失敗咗 (${task.task_id}) — task 留喺 running，等 reap-stalled-tasks cron requeue:`, e2?.message ?? e2);
      }
    }
  }

  await client.close();
  log('已停止');
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// 安全網：唔好俾未捕捉嘅錯誤即刻殺 process（log 完繼續）
process.on('unhandledRejection', (e) =>
  console.error('[unhandledRejection]', e),
);
process.on('uncaughtException', (e) => console.error('[uncaughtException]', e));

main().catch((e) => {
  console.error('main crashed:', e);
  process.exit(1); // 交俾 pm2（已設 restart-delay）重啟
});
