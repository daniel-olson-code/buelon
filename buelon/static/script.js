// Global state
let currentData = null;
let currentWorker = null;
let currentJob = null;

// --- Safe rendering helpers -------------------------------------------------
// Everything the hub hands us (job names, ids, code, tracebacks, worker names)
// is untrusted text. It goes through esc() before it touches innerHTML, and
// through attr() before it lands inside a quoted attribute value. Anything
// large or newline-bearing (code, tracebacks, result JSON) skips innerHTML
// entirely and is assigned with textContent -- see setText().

// HTML-escape a value for interpolation into element content.
function esc(value) {
    return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

// Escape a value for use inside a double-quoted HTML attribute. Same escape
// set as esc(); a separate name so call sites read as attribute-safe.
const attr = esc;

// Fill a placeholder produced by a template string with untrusted text.
// Never use innerHTML for these -- code and tracebacks are arbitrary text.
function setText(root, selector, text) {
    const node = root.querySelector(selector);
    if (node) node.textContent = String(text ?? '');
}

// --- Theme (#14) ------------------------------------------------------------
// `localStorage.darkMode` stays the persisted key (it predates this file), but a
// first visit with nothing saved now follows the OS instead of assuming light --
// and keeps following it, live, until the operator picks a side.

const systemDark = window.matchMedia ? window.matchMedia('(prefers-color-scheme: dark)') : null;

function applyTheme(theme) {
    document.body.setAttribute('data-theme', theme);
    const toggle = document.getElementById('themeToggle');
    if (toggle) {
        const dark = theme === 'dark';
        toggle.setAttribute('aria-pressed', dark ? 'true' : 'false');
        toggle.title = dark ? 'Switch to light theme' : 'Switch to dark theme';
        const sr = toggle.querySelector('.sr-only');
        if (sr) sr.textContent = toggle.title;
    }
}

function initTheme() {
    const saved = localStorage.getItem('darkMode');
    const dark = saved === null ? !!(systemDark && systemDark.matches) : saved === 'true';
    applyTheme(dark ? 'dark' : 'light');

    // No saved preference yet -> track the OS. A click below stops this.
    if (saved === null && systemDark && systemDark.addEventListener) {
        systemDark.addEventListener('change', e => {
            if (localStorage.getItem('darkMode') === null) applyTheme(e.matches ? 'dark' : 'light');
        });
    }
}

function toggleTheme() {
    const next = document.body.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
    applyTheme(next);
    localStorage.setItem('darkMode', next === 'dark');
    announce(next === 'dark' ? 'Dark theme' : 'Light theme');
}

// Page navigation
// `showPage` only swaps which .page is active. It never writes history --
// the router below is the single writer, see the #15 block.
// The worker page's h1 (#17). It is sr-only and lives in index.html rather
// than being rebuilt, because a heading that comes and goes with a re-render is
// a heading a screen reader can be reading when it disappears.
function setPageTitle(text) {
    const el = document.getElementById('workerPageTitle');
    if (el) el.textContent = text || 'Worker';
}

// --- Tab title (#18) --------------------------------------------------------
// An operator leaves this tab in the background and comes back to it. The tab
// strip is the only part of the app they can see from there, so it carries the
// one number that would make them switch: the error count. Context comes second
// so a row of pinned tabs is still tellable apart.

const DOC_TITLE = 'Buelon Dashboard';

function updateDocTitle() {
    const parts = [];
    const errors = Number(currentData && currentData.counts && currentData.counts.errors) || 0;
    if (errors > 0) parts.push(`(${num(errors)} error${errors === 1 ? '' : 's'})`);

    if (currentMissing) {
        parts.push(currentMissing.kind === 'worker' ? 'Worker not found' : 'Job not found');
    } else if (currentPageNum === 3 && currentJob) {
        parts.push(currentJob.name);
    } else if (currentPageNum === 2 && currentWorker) {
        parts.push(currentWorker.name);
    }

    parts.push(DOC_TITLE);
    document.title = parts.join(' · ');
}

// Which of the three views is on screen. Only #18's transition direction reads
// it -- everything else asks the URL or the breadcrumb. It starts at 1 because
// `#page1` carries `.active` in index.html: a load with no hash never calls
// showPage() at all, and treating that as "nothing shown yet" made the first
// navigation of every session animate as an arrival instead of a step forward.
let currentPageNum = 1;

const PAGE_ENTER_CLASSES = ['is-enter-fwd', 'is-enter-back'];

// The entrance used to be an unconditional fade+rise on every showPage(), which
// meant a step *back* animated as an arrival. Match the breadcrumb instead:
// deeper slides in from the right, shallower from the left (#18).
function playPageEnter(el, pageNum) {
    PAGE_ENTER_CLASSES.forEach(c => el.classList.remove(c));
    if (prefersReducedMotion()) return;
    // Re-showing the page you are already on is not navigation.
    if (currentPageNum === pageNum) return;
    const cls = pageNum > currentPageNum ? 'is-enter-fwd' : 'is-enter-back';
    // The keyframes translate, so the page is a containing block while they
    // run. Strip the class on the way out rather than leaving every view with
    // a stale transform context (sticky table headers live inside it).
    el.addEventListener('animationend', () => el.classList.remove(cls), { once: true });
    // Restart the animation even when the class name is the one just removed.
    void el.offsetWidth;
    el.classList.add(cls);
}

function showPage(pageNum, updateBreadcrumb = true) {
    // Leaving the job page ends any run streaming into its console (#13):
    // the abort kills the subprocess on the hub, so an abandoned page cannot
    // leave real code executing behind it.
    if (pageNum !== 3) stopRunConsole();
    // Time travel is a dashboard mode (#20). Walking off the dashboard -- a
    // deep link, the Back button, a breadcrumb -- ends it, so nobody returns
    // to a frozen ledger with no memory of why it stopped updating.
    if (pageNum !== 1) exitHistory();
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
    const page = document.getElementById(`page${pageNum}`);
    page.classList.add('active');
    playPageEnter(page, pageNum);
    currentPageNum = pageNum;
    updateDocTitle();

    // Auto-refresh is skipped off the dashboard, so what is on it may be older
    // than one cadence by the time the operator walks back. Poll on arrival.
    if (pageNum === 1 && autoRefresh && lastSuccess !== null
        && Date.now() - lastSuccess >= REFRESH_MS) {
        nextPollAt = Date.now();
    }

    paintBackButtons();

    if (updateBreadcrumb) {
        updateBreadcrumbNav(pageNum);
    }
}

// The two "← Back" buttons predate the breadcrumb and the URL, and are now the
// third way back rather than the only one. They still have to be honest about
// where they land: a deep-linked job has no worker behind it, and the errors
// list is not a worker.
function paintBackButtons() {
    // A dangling deep link offers its own way out, so the shell button would
    // just be the same words twice.
    document.querySelectorAll('.page .btn-back').forEach(b => { b.hidden = !!currentMissing; });

    const back = document.querySelector('#page3 .btn-back');
    if (!back) return;
    const target = currentWorker ? 2 : 1;
    back.dataset.page = String(target);
    back.textContent = target === 1
        ? '← Back to Dashboard'
        : (jobsAreErrors() ? '← Back to Failed Jobs' : '← Back to Worker');
}

function updateBreadcrumbNav(pageNum) {
    const breadcrumb = document.getElementById('breadcrumb');
    if (!breadcrumb) return;

    // [label, page-to-navigate-to or null for the current crumb]
    const workerName = currentWorker ? currentWorker.name : 'Worker';
    const trail = [['Dashboard', pageNum === 1 ? null : 1]];
    if (currentMissing) {
        // A dangling deep link has no worker or job to name.
        trail.push([currentMissing.kind === 'worker' ? 'Worker not found' : 'Job not found', null]);
    } else {
        if (pageNum >= 2) trail.push([workerName, pageNum === 2 ? null : 2]);
        if (pageNum >= 3) trail.push([currentJob ? currentJob.name : 'Job', null]);
    }

    breadcrumb.innerHTML = `<ol class="breadcrumb-list">${trail.map(([label, page], i) => `
        <li>
            ${i ? '<span class="breadcrumb-separator" aria-hidden="true">\u203a</span>' : ''}
            ${page === null
                ? `<span class="breadcrumb-current" aria-current="page">${esc(label)}</span>`
                : `<button type="button" class="breadcrumb-link" data-action="nav-page"
                           data-page="${attr(page)}">${esc(label)}</button>`}
        </li>`).join('')}</ol>`;
}

// --- Routing (#15) ----------------------------------------------------------
// The three "pages" used to be display toggles with no URL, so a reload always
// landed on the dashboard and Back left the app. They now have addresses:
//
//   #/                 dashboard
//   #/worker/<id>      one worker's held jobs   (id `errors` = the failed list)
//   #/job/<id>         one job's detail page
//
// One direction of truth: every navigation writes the hash and `applyRoute()`
// -- driven by `hashchange`, which is what a hash-only history entry fires
// instead of `popstate` -- is the only thing that swaps pages. That is what
// makes Back walk the breadcrumb rather than exiting, with no separate history
// stack of our own to drift out of sync.
//
// Routes resolve against loaded data, so a route that arrives before the first
// /data lands is parked in `pendingRoute` and replayed by `initApp()`. An id
// that resolves to nothing is a designed state (`renderGone`), not a blank
// page -- a job that finished or was reset is genuinely gone, and saying so is
// the whole point of having deep links.

const ERRORS_WORKER_ID = 'errors';

let currentMissing = null;   // {kind:'worker'|'job', id} while a route dangles
let pendingRoute = null;     // waiting for the first successful /data

function parseRoute(hash) {
    const parts = String(hash || '').replace(/^#\/?/, '').split('/');
    const raw = parts.slice(1).join('/');
    let id;
    try {
        id = decodeURIComponent(raw);
    } catch {
        id = raw;  // a hand-mangled %-escape is still a lookup that will miss
    }
    if (parts[0] === 'worker' && id) return { page: 2, workerId: id };
    if (parts[0] === 'job' && id) return { page: 3, jobId: id };
    return { page: 1 };
}

function routeHash(route) {
    if (route.page === 2) return `#/worker/${encodeURIComponent(route.workerId)}`;
    if (route.page === 3) return `#/job/${encodeURIComponent(route.jobId)}`;
    return '#/';
}

// Push (or replace) a route and let the hashchange handler render it. When the
// hash is already what we want -- clicking the job you are already on, or the
// deep link the page loaded with -- nothing fires, so apply it directly.
function navigate(route, options = {}) {
    const hash = routeHash(route);
    if (window.location.hash === hash) {
        applyRoute(route);
    } else if (options.replace) {
        history.replaceState(null, '', hash);
        applyRoute(route);
    } else {
        window.location.hash = hash;
    }
}

// The breadcrumb and the two Back buttons say "up one level"; what that means
// depends on where you are, and a job reached by deep link has no level above
// it but the dashboard.
function navPage(pageNum) {
    if (pageNum >= 2 && currentWorker && currentWorker.id) {
        navigate({ page: 2, workerId: currentWorker.id });
        return;
    }
    navigate({ page: 1 });
}

function errorsWorker() {
    return { id: ERRORS_WORKER_ID, name: 'Errors', jobs: errorJobs };
}

function applyRoute(route) {
    // Nothing resolves before the first payload. Park it, sit on the
    // dashboard (which is showing the loader or the disconnected banner), and
    // let initApp() replay it the moment there is data to resolve against.
    if (!currentData) {
        pendingRoute = route.page === 1 ? null : route;
        currentMissing = null;
        showPage(1);
        return;
    }
    pendingRoute = null;
    currentMissing = null;

    if (route.page === 2) {
        if (route.workerId === ERRORS_WORKER_ID) {
            if (!errorJobs.length) { renderGone('worker', route.workerId); return; }
            currentWorker = errorsWorker();
            renderWorkerJobs();
            showPage(2);
            return;
        }
        if (!currentData.workers || !currentData.workers[route.workerId]) {
            renderGone('worker', route.workerId);
            return;
        }
        selectWorker(route.workerId);
        return;
    }

    if (route.page === 3) {
        const found = lookupJob(route.jobId);
        if (!found) { renderGone('job', route.jobId); return; }
        if (found.action === 'select-error-job') selectErrorJob(route.jobId);
        else selectJob(route.jobId);
        return;
    }

    currentJob = null;
    showPage(1);
}

// A deep link that resolves to nothing. Not an error -- the usual reason is
// that the work finished, which is the good outcome -- so the copy explains
// rather than apologises, and offers the two things that actually help.
function renderGone(kind, id) {
    currentMissing = { kind, id };
    currentJob = null;
    currentWorker = null;

    const worker = kind === 'worker';
    const errors = worker && id === ERRORS_WORKER_ID;
    const host = document.getElementById(worker ? 'jobsSection' : 'jobDetail');
    if (!host) return;

    const lead = errors ? 'No failed jobs' : (worker ? 'This worker is gone' : 'This job is gone');
    const body = errors
        ? `Nothing has failed, so there is no list to show. The Errors section only
           exists on the dashboard while at least one job is parked — a successful
           <em>Requeue</em> makes this link empty too.`
        : worker
            ? `No worker with this id is connected to the hub right now. Workers appear
               here only while connected, so this one has either shut down or lost its
               connection — and everything it was holding was requeued.`
            : `The hub has no job with this id in flight. Most likely it finished and
               left the counts, or a <em>Requeue</em> gave it a fresh start. Failed and
               held jobs are the only ones with a page of their own.`;

    // #page2's <section> is labelled by `jobsSectionTitle`, which normally comes
    // from the #6 section header. There is no header here, so the lead takes
    // the id rather than leaving the reference dangling.
    host.innerHTML = `
        <div class="gone">
            <svg class="gone-icon" viewBox="0 0 24 24" aria-hidden="true" focusable="false">
                <circle cx="12" cy="12" r="9"></circle>
                <path d="M8.5 12h7"></path>
            </svg>
            <!-- An h2, not a div (#17): when this is the worker page it *is*
                 that section's heading, and #jobsSection points its
                 aria-labelledby at this id. A styled div left the page with no
                 heading at all. -->
            <h2 class="gone-lead"${worker ? ' id="jobsSectionTitle"' : ''}>${esc(lead)}</h2>
            <p class="gone-body">${body}</p>
            ${errors ? '' : `<div class="gone-id"><span class="gone-id-label">${
                worker ? 'Worker' : 'Job'} id</span><code class="gone-id-value"></code></div>`}
            <div class="gone-actions">
                <button type="button" class="btn btn-primary" data-action="nav-page" data-page="1">
                    Back to Dashboard</button>
                <button type="button" class="btn btn-secondary" data-action="recheck-route">Check again</button>
            </div>
        </div>
    `;
    if (!errors) setText(host, '.gone-id-value', id);
    if (worker) setPageTitle(lead);

    showPage(worker ? 2 : 3);
    announce(lead);
}

// API functions
// `null` means the hub could not be reached (or answered with an error status),
// which is a visible app state now (#14): the banner, not a blank page. An HTTP
// error used to sail through `r.json()` and blow up somewhere in rendering.
const getData = async () => {
    try {
        const r = await fetch('/data', {
            method: 'POST',
            body: '{}',
            headers: {'Content-Type': 'application/json', 'Accept': 'application/json'}
        });
        if (!r.ok) {
            console.error('getData: hub answered', r.status, r.statusText);
            return null;
        }
        const j = await r.json();
        console.log('getData:', j);
        return j;
    } catch (error) {
        console.error('Error fetching data:', error);
        return null;
    }
};

// `{ok: true, data}` or `{ok: false, error}`. This used to return `null` on
// failure and the caller quietly kept `errorJobs = []`, which hid the whole
// Errors section while the ledger went on reporting "18 errors" two inches
// above it (#16). A fetch we could not make is not the same answer as "none".
const getErrorData = async () => {
    try {
        const r = await fetch('/errors', {
            method: 'POST',
            body: '{}',
            headers: {'Content-Type': 'application/json', 'Accept': 'application/json'}
        });
        if (!r.ok) {
            console.error('getErrorData: hub answered', r.status, r.statusText);
            return {ok: false, error: `The hub answered ${r.status} ${r.statusText}.`};
        }
        const j = await r.json();
        console.log('getErrorData:', j);
        return {ok: true, data: j};
    } catch (error) {
        console.error('Error fetching error data:', error);
        return {ok: false, error: String(error && error.message ? error.message : error)};
    }
};


// `null` means it did not happen. There was no `r.ok` check here, so a 500
// whose body happened to be JSON parsed as a success and the dashboard
// reported a requeue that never took place (#16).
const resetErrors = async () => {
    try {
        const r = await fetch('/reset-errors', {
            method: 'POST',
            body: '{}',
            headers: {'Content-Type': 'application/json', 'Accept': 'application/json'}
        });
        if (!r.ok) {
            console.error('resetErrors: hub answered', r.status, r.statusText);
            return null;
        }
        const j = await r.json();
        console.log('resetErrors:', j);
        return j;
    } catch (error) {
        console.error('Error resetting errors:', error);
        return null;
    }
};

// Returns `{ok: true, data}` -- where `data` is legitimately `null` when the hub has
// no record of the job -- or `{ok: false, error}` when the hub could not be reached.
// The two are different states on screen (#12), so they cannot both be `null` here.
const getJobParentAndResults = async (jobId) => {
    try {
        const r = await fetch('/job-parents-and-results', {
            method: 'POST',
            body: JSON.stringify({id: jobId}),
            headers: {'Content-Type': 'application/json', 'Accept': 'application/json'}
        });
        if (!r.ok) return {ok: false, error: `The hub answered ${r.status} ${r.statusText}.`};
        return {ok: true, data: await r.json()};
    } catch (error) {
        console.error('Error fetching job parents:', error);
        return {ok: false, error: String(error && error.message ? error.message : error)};
    }
};

// `{ok: true, data}` or `{ok: false, error}`. History is a *bonus*: the
// dashboard has to keep working when the sampler is broken or the server
// predates #19, so every caller falls back rather than failing.
const getHistory = async (query) => {
    try {
        const r = await fetch('/history', {
            method: 'POST',
            body: JSON.stringify(query || {}),
            headers: {'Content-Type': 'application/json', 'Accept': 'application/json'}
        });
        if (!r.ok) return {ok: false, error: `The hub answered ${r.status} ${r.statusText}.`};
        return {ok: true, data: await r.json()};
    } catch (error) {
        return {ok: false, error: String(error && error.message ? error.message : error)};
    }
};

// `{ok: true, config}` or `{ok: false, error}` -- and a 400 here carries the
// server's own `detail`, which names the allowed set. Show it verbatim.
const setHistoryInterval = async (minutes) => {
    try {
        const r = await fetch('/history/config', {
            method: 'POST',
            body: JSON.stringify({interval_minutes: minutes}),
            headers: {'Content-Type': 'application/json', 'Accept': 'application/json'}
        });
        const body = await r.json().catch(() => null);
        if (!r.ok) {
            return {ok: false, error: (body && body.detail)
                || `The hub answered ${r.status} ${r.statusText}.`};
        }
        return {ok: true, config: body && body.config};
    } catch (error) {
        return {ok: false, error: String(error && error.message ? error.message : error)};
    }
};

// The run-job stream lives with the console that renders it: see the
// `--- Run-job console (#13) ---` block down by `jobRunSection`.


// `1536` -> `'1.5 KB'`. Mirrors `hub.format_bytes`.
function formatBytes(n) {
    let size = Number(n) || 0;
    const units = ['B', 'KB', 'MB', 'GB'];
    for (let i = 0; i < units.length; i++) {
        if (size < 1024 || i === units.length - 1) {
            return i === 0
                ? `${Math.round(size)} B`
                : `${size.toLocaleString(undefined, {minimumFractionDigits: 1, maximumFractionDigits: 1})} ${units[i]}`;
        }
        size /= 1024;
    }
}

// Epoch seconds -> '3h ago'. The hub does not always send `created`, so this
// returns '' for anything missing or non-finite rather than guessing.
function timeAgo(epochSeconds) {
    const t = Number(epochSeconds);
    if (!Number.isFinite(t) || t <= 0) return '';
    const seconds = Math.max(0, Math.floor(Date.now() / 1000 - t));
    const units = [['d', 86400], ['h', 3600], ['m', 60]];
    for (const [suffix, size] of units) {
        if (seconds >= size) return `${Math.floor(seconds / size)}${suffix} ago`;
    }
    return 'just now';
}

// --- Generic modal ----------------------------------------------------------
// One shell for every modal in the app. `openModal({title, body, size, key,
// onClose})` builds it inside `#modalRoot`, traps Tab inside it, restores
// focus to whatever opened it, closes on Esc / backdrop click / the close
// button, locks `<body>` scroll while anything is open, and stacks correctly
// if two are open at once. Returns a handle so async content can fill it in
// later.
//
//   size     'sm' (help copy, 560px) | 'lg' (parent tree, tracebacks, 900px)
//   key      identity. Opening the same key again re-uses the modal that is
//            already up (focused, not stacked) instead of piling duplicates
//            on top of each other -- a double click or Enter+click on an (i)
//            must not open two of the same document.
//   onClose  called once, after the modal leaves the stack. This is where a
//            modal with live work behind it (a streaming log, a poll) stops
//            that work; the callback runs even when Esc or the backdrop
//            closed the modal, so there is no path that leaks it.
//
// A closing modal is dead to input: it is out of the stack, `inert`, and
// `pointer-events: none`, so a click that lands during the 140ms exit can
// never reach through and close the modal underneath it.

const FOCUSABLE = [
    'a[href]', 'button:not([disabled])', 'input:not([disabled])',
    'select:not([disabled])', 'textarea:not([disabled])',
    '[tabindex]:not([tabindex="-1"])',
].join(',');

// Innermost modal last. Esc and `close-modal` always act on the top of it.
let modalStack = [];
let modalSeq = 0;

function openModal(options) {
    const opts = options || {};
    const root = document.getElementById('modalRoot');
    if (!root) return null;

    // Same document asked for twice: raise the one that is already open.
    if (opts.key) {
        const open = modalStack.find(m => m.key === opts.key);
        if (open) {
            if (opts.body !== undefined) open.setBody(opts.body);
            // Re-raised from somewhere else on the page: Esc should return you
            // to *this* opener, not to the one that first opened it (#17).
            if (document.activeElement && document.activeElement !== document.body) {
                open.restore = document.activeElement;
            }
            focusInside(open.el);
            return open;
        }
    }

    // A tooltip is a hover hint for the page behind; it sits above the modal
    // layer (z 1200) and must not be left floating over a dialog.
    if (typeof hideTip === 'function') hideTip();

    const id = `modal${++modalSeq}`;
    const titleId = `${id}Title`;
    const el = document.createElement('div');
    el.className = 'modal is-open';
    el.id = id;
    el.dataset.size = opts.size === 'lg' ? 'lg' : 'sm';
    el.setAttribute('role', 'dialog');
    el.setAttribute('aria-modal', 'true');
    el.setAttribute('aria-labelledby', titleId);
    el.innerHTML = `
        <div class="modal-content">
            <div class="modal-header">
                <h2 class="modal-title" id="${attr(titleId)}">${esc(opts.title || '')}</h2>
                <button class="close-btn" type="button" data-action="close-modal" aria-label="Close">
                    <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor"
                         stroke-width="2" stroke-linecap="round" aria-hidden="true">
                        <path d="M6 6l12 12M18 6 6 18"/>
                    </svg>
                </button>
            </div>
            <div class="modal-body"></div>
        </div>
    `;

    const bodyEl = el.querySelector('.modal-body');
    bodyEl.innerHTML = opts.body || '';

    const handle = {
        id,
        el,
        bodyEl,
        key: opts.key || null,
        onClose: typeof opts.onClose === 'function' ? opts.onClose : null,
        restore: document.activeElement,
        setBody: html => { bodyEl.innerHTML = html; },
        close: () => closeModal(handle),
    };

    // Whatever was on top is now underneath: take it out of the tab order and
    // out of the accessibility tree so the stack has exactly one live dialog.
    setModalInert(modalStack[modalStack.length - 1], true);

    root.appendChild(el);
    if (!modalStack.length) setBackgroundInert(true);
    modalStack.push(handle);
    document.body.classList.add('modal-open');

    // Modals live outside the page containers, so they need their own
    // delegated listener -- `data-action` inside a modal works as usual.
    delegateClicks(el);
    el.addEventListener('click', e => { if (e.target === el) closeModal(handle); });
    el.addEventListener('keydown', e => trapFocus(e, el));

    focusInside(el);

    return handle;
}

// Focus the first thing in a dialog worth reading/acting on, falling back to
// its Close button -- so focus is never left out on the page behind.
function focusInside(el) {
    const body = el.querySelector('.modal-body');
    const first = (body && body.querySelector(FOCUSABLE)) || el.querySelector('.close-btn');
    if (first) first.focus();
}

// Anything behind the live dialog -- a stacked modal, one on its way out, or
// the whole page -- must be unreachable by Tab, click and screen reader alike.
//
// `inert` is the only mechanism that does all three, and every engine that can
// run this app has it. Where it is missing we fall back to `aria-hidden` PLUS
// removing each descendant from the tab order, because `aria-hidden` alone on a
// subtree that still holds a focusable Close button is the exact WCAG 4.1.2
// violation it looks like it is fixing: a screen-reader user tabs into a
// control the screen reader refuses to describe (#17).
const HAS_INERT = typeof HTMLElement !== 'undefined' && 'inert' in HTMLElement.prototype;

function setInert(el, inert) {
    if (!el) return;
    el.inert = inert;
    if (HAS_INERT) return;
    if (inert) {
        el.setAttribute('aria-hidden', 'true');
        el.querySelectorAll(FOCUSABLE).forEach(node => {
            if (node.dataset.inertTabindex === undefined) {
                node.dataset.inertTabindex = node.getAttribute('tabindex') ?? '';
            }
            node.setAttribute('tabindex', '-1');
        });
    } else {
        el.removeAttribute('aria-hidden');
        el.querySelectorAll('[data-inert-tabindex]').forEach(node => {
            const prev = node.dataset.inertTabindex;
            if (prev === '') node.removeAttribute('tabindex');
            else node.setAttribute('tabindex', prev);
            delete node.dataset.inertTabindex;
        });
    }
}

function setModalInert(handle, inert) {
    if (handle) setInert(handle.el, inert);
}

// The page behind the dialog. `aria-modal` alone is a promise that assistive
// tech may or may not keep, and it does nothing at all for a keyboard user
// tabbing out of the dialog into the header's refresh button. #toastRoot is
// deliberately left reachable: it sits above the modal layer, and a toast that
// cannot be dismissed is worse than one that can.
function setBackgroundInert(inert) {
    ['header', 'main'].forEach(sel => setInert(document.querySelector(sel), inert));
}

// Tab and Shift+Tab wrap inside the dialog instead of escaping to the page.
function trapFocus(event, el) {
    if (event.key !== 'Tab') return;
    // `offsetParent` is null for a `position: fixed` node as well as a hidden
    // one, so check the rendered box too -- otherwise a fixed control inside a
    // dialog is silently dropped out of the trap (#17).
    const items = [...el.querySelectorAll(FOCUSABLE)].filter(node =>
        node === document.activeElement
        || node.offsetParent !== null
        || node.getClientRects().length > 0);
    if (!items.length) return;
    const first = items[0];
    const last = items[items.length - 1];
    if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
    }
}

// Closes the topmost modal, or a specific handle. Safe to call with nothing
// open, and safe to call twice on the same handle -- a modal that has already
// left the stack is ignored rather than closing whatever is now on top.
function closeModal(handle) {
    const target = handle || modalStack[modalStack.length - 1];
    if (!target) return;
    if (!modalStack.includes(target)) return;

    modalStack = modalStack.filter(m => m !== target);
    if (!modalStack.length) {
        document.body.classList.remove('modal-open');
        setBackgroundInert(false);
    }

    // Out of the stack, so out of reach: no click, Tab or Esc can address it
    // while the exit transition plays.
    setModalInert(target, true);
    const remove = () => target.el.remove();
    if (prefersReducedMotion()) {
        remove();
    } else {
        target.el.classList.add('is-closing');
        setTimeout(remove, 140);
    }

    // The modal below becomes the live one again.
    const below = modalStack[modalStack.length - 1];
    setModalInert(below, false);

    // Put focus back where it came from. The opener can be gone -- the
    // dashboard re-renders every 30s -- and then focus would be stranded on
    // <body>, so fall back to the dialog underneath.
    const restore = target.restore;
    if (restore && document.contains(restore) && typeof restore.focus === 'function') {
        restore.focus();
    } else if (below) {
        focusInside(below.el);
    }

    if (target.onClose) target.onClose();
}

// --- Toasts (#16) -----------------------------------------------------------
// The app has no `alert()` and never will. Actions that succeed or fail away
// from where you are looking -- a requeue that empties a section, a copy that
// the browser refused, a run that died after you scrolled past the console --
// say so here instead of nowhere.
//
//   toast('Requeued 18 failed jobs', {tone: 'ok'})
//   toast('Could not reach the hub', {tone: 'danger', detail: err,
//                                     action: {label: 'Retry', name: 'refresh'}})
//
// Rules that keep it from becoming noise:
//   * Only *outcomes* get a toast. State the operator can already see change
//     (sorting a table, turning a page, toggling the theme) does not.
//   * One toast per outcome: a repeat of the same `key` replaces its own toast
//     and restarts the clock instead of stacking a second copy.
//   * The toast IS the announcement. Tone picks `role`, so a screen reader
//     hears it once -- call sites that toast must not also `announce()`.
//   * Hover, focus or a held pointer freezes the clock, so a toast can never
//     expire out from under someone reading or clicking it.
const TOAST_MAX = 3;

const TOAST_TONES = {
    ok:     { life: 4200, role: 'status', icon: '<path d="m5 13 4.5 4.5L19 7"/>' },
    info:   { life: 5200, role: 'status', icon: '<circle cx="12" cy="12" r="9"/><path d="M12 11v6M12 7.6v.1"/>' },
    warn:   { life: 8000, role: 'alert',  icon: '<path d="M12 4.5 2.8 20h18.4z"/><path d="M12 10v4.4M12 17.4v.1"/>' },
    danger: { life: 9000, role: 'alert',  icon: '<circle cx="12" cy="12" r="9"/><path d="M12 7.5v6M12 16.4v.1"/>' },
};

const toasts = new Map();   // key -> element, for the replace-don't-stack rule
let toastSeq = 0;

function toastRoot() {
    let root = document.getElementById('toastRoot');
    if (!root) {
        root = document.createElement('div');
        root.id = 'toastRoot';
        root.className = 'toast-root';
        // The toasts carry their own role; the region must not double-announce.
        root.setAttribute('aria-label', 'Notifications');
        document.body.appendChild(root);
    }
    return root;
}

function toast(message, options = {}) {
    const opts = options || {};
    const tone = TOAST_TONES[opts.tone] ? opts.tone : 'info';
    const spec = TOAST_TONES[tone];
    const key = opts.key || `toast:${++toastSeq}`;
    const root = toastRoot();

    // Same outcome again: reuse the node so a retry loop cannot paper the
    // screen with identical cards.
    const existing = toasts.get(key);
    if (existing) dismissToast(existing, true);

    const el = mk('div', 'toast');
    el.dataset.tone = tone;
    el.dataset.key = key;
    el.setAttribute('role', spec.role);

    const mark = mk('span', 'toast-mark');
    mark.setAttribute('aria-hidden', 'true');
    mark.innerHTML = `
        <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor"
             stroke-width="2" stroke-linecap="round" stroke-linejoin="round"
             focusable="false">${spec.icon}</svg>`;   // constant markup

    const body = mk('div', 'toast-body');
    body.appendChild(mk('p', 'toast-message', String(message)));
    if (opts.detail) body.appendChild(mk('p', 'toast-detail', String(opts.detail)));

    // An action is how a failure toast stays useful: "Retry" next to the
    // reason beats a reason on its own. It goes UNDER the copy, not beside
    // it -- sharing the row squeezed the message into three wrapped lines.
    if (opts.action && opts.action.name) {
        const button = mk('button', 'btn btn-sm btn-ghost toast-action', opts.action.label || 'Retry');
        button.type = 'button';
        button.dataset.action = opts.action.name;
        Object.entries(opts.action.data || {}).forEach(([k, v]) => { button.dataset[k] = v; });
        button.addEventListener('click', () => dismissToast(el));
        body.appendChild(button);
    }

    el.append(mark, body);

    const close = mk('button', 'toast-close');
    close.type = 'button';
    close.setAttribute('aria-label', 'Dismiss');
    close.innerHTML = `
        <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor"
             stroke-width="2" stroke-linecap="round" aria-hidden="true" focusable="false">
            <path d="M6 6l12 12M18 6 6 18"/>
        </svg>`;                                        // constant markup
    close.addEventListener('click', () => dismissToast(el));
    el.appendChild(close);

    // The countdown bar is decoration synced to the timer, never the timer
    // itself: under `prefers-reduced-motion` its animation is killed, and a
    // toast that lived as long as its animation would vanish instantly.
    const life = Number(opts.life) || spec.life;
    const bar = mk('span', 'toast-life');
    bar.setAttribute('aria-hidden', 'true');
    bar.style.animationDuration = `${life}ms`;
    el.appendChild(bar);

    el.toastLife = life;
    el.toastLeft = life;
    delegateClicks(el);                                 // for `data-action`
    el.addEventListener('pointerenter', () => holdToast(el, true));
    el.addEventListener('pointerleave', () => holdToast(el, false));
    el.addEventListener('focusin', () => holdToast(el, true));
    el.addEventListener('focusout', () => holdToast(el, false));

    root.appendChild(el);
    toasts.set(key, el);

    // Oldest first out, so the newest outcome is always readable.
    while (root.children.length > TOAST_MAX) dismissToast(root.firstElementChild, true);

    startToastClock(el);
    return el;
}

function startToastClock(el) {
    clearTimeout(el.toastTimer);
    el.toastStarted = Date.now();
    el.toastTimer = setTimeout(() => dismissToast(el), el.toastLeft);
}

// Freeze the clock while the toast is being read or aimed at. Resuming keeps
// the remaining time rather than granting a fresh full life on every wobble of
// the pointer.
function holdToast(el, held) {
    if (!el.isConnected) return;
    el.classList.toggle('is-held', held);
    if (held) {
        clearTimeout(el.toastTimer);
        el.toastLeft = Math.max(600, el.toastLeft - (Date.now() - el.toastStarted));
    } else {
        startToastClock(el);
    }
}

function dismissToast(el, now) {
    if (!el || !el.isConnected) return;
    clearTimeout(el.toastTimer);
    if (toasts.get(el.dataset.key) === el) toasts.delete(el.dataset.key);
    if (now || prefersReducedMotion()) {
        el.remove();
        return;
    }
    // Collapse its own height on the way out so the stack closes the gap
    // instead of jumping.
    el.style.height = `${el.offsetHeight}px`;
    el.classList.add('is-leaving');
    requestAnimationFrame(() => { el.style.height = '0px'; });
    el.addEventListener('transitionend', event => {
        if (event.propertyName === 'height') el.remove();
    });
    setTimeout(() => el.remove(), 600);                 // belt and braces
}

// --- Loading, empty and failed: one vocabulary (#16) ------------------------
// Three different nothings, and the operator has to be able to tell them
// apart at a glance:
//
//   LOADING  a skeleton in the SHAPE of the thing that is coming, so the
//            layout does not shift when data lands. Never a spinner where a
//            layout is known.
//   EMPTY    the request worked and the answer is "none". Calm, explains WHY
//            it might be empty and what would fill it. Never the danger ramp:
//            an empty queue is usually the good news.
//   FAILED   we do not know. Danger ramp, the reason, and a way to try again.
//
// `blankState()` renders EMPTY and FAILED; `skelCell()` and the two
// section-shaped skeletons below render LOADING.

const BLANK_ICONS = {
    // Empty: an open, level tray -- nothing in it, nothing wrong with it.
    empty:   '<path d="M3 14h5l1.6 2.4h4.8L16 14h5"/><path d="M4.6 14 7 5.4h10L19.4 14v3.6a1.4 1.4 0 0 1-1.4 1.4H6a1.4 1.4 0 0 1-1.4-1.4z"/>',
    // Gone: the circle-and-dash of #15's dangling deep link.
    gone:    '<circle cx="12" cy="12" r="9"/><path d="M8 12h8"/>',
    // Failed: a struck-through signal fan.
    failed:  '<path d="M12 3v6"/><path d="M5.6 10.6a9 9 0 1 0 12.8 0"/><path d="M2 2l20 20"/>',
    // Warn: something is technically fine and nonetheless wants attention.
    warn:    '<path d="M12 4.5 2.8 20h18.4z"/><path d="M12 10v4.4M12 17.4v.1"/>',
};

// `kind` is the semantic ('empty' | 'failed'), `icon` and `tone` default from
// it so a call site only overrides them deliberately.
//
//   blankState({
//       kind: 'failed', title, body,
//       detail: 'the hub answered 503',        // verbatim machine text
//       hint: '<code>bue worker</code>',       // trusted markup, author-written
//       id: {label: 'Job id', value: jobId},   // + a copy button
//       actions: [{label: 'Try again', name: 'refresh', primary: true}],
//       frame: true,                           // dashed frame: it fills a section
//   })
//
// `frame` is explicit rather than inferred from a parent selector: the same
// component stands alone inside a section (frame) and inside a modal body or a
// card that already has a frame of its own (no frame).
function blankState(options) {
    const o = options || {};
    const kind = o.kind === 'failed' ? 'failed' : 'empty';
    const icon = BLANK_ICONS[o.icon] ? o.icon : (kind === 'failed' ? 'failed' : 'empty');
    const tone = o.tone || (kind === 'failed' ? 'danger' : 'idle');

    const actions = (o.actions || []).map(a => `
        <button type="button" class="btn ${a.primary ? 'btn-primary' : 'btn-secondary'}"
                data-action="${attr(a.name)}"${a.data
            ? Object.entries(a.data).map(([k, v]) => ` data-${attr(k)}="${attr(v)}"`).join('')
            : ''}>${esc(a.label)}</button>`).join('');

    return `
        <div class="blank" data-kind="${attr(kind)}"${o.frame ? ' data-frame="1"' : ''}>
            <span class="blank-mark" data-tone="${attr(tone)}" aria-hidden="true">
                <svg viewBox="0 0 24 24" width="22" height="22" fill="none" stroke="currentColor"
                     stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"
                     focusable="false">${BLANK_ICONS[icon]}</svg>
            </span>
            <h3 class="blank-title">${esc(o.title || '')}</h3>
            ${o.body ? `<p class="blank-body">${esc(o.body)}</p>` : ''}
            ${o.detail ? `<p class="blank-detail">${esc(o.detail)}</p>` : ''}
            ${o.hint ? `<p class="blank-hint">${o.hint}</p>` : ''}
            ${o.id ? `<p class="blank-id">
                <span class="blank-id-label">${esc(o.id.label || 'id')}</span>
                <span class="blank-id-value">${esc(o.id.value)}</span>
                ${copyButtonHTML(o.id.value, o.id.label || 'id')}
            </p>` : ''}
            ${actions ? `<div class="blank-actions">${actions}</div>` : ''}
        </div>`;
}

// --- Skeletons --------------------------------------------------------------
// A skeleton is only worth having if it is the right SHAPE: the point is that
// the real thing lands into the same boxes and nothing moves. So these mirror
// `renderLedger` and `workerCardEl` structurally, and live next to nothing
// else that could drift from them silently -- if you change either renderer's
// outer shape, change its skeleton.

// A skeleton cell IS the real element, wearing `.skel` and holding a single
// `&nbsp;` instead of content: same class, same type metrics, same padding, so
// the box it reserves is the box the real value will land in. Width is the
// only thing being guessed, and a wrong guess costs nothing.
//
// This is not a style choice -- it is the only version that cannot drift. The
// first cut hand-sized every shape and reserved 299px where the ledger needed
// 404px, which is a worse jump than having no skeleton at all.
function skelCell(cls, width) {
    return `<span class="${cls} skel" style="--skel-w:${width}">&nbsp;</span>`;
}

// The status ledger's own frame, filled with nothing: headline, proportion
// bar, five sum rows plus the total, and the two aside blocks.
function ledgerSkeleton() {
    const row = (label, value, cls) => `
        <div class="ledger-row ${cls || ''}">
            <span class="ledger-op"></span>
            <span class="skel skel-dot"></span>
            ${skelCell('ledger-label', label)}
            <span class="ledger-sub"></span>
            ${skelCell('ledger-num', value)}
        </div>`;
    // Ragged widths, because six identical bars read as a loading GRAPHIC and
    // these are meant to read as six rows of text that have not arrived.
    // The last one is the sum row, which is taller: `.is-total` is what makes
    // the skeleton's 250px of rows the real thing's 266px.
    const widths = ['7rem', '6rem', '6.5rem', '8rem', '5rem'];
    const rows = widths
        .map((w, i) => row(w, ['3rem', '2.4rem', '2.8rem', '3.4rem'][i % 4])).join('')
        + row('4rem', '3.6rem', 'is-total');
    const asideBlock = () => `
        <div class="ledger-aside-block">
            ${skelCell('ledger-aside-title', '6rem')}
            ${row('8rem', '2.6rem')}${row('7rem', '2.2rem')}
        </div>`;

    return `
        <div class="ledger-card card is-skeleton" role="status" aria-label="Loading job counts">
            <div class="ledger-headline">
                <div class="ledger-headline-main">
                    ${skelCell('ledger-headline-label', '4.5rem')}
                    ${skelCell('ledger-headline-value', '7rem')}
                </div>
                <div class="ledger-headline-meta">
                    <span class="ledger-meta-item">
                        ${skelCell('ledger-meta-value', '3.5rem')}
                        ${skelCell('ledger-meta-label', '4.5rem')}
                    </span>
                    <span class="ledger-meta-item">
                        ${skelCell('ledger-meta-value', '2.5rem')}
                        ${skelCell('ledger-meta-label', '4rem')}
                    </span>
                </div>
            </div>
            <div class="ledger-bar skel"></div>
            <div class="ledger-body">
                <div class="ledger-rows">${rows}</div>
                <div class="ledger-aside">${asideBlock()}${asideBlock()}</div>
            </div>
        </div>`;
}

// Worker cards in the grid's own columns, same structure as `workerCardEl`.
// Three is enough to read as "cards are coming" without pretending to know
// how big the fleet is.
function workersSkeleton(count) {
    const card = `
        <div class="card worker-card is-skeleton" aria-hidden="true">
            <span class="worker-top">
                <span class="worker-ident">
                    ${skelCell('worker-name', '8rem')}
                    ${skelCell('worker-id', '5.5rem')}
                </span>
                <span class="worker-state">
                    <span class="skel skel-dot"></span>
                    ${skelCell('worker-state-text', '3rem')}
                </span>
            </span>
            <span class="worker-metrics">
                <span class="worker-holds">
                    ${skelCell('worker-holds-value', '2.5rem')}
                    ${skelCell('worker-holds-label', '4rem')}
                </span>
            </span>
            <span class="worker-share-bar skel"></span>
            ${skelCell('worker-note', '11rem')}
            <span class="worker-scopes">
                ${skelCell('worker-scope', '4.5rem')}${skelCell('worker-scope', '3.5rem')}
            </span>
        </div>`;
    return `<div class="workers-skeleton" role="status" aria-label="Loading workers">
        ${card.repeat(count || 3)}</div>`;
}

// Paint both skeletons. Called once, before the first `/data` lands.
function showDashboardSkeleton() {
    const ledger = document.getElementById('ledger');
    if (ledger && !ledger.dataset.painted) ledger.innerHTML = ledgerSkeleton();
    const grid = document.getElementById('workersGrid');
    if (grid && !grid.dataset.painted) grid.innerHTML = workersSkeleton(3);
}

// --- Section headers + the help registry ------------------------------------
// The app explains itself without being text heavy: every section gets a
// title, one line of description, and an (i) button whose long-form copy lives
// here as DATA. Writing help is adding an entry, not writing markup.
//
// `body` is a list of blocks:
//   ['h',  text]                 sub-heading
//   ['p',  text]                 paragraph
//   ['ul', [text, ...]]          bullets
//   ['dl', [[term, def], ...]]   definition rows (the counts table lives here)
//   ['note', text]               operator aside ("if this keeps climbing, ...")
// Every string is escaped on the way out, so copy can contain <, & and quotes.
//
// Semantics come from the counts table in `hub.bi_get_web_info` -- Pending and
// Queued are DIFFERENT hub states and conflating them has caused real bugs.
const SECTION_DOCS = {
    status: {
        title: 'Status',
        // The long copy for this section is the per-metric document in #7 --
        // one place for the lifecycle, so the (i) here and a click on any
        // ledger row open the same thing (anchored, in the row's case).
        modalTitle: 'Job states',
        size: 'lg',
        description: 'Every job the hub knows about, and whether the queue is moving.',
        render: metricDocBody,
    },
    workers: {
        title: 'Workers',
        description: 'Machines connected to the hub right now, and what each is holding.',
        body: [
            ['p', 'A worker is a process running “bue worker” that has an open connection to the hub. It asks for work, runs it, and reports back.'],
            ['h', 'Reading a card'],
            ['dl', [
                ['Live / Idle', 'Live means the worker is holding at least one job right now. Idle means it is connected and asking for work but has none — which is healthy when Pending is 0.'],
                ['Holds', 'How many jobs this worker has checked out. The per-worker holds add up to exactly the On Hold figure in the status ledger.'],
                ['Share', 'This worker’s slice of every job held across the fleet. One card near 100% while the rest sit idle means the work is not spreading — usually a scope mismatch.'],
                ['Scopes', 'The scopes of the jobs it is holding, not the scopes it advertised — the hub is only told a worker’s name, so this is derived from the work in hand. An idle worker therefore shows none.'],
            ]],
            ['p', 'Cards are ordered busiest first, then by name, so they keep their places between refreshes. Click one to see the jobs it is holding.'],
            ['h', 'When holds and jobs disagree'],
            ['p', 'Holds is a count; the job list is detail the hub may not have received yet. A worker that has just taken a hold can read “jobs not reported yet” for a refresh or two. Holds is the number to trust for “is it busy”.'],
            ['h', 'Why a worker disappears'],
            ['p', 'A worker only appears here while it is connected. If one vanishes from the grid, the process exited or lost the network — the hub does not keep a tombstone for it, and everything it held goes back on the dispatch queue.'],
            ['note', 'No workers at all is not a loading state, it is an outage: nothing can start. If Pending is large and every connected worker reads 0 holds, the jobs are matching no worker’s scopes.'],
        ],
    },
    jobs: {
        title: 'Jobs',
        description: 'What this worker has checked out right now \u2014 not what it has run.',
        body: [
            ['p', 'A worker takes a job out of the hub, runs it, and reports back. Everything in this table is in flight on that machine right now. It is not a history: a job leaves the table the moment it finishes, fails, or hands itself back, and there is no record of it here afterwards.'],
            ['h', 'The columns'],
            ['dl', [
                ['Name', 'The step\u2019s name from the pipeline. The \u21a9 badge next to it counts hand-backs \u2014 a job that keeps answering \u201cpending\u201d and going round again.'],
                ['ID', 'The hub\u2019s id for this job. Clipped to fit; the copy button puts the whole thing on the clipboard, which is what you want before grepping a worker log.'],
                ['Type', 'What executes the code \u2014 python, or one of the SQL dialects.'],
                ['Priority', 'Dispatch order, high number first. Any integer works, and the conventional range is 100 down to 0.'],
                ['Scope', 'The pool the job was submitted to. A worker only ever holds jobs from a scope it serves.'],
                ['Timeout', 'How long the worker may spend on it. \u201cnone\u201d means no ceiling, so a wedged job in that row can be held forever.'],
                ['Retries', 'The error budget from !retries: how many further attempts a failure may spend before the job is parked in Errors.'],
                ['Age', 'Time since the job was created, not since it was checked out. A row far older than the rest is the first place to look when the queue stops moving.'],
            ]],
            ['h', 'Working the table'],
            ['ul', [
                'Click a column heading to sort by it; click again to reverse. Sorting is stable, so equal rows keep their order.',
                'The filter box matches a substring of the name or the id, so a pasted id finds its row.',
                'Long lists are paged 100 rows at a time \u2014 a worker holding thousands of jobs still renders instantly.',
                'Click any row to open that job: its code, its parents, and the button that runs it by hand.',
            ]],
            ['note', 'This is a snapshot from the last refresh, and the jobs list is detail the hub may not have received yet. If the count here is short of the worker\u2019s Holds, trust Holds.'],
        ],
    },
    errors: {
        title: 'Errors',
        description: 'Jobs that failed and are parked. They do not retry on their own.',
        body: [
            ['p', 'A job lands here after it raises and runs out of retries. It is parked: the hub will not hand it to a worker again until someone asks it to.'],
            ['h', 'Reading the list'],
            ['ul', [
                'Failures are grouped by the first line of the error message, biggest blast radius first — 47 jobs killed by one bug read as one row with a × 47 badge, not 47 problems.',
                'Open a row to see the full message and traceback.',
                'The job names inside a row are links to that job’s page, where you can inspect its code and run it by hand.',
            ]],
            ['h', 'What Reset errors does'],
            ['p', 'It requeues every failed job for another attempt: the errors are cleared and the jobs go back to Pending with their back-off cleared, so workers can pick them up immediately.'],
            ['note', 'It does not delete anything and it does not fix anything. If the cause is still there, the jobs will fail straight back into this section — and a reset on a large error pile can put real load on whatever they talk to. Read one traceback first.'],
        ],
    },
    // --- Job detail page (#11) ---------------------------------------------
    // Six entries, one per block on page 3. The jargon a newcomer trips over
    // (priority, retries vs attempts, scope, hand-backs, local) is explained
    // once, in `jobConfig`, and the state badge is explained in `job`.
    job: {
        title: 'This job',
        modalTitle: 'Reading a job',
        description: '',
        body: [
            ['p', 'A job is one step of one pipeline: a piece of code, the scope it was submitted to, and its place in a graph of parents and children. Everything on this page is a snapshot from the last refresh.'],
            ['h', 'The state badge'],
            ['dl', [
                ['On hold', 'A worker has this job checked out right now — this is a job that is running. The badge says which worker underneath it.'],
                ['Error', 'It raised and ran out of retries, so the hub parked it. It will not be handed out again until someone resets the errors.'],
                ['Delayed', 'It is pending, but carries a not-before time the hub will not dispatch it ahead of: either a retry back-off, or the poll delay after it handed itself back.'],
                ['State unknown', 'The job was opened without a list to place it in. The hub sends no per-job state field, so the state here is derived from where the job was found — and honesty beats a guess.'],
            ]],
            ['note', 'Pending and Queued are hub-wide states you read in the status ledger, not on a job: a pending or queued job is not checked out by anybody, so there is no page to open it from yet.'],
        ],
    },
    jobFailure: {
        title: 'Failure',
        description: 'Why this job stopped, and the frame it stopped in.',
        body: [
            ['p', 'This job raised and spent its whole retry budget, so the hub parked it rather than handing it out again. Nothing is retrying in the background — the pipeline below it is stuck until this step succeeds.'],
            ['h', 'Reading the traceback'],
            ['ul', [
                'The highlighted tail is the last frame and the exception itself. That is where the failure actually happened; the frames above it are how the code got there.',
                'Frames inside buelon are the harness calling your step. Frames in your own module are the ones to act on.',
                'The copy button takes the whole traceback, message included, so it can go straight into a ticket.',
            ]],
            ['h', 'Getting it running again'],
            ['p', 'Run this job now (at the bottom of the page) re-runs this step alone, on the hub, and streams its output back — the fastest way to check a fix without disturbing the queue. Reset errors, in the Errors section on the dashboard, requeues every parked failure at once.'],
            ['note', 'Attempts against retries tells you whether the failure is consistent. Retries spent on a first attempt means the job has no error budget at all — that is a “!retries 0” step, not a flaky one.'],
        ],
    },
    jobLineage: {
        title: 'Lineage',
        description: 'What this job waits for, and what is waiting on it.',
        body: [
            ['p', 'Pipelines are graphs, not lists. A job runs only once every one of its parents has succeeded, and its own return value is retained until its children have read it.'],
            ['h', 'The chips'],
            ['ul', [
                'A chip with a name is a job in hand — held by a connected worker, or parked in Errors. Click it to open that job.',
                'A greyed chip is an id with no page: the job has already completed and been cleared, or it is pending or queued and no worker has it. The tooltip carries the full id and the copy button puts it on the clipboard.',
                'Parent tree & results walks the whole ancestry and shows what each parent returned — the way to see the data this job is about to be handed.',
            ]],
            ['note', 'A job stuck with parents that never complete is the Queued number in the status ledger. Look upstream at the parents, not at the workers.'],
        ],
    },
    jobCode: {
        title: 'Code',
        description: 'The step body as it was submitted, in the language its type runs.',
        body: [
            ['p', 'This is the code the worker executes for this step, exactly as the pipeline submitted it. Editing it here is not possible — it is a snapshot of what the hub holds.'],
            ['h', 'Type decides what runs it'],
            ['dl', [
                ['python', 'The named function is called in a temporary module, with each parent’s result passed in as an argument.'],
                ['postgres / sqlite3 / other dialects', 'The body is executed against that connection. The colouring is a hint, not a parser — it will not catch a syntax error for you.'],
            ]],
            ['note', 'The highlighting is hand-rolled and deliberately shallow: comments and strings are what matter when you are scanning for the line a traceback named.'],
        ],
    },
    jobConfig: {
        title: 'Configuration',
        description: 'The dispatch settings — rarely what you came for, so they sit down here.',
        body: [
            ['h', 'What each one means'],
            ['dl', [
                ['Function', 'For a python step, the function called inside the code above. Not necessarily the job’s name.'],
                ['Type', 'What executes the code: python, or one of the SQL dialects.'],
                ['Timeout', 'How long a worker may spend on this job before it is treated as failed. “none” means no ceiling at all — a wedged job with no timeout can be held forever.'],
                ['Runs on', 'Local steps run on the hub itself instead of being handed to a worker. Most steps are not local.'],
                ['Attempts', 'How many times this job has already been tried. Attempts against Retries is its remaining error budget.'],
                ['Hand-backs', 'How many times the job has answered “not ready yet” and gone round again. That is the supported way to poll; a count in the thousands is a job polling for something that may never arrive.'],
                ['Created', 'When the job was built, not when a worker took it. A job far older than its neighbours is the first place to look when the queue stops moving.'],
            ]],
            ['h', 'Priority, Retries and Scope'],
            ['p', 'These three are up in the header rather than down here, because they are the ones an operator actually reads. Priority is dispatch order, highest number first, conventionally 100 down to 0. Retries is the error budget from “!retries”: how many further attempts a failure may spend before the job is parked in Errors. Scope is the pool it was submitted to — a job in a scope no connected worker serves waits forever, however many workers you add.'],
        ],
    },
    jobRun: {
        title: 'Run by hand',
        description: 'Re-run this one step on the hub and watch its output.',
        body: [
            ['p', 'This runs the step’s code immediately, on the hub, outside the queue — it does not take the job off any worker and it does not clear an error. Output streams back into the log below as it happens.'],
            ['note', 'It is the real code against the real credentials. A step that writes to a warehouse will write to it again, and a step that costs money will spend it. Read the code above first.'],
        ],
    },
};

function helpBlocks(blocks) {
    return (blocks || []).map(block => {
        const [kind, value] = block;
        if (kind === 'h') return `<h3 class="help-h">${esc(value)}</h3>`;
        if (kind === 'p') return `<p class="help-p">${esc(value)}</p>`;
        if (kind === 'note') return `<p class="help-note">${esc(value)}</p>`;
        if (kind === 'ul') {
            return `<ul class="help-ul">${value.map(item => `<li>${esc(item)}</li>`).join('')}</ul>`;
        }
        if (kind === 'dl') {
            return `<dl class="help-dl">${value.map(([term, def]) => `
                <div class="help-dl-row">
                    <dt>${esc(term)}</dt>
                    <dd>${esc(def)}</dd>
                </div>
            `).join('')}</dl>`;
        }
        return '';
    }).join('');
}

// The (i) button. `section` keys into SECTION_DOCS.
function infoButton(section) {
    const doc = SECTION_DOCS[section];
    if (!doc) return '';
    return `
        <button class="section-info" type="button" data-action="show-help"
                data-help="${attr(section)}"
                aria-label="About the ${attr(doc.title)} section">
            <svg viewBox="0 0 24 24" width="17" height="17" fill="none" stroke="currentColor"
                 stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
                <circle cx="12" cy="12" r="9"/><path d="M12 11v5"/><path d="M12 7.6h.01"/>
            </svg>
        </button>
    `;
}

// The reusable section header: title (with an inline count/status where it
// helps), one line of description, (i) on the right. `extra` is an optional
// control that sits next to the (i) -- the errors section puts its reset
// control there.
function sectionHeader(section, options) {
    const doc = SECTION_DOCS[section] || {};
    const opts = options || {};
    const title = opts.title || doc.title || '';
    const description = opts.description || doc.description || '';
    return `
        <div class="section-heading">
            ${opts.icon || ''}
            <div class="section-heading-text">
                <h2 class="section-title" id="${attr(section)}SectionTitle">
                    ${esc(title)}${opts.meta ? `<span class="section-meta">${esc(opts.meta)}</span>` : ''}
                </h2>
                ${description ? `<p class="section-description">${esc(description)}</p>` : ''}
            </div>
            <div class="section-actions">
                ${opts.extra || ''}
                ${infoButton(section)}
            </div>
        </div>
    `;
}

// `anchorKey` is a metric key (#7): the document opens scrolled to that entry
// with it highlighted, so a row click lands on its own explanation without
// losing the lifecycle around it.
function showHelp(section, anchorKey) {
    // A `data-help` that names no entry used to be a dead button (#16).
    if (!SECTION_DOCS[section]) {
        toast('No help written for that yet', {
            tone: 'info',
            key: 'help-missing',
            detail: 'This is a gap in the dashboard, not in your setup.',
        });
        return;
    }
    const doc = SECTION_DOCS[section];
    const handle = openModal({
        // One modal per document: clicking a second metric row (or the (i)
        // twice) re-uses and re-anchors this one instead of stacking.
        key: `help:${section}`,
        title: doc.modalTitle || doc.title,
        size: doc.size || 'sm',
        body: doc.render
            ? doc.render(anchorKey)
            : `<div class="help-doc">${helpBlocks(doc.body)}</div>`,
    });
    if (handle && anchorKey) anchorHelp(handle, anchorKey);
}

// Scroll the modal body to one entry and put focus on it. The entry carries
// `tabindex="0"` only while it is the active one, so the focus trap keeps
// working and the document does not grow ten extra tab stops.
function anchorHelp(handle, key) {
    const target = handle.el.querySelector(`#metric-${key}`);
    if (!target) return;
    target.focus({ preventScroll: true });
    target.scrollIntoView({
        block: 'center',
        behavior: prefersReducedMotion() ? 'auto' : 'smooth',
    });
}

// Static sections declare `<div class="section-header" data-section="...">` in
// index.html and get filled from the registry once, on load. `setSectionMeta`
// keeps the inline count fresh without re-rendering the header.
function mountSectionHeaders() {
    document.querySelectorAll('.section-header[data-section]').forEach(host => {
        const extra = SECTION_HEADER_EXTRA[host.dataset.section];
        host.innerHTML = sectionHeader(host.dataset.section, extra ? {extra: extra()} : undefined);
    });
}

// The status header carries #20's two affordances in the slot the errors
// section uses for its reset control: the keyboard hint (hidden until there is
// something to browse) and the button that opens the table.
const SECTION_HEADER_EXTRA = {
    status: () => `
        <span class="history-hint" hidden>
            <kbd>←</kbd><kbd>→</kbd> to browse history
        </span>
        <button type="button" class="btn btn-sm btn-ghost history-open"
                data-action="history-table" aria-label="Open the status history table">
            ${HISTORY_ICON}<span>History</span>
        </button>`,
};

// A section's one-line description is a claim about what you are looking at,
// and "right now" stops being true in history mode (#20). Passing null puts
// the registry's copy back.
function setSectionDescription(section, text) {
    const host = document.querySelector(`.section-header[data-section="${section}"]`);
    const el = host && host.querySelector('.section-description');
    if (!el) return;
    const doc = SECTION_DOCS[section] || {};
    el.textContent = text === null || text === undefined ? (doc.description || '') : text;
}

function setSectionMeta(section, text) {
    const host = document.querySelector(`.section-header[data-section="${section}"]`);
    const title = host && host.querySelector('.section-title');
    if (!title) return;
    let meta = title.querySelector('.section-meta');
    if (!text) {
        if (meta) meta.remove();
        return;
    }
    if (!meta) {
        meta = document.createElement('span');
        meta.className = 'section-meta';
        title.appendChild(meta);
    }
    meta.textContent = text;
}

// --- Tooltips ---------------------------------------------------------------
// One floating tooltip, shared by every `[data-tip]` element in the app.
// Mouse hover and keyboard focus both show it; touch deliberately does not --
// a tap opens the full explanation instead (see `show-metric`), which is the
// only honest degradation for a hover affordance on a touchscreen.

let tipEl = null;
let tipAnchor = null;

function ensureTip() {
    if (tipEl) return tipEl;
    tipEl = document.createElement('div');
    tipEl.className = 'tooltip';
    tipEl.id = 'tooltip';
    tipEl.setAttribute('role', 'tooltip');
    tipEl.hidden = true;
    document.body.appendChild(tipEl);
    return tipEl;
}

function showTip(anchor) {
    const text = anchor && anchor.dataset && anchor.dataset.tip;
    if (!text) return;
    if (tipAnchor && tipAnchor !== anchor) hideTip();
    const el = ensureTip();
    el.textContent = text;
    el.hidden = false;
    tipAnchor = anchor;
    anchor.setAttribute('aria-describedby', 'tooltip');
    positionTip(anchor);
}

function hideTip() {
    if (tipAnchor) tipAnchor.removeAttribute('aria-describedby');
    tipAnchor = null;
    if (tipEl) tipEl.hidden = true;
}

// Fixed-position, above the anchor when there is room and below when there is
// not, and always clamped inside the viewport -- at 400px a centered tooltip
// would otherwise hang off the edge.
function positionTip(anchor) {
    const el = tipEl;
    if (!el) return;
    const gap = 8;
    const margin = 8;
    el.style.left = '0px';
    el.style.top = '0px';
    const a = anchor.getBoundingClientRect();
    const t = el.getBoundingClientRect();
    let top = a.top - t.height - gap;
    let placement = 'top';
    if (top < margin) {
        top = a.bottom + gap;
        placement = 'bottom';
    }
    const left = Math.max(margin, Math.min(
        a.left + a.width / 2 - t.width / 2,
        window.innerWidth - t.width - margin,
    ));
    el.dataset.placement = placement;
    el.style.left = `${Math.round(left)}px`;
    el.style.top = `${Math.round(top)}px`;
}

function tipTarget(node) {
    return node && node.closest ? node.closest('[data-tip]') : null;
}

document.addEventListener('pointerover', e => {
    // `pointerType` is '' for synthetic events; only a real mouse hovers.
    if (e.pointerType && e.pointerType !== 'mouse') return;
    const el = tipTarget(e.target);
    if (el) showTip(el);
    else if (tipAnchor && !tipAnchor.contains(e.target)) hideTip();
});

document.addEventListener('pointerdown', () => hideTip());
document.addEventListener('focusin', e => {
    const el = tipTarget(e.target);
    if (el) showTip(el);
    else hideTip();
});
document.addEventListener('focusout', () => hideTip());
// A scroll or resize invalidates the position, and re-measuring on every frame
// is not worth it for a hover hint.
window.addEventListener('scroll', () => hideTip(), true);
window.addEventListener('resize', () => hideTip());

// --- Metric explanations (#7) ----------------------------------------------
// Every ledger row explains itself twice over: a one-line tooltip on hover or
// focus, and a click that opens ONE "Job states" document anchored at that
// metric. One document rather than ten popovers is deliberate -- the confusing
// part is the relationships (Pending vs Queued, what is a subset, what is not
// a job at all), and you only learn those by reading a row's neighbours.
//
// Copy is checked against `hub.py` line by line. In particular: `jobs`
// (Pending) is the dispatchable queue in `STEPS`; `queued` is a job BLOCKED on
// a parent that has not finished, kept deliberately out of `STEPS`. They are
// different hub states and conflating them has caused real bugs (BUGS.md
// #51/#52) -- never merge these two entries.

// group: 'sum' (a component of Total), 'subset' (already inside one of those
// rows), 'other' (not a job at all). `tone` matches the ledger row's dot.
const METRIC_DOCS = [
    {
        key: 'staged',
        label: 'Staged uploads',
        group: 'other',
        tone: 'idle',
        tip: 'Chunks of an upload that has not been committed yet. Nothing in them can run.',
        body: [
            ['p', 'An upload arrives in chunks and the hub only turns them into jobs when the last chunk lands and the upload is committed. Until then they are staged: counted here, and in none of the rows above.'],
            ['p', 'Staged chunks belong to the connection that sent them, and are dropped if it closes. So a staged count means a client that is still connected and has stopped sending.'],
            ['note', 'Staged sitting on a number that never changes is a stalled upload. Those chunks will never run on their own — the fix is to run the upload again.'],
        ],
    },
    {
        key: 'queued',
        label: 'Queued',
        group: 'sum',
        tone: 'info',
        tip: 'Blocked: waiting on a parent job that has not finished. Cannot be dispatched.',
        body: [
            ['p', 'A queued job has everything it needs except its inputs. It is held outside the dispatch queue entirely, and the hub promotes it to Pending only once every one of its parents has succeeded — not when the first one does.'],
            ['p', 'Queued and Pending are different hub states. Reading them as one number hides the thing you actually want to know: whether work is blocked, or merely waiting for a worker.'],
            ['note', 'Queued high while Pending sits near zero and workers are idle means the pipeline is waiting on itself. Look upstream at the parents, not at the workers.'],
        ],
    },
    {
        key: 'jobs',
        label: 'Pending',
        group: 'sum',
        tone: 'idle',
        tip: 'Ready to run, waiting for a free worker. This is the dispatchable backlog.',
        body: [
            ['p', 'Pending jobs are in the dispatch queue: their parents are done, their inputs exist, and the next worker that asks for work with a matching scope can take one.'],
            ['p', 'This is the number that should fall when you add workers. If it does not, the jobs are either delayed (below) or matching no connected worker’s scopes.'],
        ],
    },
    {
        key: 'delayed',
        label: 'of which delayed',
        group: 'subset',
        tone: 'warn',
        tip: 'Pending jobs the hub is holding back for now: retry back-off, or a hand-back delay.',
        body: [
            ['p', 'Already counted in Pending — this is not extra work. A delayed job carries a timestamp the hub will not dispatch it before, for one of two reasons: it failed and is serving out its retry back-off, or it handed itself back and is waiting out the poll delay.'],
            ['note', 'Delayed close to Pending on an idle cluster is normal for a pipeline that polls. Delayed close to Pending with nothing ever completing is a retry loop — read a traceback in Errors.'],
        ],
    },
    {
        key: 'holds',
        label: 'On Hold',
        group: 'sum',
        tone: 'warn',
        tip: 'Checked out by a worker right now. This is what is actually running.',
        body: [
            ['p', 'A hold is a job a worker has taken and not yet reported on. The per-worker Holds figures in the Workers section add up to exactly this number.'],
            ['note', 'If a worker disconnects or the hub restarts, everything it held goes back on the dispatch queue rather than being lost — which means a job can run twice. Jobs should tolerate that.'],
        ],
    },
    {
        key: 'handbacks',
        label: 'of which handed back',
        group: 'subset',
        tone: 'warn',
        tip: 'Live jobs that have already returned “not ready yet” at least once.',
        body: [
            ['p', 'A job can hand itself back instead of finishing — the supported way to poll for something that is not ready. This counts the jobs still in play, pending or on hold, that have done it at least once; the max figure is the worst offender’s count.'],
            ['p', 'They are already inside Pending and On Hold, so they are not added to the total.'],
            ['note', 'There is deliberately no cap — unbounded re-queueing is the point. A max in the thousands is the warning: that job is polling for something that may never arrive. Find the worker holding it.'],
        ],
    },
    {
        key: 'done',
        label: 'Completed',
        group: 'sum',
        tone: 'ok',
        tip: 'Steps that succeeded and whose pipeline is still finishing.',
        body: [
            ['p', 'Completion is counted per step, not per pipeline. When the last step of a pipeline succeeds, the hub clears the whole thing — its jobs and their retained results — so Completed can go down as well as up.'],
            ['note', 'That makes this a live population, not a lifetime tally. A hub keeping up hovers; a Completed count that only climbs means pipelines are finishing steps without ever finishing.'],
        ],
    },
    {
        key: 'errors',
        label: 'Errors',
        group: 'sum',
        tone: 'danger',
        tip: 'Failed and parked. They do not retry on their own.',
        body: [
            ['p', 'A job lands here once it raises and runs out of retries. It stays parked until an operator asks for it: Reset errors requeues every failed job back to Pending with its back-off cleared.'],
            ['p', 'The Errors section appears above the ledger whenever this row is non-zero, grouped by message, so a stampede of one bug reads as one problem rather than fifty.'],
        ],
    },
    {
        key: 'results',
        label: 'Results held',
        group: 'other',
        tone: 'idle',
        tip: 'Intermediate results kept until a pipeline finishes. Data, not jobs.',
        body: [
            ['p', 'Each step’s return value is retained so its children can read it, and dropped along with the jobs when the pipeline it belongs to finishes. The byte figure is an estimate of that retained data, not a measurement.'],
            ['note', 'Results held climbing while Completed is flat means pipelines are starting and not finishing, and the memory is going with them.'],
        ],
    },
    {
        key: 'total',
        label: 'Σ Total',
        group: 'sum-line',
        tone: 'accent',
        tip: 'Pending + Queued + On Hold + Completed + Errors — every job the hub holds.',
        body: [
            ['p', 'A real sum of the five rows above it: every job the hub is holding right now, each counted exactly once. Remaining is this figure minus Completed.'],
            ['p', 'It is a live population, not a lifetime counter — a finished pipeline is cleared out, so the total falls when work completes.'],
        ],
    },
];

const METRIC_BY_KEY = {};
METRIC_DOCS.forEach(metric => { METRIC_BY_KEY[metric.key] = metric; });

// The chip on each entry, so "is this in the total?" is answered in place.
const METRIC_CHIPS = {
    sum: 'in the total',
    'sum-line': 'the sum',
    subset: 'already counted above',
    other: 'not a job',
};

function metricTip(key) {
    const metric = METRIC_BY_KEY[key];
    return metric ? metric.tip : '';
}

// The lifecycle, drawn rather than described: the branches (delayed, handed
// back, errors) are what a paragraph cannot show. Inline SVG, no library, and
// every colour comes from the status ramp via CSS so both themes work.
function lifecycleDiagram() {
    return `
        <figure class="lifecycle">
            <svg viewBox="0 0 700 196" class="lifecycle-svg" role="img"
                 aria-label="A job is staged, then queued while it waits on its parents, then pending, then held by a worker, and ends completed or errored. Held jobs can hand themselves back to pending, and pending jobs can be delayed.">
                <defs>
                    <marker id="lcArrow" viewBox="0 0 10 10" refX="9" refY="5"
                            markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                        <path class="lc-head" d="M0 0 L10 5 L0 10 z"/>
                    </marker>
                </defs>

                <g class="lc-flow">
                    <path d="M118 67 H146"/>
                    <path d="M260 67 H288"/>
                    <path d="M402 67 H430"/>
                    <path d="M544 67 H572"/>
                    <path d="M552 67 V151 H572"/>
                    <path d="M325 86 V126"/>
                    <path d="M357 126 V88"/>
                    <path class="lc-flow-soft" d="M489 48 C489 8 347 8 347 46"/>
                </g>

                <g class="lc-node tone-idle">
                    <rect x="8" y="48" width="110" height="38" rx="8"/>
                    <text x="63" y="72">Staged</text>
                </g>
                <g class="lc-node tone-info">
                    <rect x="150" y="48" width="110" height="38" rx="8"/>
                    <text x="205" y="72">Queued</text>
                </g>
                <g class="lc-node tone-idle">
                    <rect x="292" y="48" width="110" height="38" rx="8"/>
                    <text x="347" y="72">Pending</text>
                </g>
                <g class="lc-node tone-warn">
                    <rect x="434" y="48" width="110" height="38" rx="8"/>
                    <text x="489" y="72">On hold</text>
                </g>
                <g class="lc-node tone-ok">
                    <rect x="576" y="48" width="110" height="38" rx="8"/>
                    <text x="631" y="72">Completed</text>
                </g>
                <g class="lc-node tone-warn is-branch">
                    <rect x="292" y="126" width="110" height="34" rx="8"/>
                    <text x="347" y="148">Delayed</text>
                </g>
                <g class="lc-node tone-danger is-branch">
                    <rect x="576" y="134" width="110" height="34" rx="8"/>
                    <text x="631" y="156">Errors</text>
                </g>

                <text class="lc-edge" x="418" y="42">hands itself back</text>
                <text class="lc-edge" x="274" y="38">parents done</text>
                <text class="lc-edge" x="560" y="118" text-anchor="start">raises</text>
            </svg>
            <figcaption class="lifecycle-caption">
                Uploaded chunks are staged until they are committed. A job then waits on its
                parents (Queued), joins the dispatch queue (Pending), and is checked out by a
                worker (On hold) before it completes or fails. Delayed and handed-back jobs are
                still Pending or On hold — they are branches off the path, not extra states.
            </figcaption>
        </figure>
    `;
}

// The arithmetic, spelled out once so the ledger's `+` / `Σ` / `↳` notation
// needs no explaining anywhere else.
function metricArithmetic() {
    return `
        <div class="metric-math">
            <div class="metric-math-line">
                <span class="metric-math-key">Total</span>
                <span>= Pending + Queued + On&nbsp;Hold + Completed + Errors</span>
            </div>
            <div class="metric-math-line">
                <span class="metric-math-key">Remaining</span>
                <span>= Total − Completed</span>
            </div>
        </div>
    `;
}

function metricEntry(metric, activeKey) {
    const active = metric.key === activeKey;
    const nameId = `metricName-${metric.key}`;
    return `
        <!-- A div with role=group, not a <section> (#17): ten <section
             aria-labelledby> entries put ten nested "region" landmarks inside
             one dialog, which turns the landmark list into noise. -->
        <div class="metric tone-${attr(metric.tone)}${active ? ' is-active' : ''}"
             id="metric-${attr(metric.key)}" role="group" aria-labelledby="${attr(nameId)}"
             ${active ? 'tabindex="0" aria-current="true"' : ''}>
            <div class="metric-head">
                <span class="metric-dot" aria-hidden="true"></span>
                <h4 class="metric-name" id="${attr(nameId)}">${esc(metric.label)}</h4>
                <span class="metric-chip" data-group="${attr(metric.group)}">${esc(METRIC_CHIPS[metric.group] || '')}</span>
                <code class="metric-key">counts.${esc(metric.key)}</code>
            </div>
            <p class="metric-tip">${esc(metric.tip)}</p>
            ${helpBlocks(metric.body)}
        </div>
    `;
}

// The whole document. `SECTION_DOCS.status.render` points here, so the Status
// section's (i) and a click on any ledger row open the same thing.
function metricDocBody(activeKey) {
    return `
        <div class="help-doc metric-doc">
            <p class="help-p">Every job the hub is holding is in exactly one of five states, and the ledger
                lists them in the order a job passes through them. Two more rows are subsets of those
                five, and two count things that are not jobs at all.</p>
            ${lifecycleDiagram()}
            <h3 class="help-h">How the numbers relate</h3>
            ${metricArithmetic()}
            <p class="help-p">Delayed and handed-back jobs are already inside Pending and On&nbsp;Hold, so
                they are never added on. Results held and Staged uploads are not jobs, and sit outside the
                sum entirely.</p>
            <h3 class="help-h">Every row, in order</h3>
            ${METRIC_DOCS.map(metric => metricEntry(metric, activeKey)).join('')}
        </div>
    `;
}

// --- Trend history and sparklines (#18) -------------------------------------
// A count answers "how much". The operator's real question is "is it
// climbing?", and today the only way to answer it is to sit and watch the tab.
//
// This is the *client-side* buffer the plan calls the fallback: one sample per
// successful poll, capped at TREND_MAX, mirrored into sessionStorage so a reload
// does not erase the trend. It is per-tab and it starts empty, which is exactly
// why #19 exists -- server-side history accumulates with nobody watching and is
// the same series for every browser. When #19 lands, `trendSeries()` is the one
// function that has to change.

const TREND_MAX = 60;                 // ~30 minutes at the 30s cadence
const TREND_STORE = 'boo.trend.v1';
const TREND_KEYS = ['total', 'jobs', 'errors'];

// Sparklines are drawn for these ledger rows only. `total` is the headline;
// `jobs` (Pending) and `errors` are the two rows whose direction changes what
// the operator does next.
const SPARK_ROWS = ['jobs', 'errors'];

// [{ t: msEpoch, total, jobs, errors }, ...] oldest first.
let trendSamples = loadTrend();

function loadTrend() {
    try {
        const raw = sessionStorage.getItem(TREND_STORE);
        const parsed = raw ? JSON.parse(raw) : null;
        if (!Array.isArray(parsed)) return [];
        // Anything malformed is dropped rather than repaired: a trend is a
        // convenience, and a half-parsed one is worse than none.
        return parsed
            .filter(s => s && Number.isFinite(Number(s.t)))
            .slice(-TREND_MAX);
    } catch (error) {
        return [];
    }
}

function saveTrend() {
    try {
        sessionStorage.setItem(TREND_STORE, JSON.stringify(trendSamples));
    } catch (error) {
        // Private mode, blocked site data, quota. The in-memory buffer still
        // works; only surviving a reload is lost.
    }
}

// Called once per successful /data, by the caller -- never by renderLedger,
// which stays a pure function of its arguments so #20 can hand it history.
function recordTrend(counts) {
    if (!counts) return;
    const sample = { t: Date.now() };
    TREND_KEYS.forEach(key => { sample[key] = Number(counts[key]) || 0; });
    trendSamples.push(sample);
    if (trendSamples.length > TREND_MAX) {
        trendSamples = trendSamples.slice(-TREND_MAX);
    }
    saveTrend();
}

// The seam #19 promised, now closed by #20: the SERVER series is the truth, and
// the sessionStorage buffer above is only what is left when `/history` is
// unavailable (a hub older than #19, or a broken sampler). The server series
// survives a reload, is the same in every browser, and accumulated while
// nobody was watching -- everything the client buffer cannot do.
//
// `upTo` is an index into `historySamples`: pass it and the window ENDS at that
// sample, which is how time travel draws the trend as it stood then. Passing it
// also disables the client-buffer fallback -- a live squiggle under a two-hour-old
// count is the one thing this feature must never draw.
function serverSeries(key, upTo) {
    if (!historySamples.length) return null;
    const end = upTo === null || upTo === undefined ? historySamples.length - 1 : upTo;
    const rows = historySamples.slice(0, end + 1).filter(s => s && s.counts);
    if (rows.length < 2) return null;
    const use = rows.slice(-TREND_MAX);
    return {
        values: use.map(s => Number(s.counts[key]) || 0),
        spanMs: (Number(use[use.length - 1].ts) - Number(use[0].ts)) * 1000,
    };
}

function trendSeries(key, upTo) {
    const server = serverSeries(key, upTo);
    if (server) return server.values;
    if (upTo !== null && upTo !== undefined) return [];
    return trendSamples.map(s => Number(s[key]) || 0);
}

function trendSpanMs(upTo) {
    const server = serverSeries('total', upTo);
    if (server) return server.spanMs;
    if (upTo !== null && upTo !== undefined) return 0;
    if (trendSamples.length < 2) return 0;
    return trendSamples[trendSamples.length - 1].t - trendSamples[0].t;
}

// --- Drawing ----------------------------------------------------------------
// Inline SVG, no library, currentColor throughout -- so a sparkline inherits
// its row's tone and can never disagree with the dot next to it.

const SPARK_PAD = 2;   // room for the head dot's stroke at both ends

function sparkline(values, options) {
    const opts = options || {};
    const w = opts.w || 44;
    const h = opts.h || 14;
    if (!Array.isArray(values) || values.length < 2) return '';

    // A series that has been zero the whole window has nothing to say, and a
    // red flat line through the Errors row of a healthy hub says the opposite
    // of nothing. No trend is the honest answer.
    if (values.every(v => v === 0)) return '';

    const min = Math.min(...values);
    const max = Math.max(...values);
    const span = max - min;
    const top = SPARK_PAD;
    const bottom = h - SPARK_PAD;
    // A flat series has no shape to show, so it draws down the middle rather
    // than pinned to an arbitrary edge.
    const y = value => span === 0
        ? h / 2
        : top + (bottom - top) * (1 - (value - min) / span);
    const dx = (w - SPARK_PAD * 2) / (values.length - 1);
    const pts = values.map((value, i) => [SPARK_PAD + i * dx, y(value)]);
    const line = pts.map(([px, py], i) =>
        `${i ? 'L' : 'M'}${px.toFixed(2)},${py.toFixed(2)}`).join(' ');
    // The area is what makes a 14px squiggle legible in a table row. It is the
    // wrong call twice over: under a flat line it is a filled rectangle, and at
    // the headline's 96x22 with a baseline to read against it stops looking
    // like a trend and starts looking like a bar. Both cases draw the line
    // alone -- see `opts.area`.
    const area = span === 0 || opts.area === false ? '' : `<path class="spark-area" d="${line} `
        + `L${pts[pts.length - 1][0].toFixed(2)},${h} L${pts[0][0].toFixed(2)},${h} Z"/>`;
    const [hx, hy] = pts[pts.length - 1];

    // A dashed rule at where the window started: it is what turns a squiggle
    // into "higher than it was". Pointless under a flat line, which is already
    // its own baseline, and only the headline spark has the room for it.
    const base = opts.baseline && span !== 0
        ? `<path class="spark-base" d="M0,${y(values[0]).toFixed(2)} L${w},${y(values[0]).toFixed(2)}"/>`
        : '';

    return `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}"
                 aria-hidden="true" focusable="false">
        ${area}
        ${base}
        <path class="spark-line" d="${line}"/>
        <circle class="spark-head" cx="${hx.toFixed(2)}" cy="${hy.toFixed(2)}" r="1.9"/>
    </svg>`;
}

// One sentence, used as both the tooltip and the accessible name. A sparkline
// with no readable equivalent is decoration.
function trendSentence(label, values, spanMs) {
    const first = values[0];
    const last = values[values.length - 1];
    const delta = last - first;
    const over = spanMs ? ` over the last ${shortAge(spanMs)}` : '';
    if (delta === 0) return `${label}: flat at ${num(last)}${over}`;
    const word = delta > 0 ? 'up' : 'down';
    return `${label}: ${word} ${num(Math.abs(delta))}${over}`
        + ` — ${num(first)} to ${num(last)}`;
}

// Fills the empty slots renderLedger left behind. Separate from renderLedger on
// purpose: the ledger renders one sample, this paints a series over it, and #20
// can call it with a historical window instead of the live buffer.
function paintLedgerTrends(container, upTo) {
    if (!container) return;
    const spanMs = trendSpanMs(upTo);

    container.querySelectorAll('[data-spark]').forEach(slot => {
        const key = slot.dataset.spark;
        const values = trendSeries(key, upTo);
        const metric = METRIC_BY_KEY[key];
        const label = (metric && metric.label) || key;
        const head = slot.dataset.sparkSize === 'head';
        // `sparkline` refuses a series with nothing to show (fewer than two
        // samples, or zero throughout), and that refusal is the empty state.
        const svg = sparkline(values, head
            ? { w: 96, h: 22, baseline: true, area: false }
            : { w: 44, h: 14 });

        slot.innerHTML = svg;
        slot.classList.toggle('is-on', !!svg);
        if (!svg) {
            slot.removeAttribute('data-tip');
            slot.removeAttribute('role');
            slot.removeAttribute('aria-label');
            return;
        }
        // A sparkline with no readable equivalent is decoration: the same
        // sentence is the hover tooltip and the accessible name.
        const sentence = trendSentence(label, values, spanMs);
        slot.dataset.tip = sentence;
        slot.setAttribute('role', 'img');
        slot.setAttribute('aria-label', sentence);
    });

    paintHeadlineDelta(container, spanMs, upTo);

    const head = container.querySelector('.ledger-headline-trend');
    if (head) head.classList.toggle('is-on', !!head.querySelector('.ledger-spark.is-on'));
}

// The headline gets the arithmetic spelled out next to its sparkline: a
// direction arrow and the change over the window. Deliberately uncoloured --
// see the note in style.css.
function paintHeadlineDelta(container, spanMs, upTo) {
    const el = container.querySelector('.spark-delta');
    if (!el) return;
    const values = trendSeries('total', upTo);
    if (values.length < 2 || !spanMs) {
        el.textContent = '';
        el.removeAttribute('data-dir');
        return;
    }
    const delta = values[values.length - 1] - values[0];
    const dir = delta > 0 ? 'up' : delta < 0 ? 'down' : 'flat';
    const glyph = { up: '↑', down: '↓', flat: '→' }[dir];
    el.dataset.dir = dir;
    el.textContent = delta === 0
        ? `${glyph} flat · ${shortAge(spanMs)}`
        : `${glyph} ${num(Math.abs(delta))} · ${shortAge(spanMs)}`;
}

// --- Status history: time travel and the table (#20) ------------------------
// #19 records a sample of `counts` on a timer. This is the reader: arrow keys
// walk the ledger backwards through those samples, and a modal shows the whole
// series as a table you can scan for the moment something moved.
//
// The one real failure mode of a feature like this is an operator reading a
// two-hour-old error count as current, so everything here is built against
// that: a loud bar, deltas instead of bare figures, auto-refresh visibly
// stopped, `Esc` and a button back to live, and the two sections whose data is
// NOT in a sample (worker cards, failure detail) replaced by a notice saying so
// rather than left showing live content beside historical counts.
//
// `historyIndex === null` means live. Any number means the ledger on screen is
// `historySamples[historyIndex]` and nothing on the dashboard is polling.

const HISTORY_KEEP = 2000;          // matches #19's MAX_SAMPLES; a client cap too

// [{ts, counts, workers}, ...] oldest first. A FAILED sample is `{ts, error}`
// with no `counts` -- a gap in the record is information, so it is kept and
// rendered, never filtered out.
let historySamples = [];
let historyConfig = null;
let historyNextSampleAt = null;
let historyFailure = '';
let historyIndex = null;
// Set by mergeHistory when the series really changed, so a poll that added
// nothing does not rebuild the open table under the reader.
let historyChanged = false;

function inHistory() {
    return historyIndex !== null;
}

// Fewer than two samples is the empty state: there is nothing to compare.
function historyReady() {
    return historySamples.length >= 2;
}

function historySample(index) {
    return historySamples[index] || null;
}

// --- Fetching and merging ---------------------------------------------------
// Incremental by default: `since` the newest `ts` we hold, which at a 10-minute
// cadence is an empty list on 19 out of 20 polls. A full reload is only needed
// when the interval changes (the series is respaced) or nothing is held yet.
async function loadHistory(options) {
    const full = !!(options && options.full) || !historySamples.length;
    const query = {};
    if (!full) query.since = historySamples[historySamples.length - 1].ts;

    const answer = await getHistory(query);
    if (!answer.ok) {
        historyFailure = answer.error;
        paintHistoryAffordance();
        return false;
    }
    historyFailure = '';
    const data = answer.data || {};
    historyConfig = data.config || historyConfig;
    historyNextSampleAt = Number(data.next_sample_at);
    if (!Number.isFinite(historyNextSampleAt)) historyNextSampleAt = null;
    mergeHistory(Array.isArray(data.samples) ? data.samples : [], full);
    paintHistoryAffordance();
    return true;
}

// Retention can trim the front of the series out from under a cursor, so the
// cursor is re-found by `ts` rather than trusting its index across a merge.
function mergeHistory(rows, replace) {
    const cursor = inHistory() ? historySample(historyIndex) : null;
    const clean = rows.filter(row => row && Number.isFinite(Number(row.ts)));

    if (replace) {
        historySamples = clean;
        historyChanged = true;
    } else if (clean.length) {
        const newest = Number(historySamples[historySamples.length - 1].ts);
        const fresh = clean.filter(row => Number(row.ts) > newest);
        if (fresh.length) {
            historySamples = historySamples.concat(fresh);
            historyChanged = true;
        }
    }
    if (historySamples.length > HISTORY_KEEP) {
        historySamples = historySamples.slice(-HISTORY_KEEP);
    }

    if (!cursor) return;
    const found = historySamples.findIndex(row => row.ts === cursor.ts);
    if (found >= 0) historyIndex = found;
    else if (!historySamples.length) exitHistory();
    else historyIndex = Math.min(historyIndex, historySamples.length - 1);
}

// --- Formatting -------------------------------------------------------------
// `ts` is unix seconds from a server that has no idea what timezone the
// operator is in, so every string here is built in the browser.
function historyClock(ts) {
    const d = new Date(Number(ts) * 1000);
    if (Number.isNaN(d.getTime())) return '—';
    return d.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'});
}

// The same instant with a date on it, for the table and the tooltips: a series
// can span a week, and "14:35" on its own is then a guess.
function historyStamp(ts) {
    const d = new Date(Number(ts) * 1000);
    if (Number.isNaN(d.getTime())) return '—';
    const sameDay = d.toDateString() === new Date().toDateString();
    return sameDay
        ? d.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit', second: '2-digit'})
        : d.toLocaleString([], {month: 'short', day: 'numeric',
                                hour: '2-digit', minute: '2-digit'});
}

// `shortAge` rounds to one unit, which turns "2h 10m" into a flat "2h" -- and
// on a history bar the minutes are the part that says which sample you are on.
function historyAge(ms) {
    const s = Math.max(0, Math.round(ms / 1000));
    if (s < 60) return `${s}s ago`;
    const m = Math.floor(s / 60);
    if (m < 60) return `${m}m ago`;
    const h = Math.floor(m / 60);
    if (h < 24) return (m % 60) ? `${h}h ${m % 60}m ago` : `${h}h ago`;
    const d = Math.floor(h / 24);
    return (h % 24) ? `${d}d ${h % 24}h ago` : `${d}d ago`;
}

function historySampleAge(sample) {
    return historyAge(Date.now() - Number(sample.ts) * 1000);
}

// Which direction is GOOD for a metric: +1 up is good, -1 up is trouble, 0 the
// number is neither (holds climbing means workers are busy). This is the whole
// reason a delta can be coloured at all -- see METRIC_DOCS for the semantics.
const METRIC_GOOD_UP = {
    done: 1, workers: 1,
    errors: -1, jobs: -1, queued: -1, delayed: -1, handbacks: -1, handbacks_max: -1,
    remaining: -1, staged: -1, staged_uploads: -1,
    total: 0, holds: 0, results: 0, results_bytes: 0,
};

// -1 bad, 0 neutral, +1 good, for a change of `delta` in `key`.
function changeTone(key, delta) {
    if (!delta) return 0;
    const good = METRIC_GOOD_UP[key];
    if (!good) return 0;
    return delta > 0 ? good : -good;
}

function signed(delta) {
    return `${delta > 0 ? '+' : '−'}${num(Math.abs(delta))}`;
}

// A sample's value for a column key. `workers` is top-level on the sample, not
// a member of `counts` -- it is not a job count.
function historyValue(sample, key) {
    if (!sample) return null;
    if (key === 'workers') {
        const n = Number(sample.workers);
        return Number.isFinite(n) ? n : null;
    }
    if (!sample.counts) return null;
    const n = Number(sample.counts[key]);
    return Number.isFinite(n) ? n : null;
}

// The reading that comes AFTER a sample is the reference its delta uses.
// Stepping back from live, that is the figure the operator just left -- with
// the older neighbour instead, the first thing they see is a change measured
// against a sample they never looked at, which reads as wrong even when the
// arithmetic is right.
//
// For the newest recorded sample the newer reading is the LIVE one. That is the
// one place this feature touches live data on purpose: it is a labelled
// reference point, never a figure shown as if it were historical.
function liveSampleRef() {
    if (!currentData || !currentData.counts || lastSuccess === null) return null;
    return {ts: lastSuccess / 1000, counts: currentData.counts, live: true};
}

// The sample one step newer than `index`, or the live reading past the end.
function historyNextRef(index) {
    return historySamples[index + 1] || liveSampleRef();
}

// "by 09:43 AM" / "by the live reading" -- what a delta is measured to.
function refClock(ref) {
    if (!ref) return '';
    return ref.live ? 'the live reading' : historyClock(ref.ts);
}

// --- Moving through the series ----------------------------------------------
function gotoHistory(index) {
    if (index === null || index === undefined) return exitHistory();
    if (!historySamples.length) return;
    const clamped = Math.max(0, Math.min(historySamples.length - 1, index));
    const first = !inHistory();
    historyIndex = clamped;
    if (first) {
        document.body.classList.add('is-history');
        // The clock keeps ticking, the poll does not. Anything that fires
        // while browsing would repaint the ledger with live numbers under a
        // banner claiming it is 14:35.
        paintLiveState();
    }
    renderHistoryView();
}

// `delta` in samples: -1 is one step back in time. From live, back means the
// newest recorded sample; forward from the newest means live.
function stepHistory(delta) {
    if (!historySamples.length) return;
    if (!inHistory()) {
        if (delta < 0) gotoHistory(historySamples.length - 1);
        return;
    }
    const next = historyIndex + delta;
    if (next > historySamples.length - 1) return exitHistory();
    if (next < 0) {
        announce('Oldest sample in the record');
        return;
    }
    gotoHistory(next);
}

function exitHistory() {
    if (!inHistory()) return;
    historyIndex = null;
    document.body.classList.remove('is-history');
    const bar = document.getElementById('historyBar');
    if (bar) bar.remove();
    // The worker grid was replaced wholesale by the live-only notice, so the
    // card cache no longer matches the DOM. Clearing both lets renderWorkers
    // rebuild from scratch (and re-run its first-fill cascade).
    workerCards.clear();
    const grid = document.getElementById('workersGrid');
    if (grid) grid.textContent = '';
    renderLive({flash: false});
    // "Back to live" has to mean live. Browsing for ten minutes froze the
    // poll, so what renderLive just painted can be a whole cadence stale --
    // same reasoning as walking back onto the dashboard in showPage().
    scheduleNextPoll();
    if (autoRefresh && lastSuccess !== null && Date.now() - lastSuccess >= REFRESH_MS) {
        nextPollAt = Date.now();
    }
    paintLiveState();
    announce('Back to live');
}

// --- The history bar --------------------------------------------------------
// Unmistakably not live: it sits above the ledger, carries the tone the rest of
// the app never uses, and holds the only three things you want from here --
// step, open the table, go back to live.
const HISTORY_ICON = `
    <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor"
         stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
        <path d="M3.2 9.5A9 9 0 1 1 3 12"/><path d="M3 4.5V10h5.2"/>
        <path d="M12 7.6V12l3.2 2"/>
    </svg>`;

function historyBarEl() {
    let bar = document.getElementById('historyBar');
    if (bar) return bar;
    const section = document.getElementById('statusSection');
    const ledger = document.getElementById('ledger');
    if (!section || !ledger) return null;
    bar = document.createElement('div');
    bar.id = 'historyBar';
    bar.className = 'history-bar';
    // role=status, not alert: it announces itself when it appears and when the
    // sample changes, without interrupting.
    bar.setAttribute('role', 'status');
    bar.innerHTML = `
        <span class="history-bar-mark" aria-hidden="true">${HISTORY_ICON}</span>
        <div class="history-bar-text">
            <p class="history-bar-head" id="historyBarHead"></p>
            <p class="history-bar-meta" id="historyBarMeta"></p>
        </div>
        <div class="history-bar-nav">
            <button type="button" class="btn btn-sm btn-secondary history-step"
                    data-action="history-step" data-step="-1"
                    aria-label="Older sample (left arrow)" title="Older sample (←)">‹</button>
            <button type="button" class="btn btn-sm btn-secondary history-step"
                    data-action="history-step" data-step="1"
                    aria-label="Newer sample (right arrow)" title="Newer sample (→)">›</button>
            <button type="button" class="btn btn-sm btn-secondary"
                    data-action="history-table">All samples</button>
            <button type="button" class="btn btn-sm btn-primary"
                    data-action="history-live">Back to live</button>
        </div>`;
    section.insertBefore(bar, ledger);
    return bar;
}

// Called on every sample change and, for the age, from the 250ms clock.
function paintHistoryClock() {
    if (!inHistory()) return;
    const sample = historySample(historyIndex);
    const head = document.getElementById('historyBarHead');
    const meta = document.getElementById('historyBarMeta');
    if (!sample || !head || !meta) return;

    const gap = !sample.counts;
    const headText = gap
        ? `No sample at ${historyClock(sample.ts)}`
        : `Viewing ${historyClock(sample.ts)}`;
    const text = `${headText} — ${historySampleAge(sample)}`;
    if (head.textContent !== text) head.textContent = text;

    const parts = [
        `sample ${num(historyIndex + 1)} of ${num(historySamples.length)}`,
        'auto-refresh paused',
    ];
    if (gap) parts.unshift('the server was unavailable');
    // Name the references the deltas are measured to. Without it the only way
    // to learn what a "-552" is against was to hover it -- and with two chips
    // per row, which is which has to be readable without hovering either.
    const nextSample = historySamples[historyIndex + 1] || null;
    const live = liveSampleRef();
    const refs = [];
    if (nextSample && nextSample.counts) refs.push(`next (${historyClock(nextSample.ts)})`);
    if (live && live.counts) refs.push('live');
    if (!gap && refs.length) parts.splice(1, 0, `changes to ${refs.join(' and ')}`);
    const metaText = parts.join('  ·  ');
    if (meta.textContent !== metaText) meta.textContent = metaText;

    const older = document.querySelector('.history-step[data-step="-1"]');
    if (older) older.disabled = historyIndex === 0;
}

// The quiet discovery affordance: it appears only once there is something to
// browse, and says why when there is not.
function paintHistoryAffordance() {
    const hint = document.querySelector('.history-hint');
    if (hint) hint.hidden = !historyReady();
    const open = document.querySelector('.history-open');
    if (open) {
        open.classList.toggle('is-warn', !!historyFailure);
        open.setAttribute('aria-label', historyFailure
            ? 'History unavailable — open for details'
            : 'Open the status history table');
    }
}

// --- Rendering a sample -----------------------------------------------------
function renderHistoryView() {
    const sample = historySample(historyIndex);
    if (!sample) return exitHistory();
    historyBarEl();
    paintHistoryClock();

    const container = document.getElementById('ledger');
    // Kept apart: `next` is the newer *recorded* sample (null at the end of the
    // series), `live` is the reading now. At the end of the series they are the
    // same thing, and paintLedgerDeltas renders one chip rather than two.
    const next = historySamples[historyIndex + 1] || null;
    const live = liveSampleRef();

    if (!sample.counts) {
        // A failed sample. There are no numbers to show and inventing a zero
        // for each of them would be a lie, so the ledger says what happened.
        ledgerPrev = null;
        if (container) {
            container.innerHTML = blankState({
                kind: 'failed',
                title: `No sample recorded at ${historyClock(sample.ts)}`,
                body: 'The sampler ran and could not reach the hub, so this point in the'
                    + ' record is a gap rather than a set of counts. The web app was up;'
                    + ' the hub behind it was not answering.',
                detail: sample.error ? String(sample.error) : '',
                actions: [
                    {label: '‹ Older sample', name: 'history-step', data: {step: -1}},
                    {label: 'Newer sample ›', name: 'history-step', data: {step: 1}},
                    {label: 'Back to live', name: 'history-live', primary: true},
                ],
                frame: true,
            });
        }
    } else {
        renderLedger(sample.counts, {flash: false});
        // The trend as it stood THEN: the window ends at this sample.
        paintLedgerTrends(container, historyIndex);
        paintLedgerDeltas(container, sample, next, live);
    }

    renderHistoryErrors(sample);
    renderHistoryWorkers(sample);
    announce(sample.counts
        ? `Viewing the sample from ${historyClock(sample.ts)}, ${historySampleAge(sample)}.`
            + ` Total ${num(historyValue(sample, 'total'))},`
            + ` errors ${num(historyValue(sample, 'errors'))}.`
        : `No sample at ${historyClock(sample.ts)}: the server was unavailable.`);
}

// One chip: a signed number, coloured by what that direction MEANS for the
// metric, with the sentence version as both tooltip and accessible name.
function deltaChip(key, tag, from, to, refText) {
    const delta = to - from;
    if (!delta) return '';
    const tone = changeTone(key, delta);
    const cls = tone > 0 ? 'is-good' : tone < 0 ? 'is-bad' : 'is-flat';
    const label = (METRIC_BY_KEY[key] && METRIC_BY_KEY[key].label) || key;
    const sentence = `${label} ${delta > 0 ? 'up' : 'down'} ${num(Math.abs(delta))}`
        + ` to ${num(to)} ${refText}`;
    return `<span class="ledger-delta-chip ${cls}" role="img"
                  aria-label="${attr(sentence)}" data-tip="${attr(sentence)}"
        ><span class="ledger-delta-tag" aria-hidden="true">${esc(tag)}</span
        >${esc(signed(delta))}</span>`;
}

// The delta is the insight; the absolute number is context. Written into the
// slot `ledgerRow` always leaves, so nothing shifts when it arrives.
//
// TWO references, because they answer different questions and an operator
// browsing the record asks both: `next` is the sample one step newer -- what
// happened right after this point -- and `live` is the reading now, which is
// how this sample compares with what the dashboard shows when you leave
// history. Both are `reference - sample`, signed so up is still up; measuring
// the other way round would invert every changeTone() verdict, painting
// falling errors as trouble.
//
// Viewing the newest sample, `next` IS live, so only the live chip renders --
// two identical numbers side by side would be noise.
function paintLedgerDeltas(container, sample, next, live) {
    if (!container) return;
    const cur = sample && sample.counts;
    const after = next && next.counts;
    const now = live && live.counts;
    container.querySelectorAll('[data-delta-key]').forEach(slot => {
        const key = slot.dataset.deltaKey;
        slot.textContent = '';
        slot.className = 'ledger-delta';
        slot.removeAttribute('data-tip');
        slot.removeAttribute('aria-label');
        slot.removeAttribute('role');
        if (!cur) return;

        if (!after && !now) {
            // Nothing newer to compare against at all. Say which -- a blank
            // cell looks like "no change".
            slot.classList.add('is-none');
            slot.textContent = next ? 'before gap' : 'latest';
            slot.dataset.tip = next
                ? 'The next sample is a gap, and there is no live reading to compare'
                    + ' against either.'
                : 'The newest reading in the record: nothing after it to compare against yet.';
            return;
        }

        const value = Number(cur[key]) || 0;
        const chips = [];
        if (after) {
            chips.push(deltaChip(key, 'next', value, Number(after[key]) || 0,
                `by ${refClock(next)}, the next sample`));
        }
        if (now) {
            chips.push(deltaChip(key, 'live', value, Number(now[key]) || 0,
                'at the live reading now'));
        }
        const html = chips.filter(Boolean).join('');
        if (html) slot.innerHTML = html;
    });
}

// Samples hold counts, not job lists. So the failure list and the worker cards
// are LIVE ONLY, and they say so -- showing live detail under a historical
// count is the exact confusion this feature has to avoid.
function renderHistoryErrors(sample) {
    const section = document.getElementById('errorsSection');
    if (!section) return;
    const count = Number(historyValue(sample, 'errors')) || 0;
    expandedErrors = new Set();
    errorsShowAll = false;
    if (!sample.counts || !count) {
        section.hidden = true;
        section.innerHTML = '';
        return;
    }
    section.hidden = false;
    section.innerHTML = `
        ${sectionHeader('errors', {
            title: `${num(count)} failed job${count === 1 ? '' : 's'}`,
            meta: `at ${historyClock(sample.ts)}`,
            icon: ERROR_SECTION_ICON,
        })}
        ${blankState({
            kind: 'empty',
            icon: 'warn',
            tone: 'warn',
            title: 'Failure detail is live only',
            body: 'History records how many jobs had failed at this moment, not their'
                + ' messages or tracebacks. Those come from the hub on request, and the'
                + ' hub only has the ones that are still there now.',
        })}
    `;
}

function renderHistoryWorkers(sample) {
    const grid = document.getElementById('workersGrid');
    if (!grid) return;
    if (tipAnchor && grid.contains(tipAnchor)) hideTip();
    workerCards.clear();
    const n = historyValue(sample, 'workers');
    const known = n !== null;
    setSectionMeta('workers', known ? `${num(n)} at ${historyClock(sample.ts)}` : 'not recorded');
    setSectionDescription('workers',
        `How many machines were connected when this sample was recorded, at ${historyClock(sample.ts)}.`);
    grid.innerHTML = blankState({
        kind: known && n === 0 ? 'failed' : 'empty',
        icon: known && n === 0 ? 'warn' : 'empty',
        tone: known && n === 0 ? 'warn' : 'idle',
        title: !known
            ? 'Worker count not recorded for this sample'
            : n === 0
                ? `No workers were connected at ${historyClock(sample.ts)}`
                : `${num(n)} worker${n === 1 ? '' : 's'} connected at ${historyClock(sample.ts)}`,
        body: known && n === 0
            ? 'Nothing could be dispatched at this point in the record, whatever was pending.'
                + ' The fleet is only a count in history — which machines they were is not kept.'
            : 'History keeps how many workers were connected, not which ones or what each was'
                + ' holding. The card grid is live only.',
        actions: [{label: 'Back to live', name: 'history-live', primary: true}],
        frame: true,
    });
}

// --- The table --------------------------------------------------------------
// The index to the ledger's reader: one row per sample, newest first, and a
// per-cell tint that lets you SEE where something moved before reading a
// figure. Clicking a row hands that sample to the ledger and closes.
const HISTORY_COLUMNS = [
    {key: 'total', label: 'Total'},
    {key: 'remaining', label: 'Remaining'},
    {key: 'jobs', label: 'Pending'},
    {key: 'queued', label: 'Queued'},
    {key: 'holds', label: 'Holds'},
    {key: 'done', label: 'Done'},
    {key: 'errors', label: 'Errors'},
    {key: 'delayed', label: 'Delayed'},
    {key: 'workers', label: 'Workers'},
];

// Tint intensity is relative to the biggest move that column makes anywhere in
// the series, so a table of small numbers reads like a table of large ones.
//
// Two deliberate suppressions, both learned from tinting everything first: the
// exponent bends small changes DOWN rather than up, and anything under
// TINT_FLOOR of the column's biggest move gets no tint at all. Real data
// jitters by a few counts on every sample; a table where every cell is tinted
// shows you exactly as much as a table where none is.
const TINT_FLOOR = 0.12;
const TINT_CURVE = 1.7;

function historyTint(delta, max) {
    if (!delta || !max) return 0;
    const share = Math.min(1, Math.abs(delta) / max);
    if (share < TINT_FLOOR) return 0;
    return Math.pow(share, TINT_CURVE);
}

function historyMaxChange() {
    const max = {};
    HISTORY_COLUMNS.forEach(col => { max[col.key] = 0; });
    // Adjacent pairs, plus the newest-to-live pair, because that pair is now
    // shown as a delta too and a tint scale has to know about it.
    const series = historySamples.concat(liveSampleRef() ? [liveSampleRef()] : []);
    for (let i = 1; i < series.length; i++) {
        const cur = series[i];
        const prev = series[i - 1];
        if (!cur.counts || !prev.counts) continue;
        HISTORY_COLUMNS.forEach(col => {
            const a = historyValue(cur, col.key);
            const b = historyValue(prev, col.key);
            if (a === null || b === null) return;
            max[col.key] = Math.max(max[col.key], Math.abs(a - b));
        });
    }
    return max;
}

// `next` is the reading one step newer -- the row ABOVE this one, since the
// table is newest-first -- so a cell's delta is what changed after it, the same
// convention the ledger's time travel uses.
function historyCell(sample, next, col, max) {
    const value = historyValue(sample, col.key);
    if (value === null) {
        return `<td class="hist-cell is-blank">—</td>`;
    }
    const after = next ? historyValue(next, col.key) : null;
    const delta = after === null ? 0 : after - value;
    const tone = changeTone(col.key, delta);
    // Only columns where a direction MEANS something get a tint. Total and
    // Holds moving is not news -- a grey wash down those columns reads as
    // "disabled" and buys nothing. They still carry the figure and the delta.
    const tint = tone ? historyTint(delta, max[col.key]) : 0;
    const cls = ['hist-cell'];
    if (delta) cls.push(tone > 0 ? 'is-good' : tone < 0 ? 'is-bad' : 'is-flat');
    const tip = delta
        ? ` data-tip="${attr(`${col.label} ${delta > 0 ? 'up' : 'down'} `
            + `${num(Math.abs(delta))} to ${num(after)} by ${refClock(next)}`
            + ` — sampled ${historyStamp(sample.ts)}`)}"`
        : '';
    return `<td class="${cls.join(' ')}" style="--tint:${tint.toFixed(3)}"${tip}>
        <span class="hist-value">${num(value)}</span>
        ${delta ? `<span class="hist-delta">${esc(signed(delta))}</span>` : ''}
    </td>`;
}

function historyRow(index, max) {
    const sample = historySamples[index];
    const next = historyNextRef(index);
    const current = inHistory() && historyIndex === index;
    // The row is clickable for the mouse; the time cell is a real button so it
    // is one tab stop with a real accessible name, rather than a div pretending.
    const head = `
        <th scope="row" class="hist-time">
            <button type="button" class="hist-jump" data-action="history-goto"
                    data-index="${attr(index)}"
                    aria-label="Show the sample from ${attr(historyStamp(sample.ts))} on the dashboard">
                <span class="hist-clock">${esc(historyClock(sample.ts))}</span>
                <span class="hist-rel">${esc(historySampleAge(sample))}</span>
            </button>
        </th>`;

    if (!sample.counts) {
        return `<tr class="hist-row is-gap${current ? ' is-current' : ''}"
                    data-action="history-goto" data-index="${attr(index)}">
            ${head}
            <td class="hist-gap" colspan="${HISTORY_COLUMNS.length}"
                ${sample.error ? `data-tip="${attr(sample.error)}"` : ''}>
                — server unavailable —${sample.error ? `<span class="hist-gap-why">${esc(sample.error)}</span>` : ''}
            </td>
        </tr>`;
    }

    return `<tr class="hist-row${current ? ' is-current' : ''}"
                data-action="history-goto" data-index="${attr(index)}">
        ${head}
        ${HISTORY_COLUMNS.map(col => historyCell(sample, next, col, max)).join('')}
    </tr>`;
}

function historyTable() {
    const max = historyMaxChange();
    const rows = [];
    for (let i = historySamples.length - 1; i >= 0; i--) rows.push(historyRow(i, max));
    return `
        <!-- tabindex=0 for the same two reasons as the jobs table (#9/#17): the
             one horizontally-scrolling box on screen can be panned with the
             arrow keys, and it is the first focusable thing in the dialog --
             which is what keeps openModal's focusInside() off the interval
             select, where a stray arrow key would change the server's
             sampling cadence. -->
        <div class="hist-table-wrap" tabindex="0" role="group"
             aria-label="Recorded status samples, newest first">
            <table class="hist-table">
                <caption class="sr-only">Recorded status samples, newest first.
                    Each cell carries what changed after it was recorded, measured to
                    the row above -- the newest row to the live reading -- and is tinted
                    by the size of that change.</caption>
                <thead>
                    <tr>
                        <th scope="col">Time</th>
                        ${HISTORY_COLUMNS.map(col =>
                            `<th scope="col">${esc(col.label)}</th>`).join('')}
                    </tr>
                </thead>
                <tbody>${rows.join('')}</tbody>
            </table>
        </div>`;
}

function historyIntervalControl() {
    const config = historyConfig || {};
    const allowed = Array.isArray(config.allowed_intervals) && config.allowed_intervals.length
        ? config.allowed_intervals
        : [1, 5, 10, 15, 30, 60];
    const current = Number(config.interval_minutes) || 10;
    const options = allowed.map(m =>
        `<option value="${attr(m)}"${m === current ? ' selected' : ''}>${esc(
            m === 60 ? 'hour' : `${m} minutes`)}</option>`).join('');
    // A fixed set, not a text box: there is no good reason to sample every 7
    // seconds and a hand-typed 0 must not be reachable (#19).
    return `
        <label class="hist-interval">
            <span class="hist-interval-label">Sample every</span>
            <select class="hist-interval-select" id="historyInterval">${options}</select>
        </label>`;
}

function historyRetentionLine() {
    const r = (historyConfig && historyConfig.retention) || {};
    const kept = Number(r.samples);
    const parts = [];
    if (Number.isFinite(kept)) parts.push(`${num(kept)} sample${kept === 1 ? '' : 's'} kept`);
    if (Number.isFinite(Number(r.max_samples)) && Number.isFinite(Number(r.max_age_days))) {
        parts.push(`trimmed at ${num(r.max_samples)} samples or ${num(r.max_age_days)} days,`
            + ' whichever comes first');
    }
    if (historyConfig && historyConfig.explicit === false) {
        parts.push('the interval is the server default');
    }
    return parts.join(' · ');
}

// "in about 4 minutes" for the empty state's countdown, repainted by the clock.
function historyNextIn() {
    if (!historyNextSampleAt) return '';
    const ms = historyNextSampleAt * 1000 - Date.now();
    if (ms <= 0) return 'due now';
    return `in ${historyAge(ms).replace(' ago', '')}`;
}

function paintHistoryCountdown() {
    const el = document.getElementById('historyNextIn');
    if (!el) return;
    const text = historyNextIn();
    if (el.textContent !== text) el.textContent = text;
}

function historyModalBody() {
    if (historyFailure) {
        return blankState({
            kind: 'failed',
            title: 'Could not read the status history',
            body: 'The dashboard is up and the counts above are current. The recorded'
                + ' series is served by a separate endpoint, and that request failed —'
                + ' so there is no history to show, not an empty one.',
            detail: historyFailure,
            actions: [{label: 'Try again', name: 'history-reload', primary: true}],
        });
    }

    // The controls sit BELOW the table, not above it: the series is what you
    // opened this for, and a `<select>` above it would be the first focusable
    // element in the dialog (see the note on the table wrap).
    const toolbar = `
        <div class="hist-toolbar">
            <div class="hist-toolbar-config">
                ${historyIntervalControl()}
                <p class="hist-retention">${esc(historyRetentionLine())}</p>
            </div>
            <div class="hist-toolbar-actions">
                <button type="button" class="btn btn-sm btn-secondary"
                        data-action="history-copy-csv">Copy CSV</button>
                <button type="button" class="btn btn-sm btn-secondary"
                        data-action="history-download-csv">Download .csv</button>
            </div>
        </div>`;

    if (!historyReady()) {
        const only = historySamples.length === 1 ? historySamples[0] : null;
        return blankState({
            kind: 'empty',
            title: historySamples.length
                ? 'One sample so far — nothing to compare it to'
                : 'No samples recorded yet',
            body: historySamples.length
                ? 'The server started recording at ' + historyStamp(only.ts) + '. A history'
                  + ' needs two points; the next one is what makes this table worth opening.'
                : 'The sampler records the status counts on a timer and keeps them across'
                  + ' restarts. Nothing has been recorded yet — leave the server running.',
            hint: historyNextSampleAt
                ? `Next sample <strong id="historyNextIn">${esc(historyNextIn())}</strong>.`
                : '',
            // Worth having on its own (the countdown is an estimate), and it
            // is also what keeps focus off the interval select in the one
            // state that has no table above it.
            actions: [{label: 'Check for a new sample', name: 'history-reload', primary: true}],
        }) + toolbar;
    }

    return `
        <p class="hist-lede">One row per recorded sample, newest first. A cell is tinted
            by how far that figure moved from the row below it — green where the direction
            is good news, red where it is not. Click a row to load that moment into the
            dashboard<span class="hist-lede-keys">, or use <kbd>←</kbd> <kbd>→</kbd> there</span>.</p>
        ${historyTable()}
        ${toolbar}`;
}

// --- CSV --------------------------------------------------------------------
// Someone will want this in a spreadsheet the first week it exists. Oldest
// first, one column per metric, and a trailing `error` column so the gaps
// survive the export instead of vanishing into blank rows.
function historyCsvCell(value) {
    const text = value === null || value === undefined ? '' : String(value);
    return /[",\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function historyCsv() {
    const head = ['time', 'ts', ...HISTORY_COLUMNS.map(col => col.label.toLowerCase()), 'error'];
    const lines = [head.join(',')];
    historySamples.forEach(sample => {
        const iso = new Date(Number(sample.ts) * 1000).toISOString();
        const cells = [iso, sample.ts];
        HISTORY_COLUMNS.forEach(col => {
            const value = historyValue(sample, col.key);
            cells.push(value === null ? '' : value);
        });
        cells.push(sample.counts ? '' : (sample.error || 'no sample'));
        lines.push(cells.map(historyCsvCell).join(','));
    });
    return lines.join('\n') + '\n';
}

async function copyHistoryCsv(button) {
    const ok = await writeClipboard(historyCsv());
    flashCopy(button, ok, 'history CSV');
    if (ok) toast(`Copied ${num(historySamples.length)} samples as CSV`, {tone: 'ok', key: 'hist-csv'});
}

function downloadHistoryCsv() {
    // The dashboard is served from a local process, so a blob download is the
    // simplest honest export. Revoked on the next tick: keeping the URL alive
    // pins the whole string in memory for the life of the tab.
    try {
        const blob = new Blob([historyCsv()], {type: 'text/csv'});
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `buelon-history-${new Date().toISOString().slice(0, 19).replace(/[:T]/g, '')}.csv`;
        document.body.appendChild(a);
        a.click();
        a.remove();
        setTimeout(() => URL.revokeObjectURL(url), 1000);
        announce('History CSV downloaded');
    } catch (error) {
        toast('Could not build the CSV', {
            tone: 'warn',
            key: 'hist-csv',
            detail: String(error && error.message ? error.message : error),
        });
    }
}

// --- Opening it -------------------------------------------------------------
function showHistoryTable() {
    const handle = openModal({
        key: 'history',
        title: 'Status history',
        size: 'lg',
        body: historyModalBody(),
    });
    if (handle) bindHistoryModal(handle);
    return handle;
}

// The interval `<select>` fires `change`, which the delegated click handler
// cannot see. Bound directly, and re-bound every time the body is replaced.
function bindHistoryModal(handle) {
    const select = handle.el.querySelector('#historyInterval');
    if (!select) return;
    select.addEventListener('change', async () => {
        const minutes = Number(select.value);
        const was = Number(historyConfig && historyConfig.interval_minutes) || 10;
        select.disabled = true;
        const answer = await setHistoryInterval(minutes);
        select.disabled = false;
        if (!answer.ok) {
            select.value = String(was);
            toast('The hub refused that interval', {
                tone: 'warn',
                key: 'hist-interval',
                detail: answer.error,
            });
            return;
        }
        historyConfig = answer.config || historyConfig;
        // The cadence changed, so the spacing of everything already recorded
        // is now mixed. Reload the whole series rather than stitching.
        await loadHistory({full: true});
        if (inHistory()) renderHistoryView();
        refreshHistoryModal();
        toast(`Sampling every ${minutes === 60 ? 'hour' : `${minutes} minutes`}`, {
            tone: 'ok',
            key: 'hist-interval',
            detail: 'Existing history is kept; the change takes effect on the next tick.',
        });
    });
}

function refreshHistoryModal() {
    const open = modalStack.find(m => m.key === 'history');
    if (!open) return;
    // Rebuilt from a string, so both scrollers reset to the top. A table you
    // had scrolled 40 rows into is not worth "refreshing" if it jumps.
    const wrap = open.el.querySelector('.hist-table-wrap');
    const wrapTop = wrap ? wrap.scrollTop : 0;
    const bodyTop = open.bodyEl.scrollTop;
    open.setBody(historyModalBody());
    bindHistoryModal(open);
    const next = open.el.querySelector('.hist-table-wrap');
    if (next) next.scrollTop = wrapTop;
    open.bodyEl.scrollTop = bodyTop;
}

// --- Keyboard ---------------------------------------------------------------
// A global arrow binding is a liability: it must not fire in the #9 filter box,
// inside a modal, or on a page that has no ledger to move. Every one of those
// is checked BEFORE the key is claimed, so the default action survives.
function historyKeysAllowed(event) {
    if (event.metaKey || event.ctrlKey || event.altKey || event.shiftKey) return false;
    if (modalStack.length) return false;
    if (!document.querySelector('#page1.active')) return false;
    const el = document.activeElement;
    if (!el) return true;
    if (el.isContentEditable) return false;
    return !['INPUT', 'TEXTAREA', 'SELECT', 'OPTION'].includes(el.tagName);
}

function onHistoryKey(event) {
    const key = event.key;
    if (key === 'ArrowLeft' || key === 'ArrowRight' || key === 'Home' || key === 'End') {
        if (!historyReady() || !historyKeysAllowed(event)) return;
        event.preventDefault();
        if (key === 'ArrowLeft') stepHistory(-1);
        else if (key === 'ArrowRight') stepHistory(1);
        else if (key === 'Home') gotoHistory(0);
        // End is the newest RECORDED sample, which on a real server is up to
        // one interval old. Live is a different thing, and Esc is how you
        // get there.
        else gotoHistory(historySamples.length - 1);
    }
}

// --- Status ledger ----------------------------------------------------------
// Replaces the old 12-card stats grid. The information architecture is the
// counts table in `hub.bi_get_web_info`: five keys that really are a sum, two
// that are subsets of it, and two that are not jobs at all. The ledger makes
// that arithmetic visible instead of flattening it into peer tiles.
//
// `renderLedger(counts)` is a pure function of a counts object -- it never
// reads `currentData` -- so a historical sample renders exactly as happily as
// the live one (#20 time-travel depends on this).

// Handed-back jobs are normal in small numbers; a job polling forever is not.
const HANDBACK_ALARM = 50;

// The five components of `total`, in pipeline order: waiting -> running -> done.
// `tone` picks the status-ramp color for the row dot and the matching bar
// segment, so the bar and the list read as the same five things.
const LEDGER_SUM_ROWS = [
    { key: 'jobs', label: 'Pending', tone: 'idle' },
    { key: 'queued', label: 'Queued', tone: 'info' },
    { key: 'holds', label: 'On Hold', tone: 'warn' },
    { key: 'done', label: 'Completed', tone: 'ok' },
    { key: 'errors', label: 'Errors', tone: 'danger' },
];

// Last values we painted, keyed the same way as the DOM's `data-num-key`.
// Used to tween numbers between refreshes and flash the rows that moved.
let ledgerPrev = null;

function prefersReducedMotion() {
    return window.matchMedia
        && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
}

function num(value) {
    return (Number(value) || 0).toLocaleString();
}

// One ledger line. `prefix` carries the sum notation ('+', 'Σ', '↳').
function ledgerRow(row) {
    const value = Number(row.value) || 0;
    const classes = [
        'ledger-row',
        `tone-${row.tone || 'idle'}`,
        value === 0 ? 'is-zero' : '',
        row.alarm && value > 0 ? 'is-alarm' : '',
        row.total ? 'is-total' : '',
    ].filter(Boolean).join(' ');

    // Every row is a hover/focus tooltip and a click into the "Job states"
    // document, anchored at this metric (#7). A row with no doc entry stays
    // inert rather than opening an empty modal.
    const tip = metricTip(row.key);
    const explain = tip
        ? ` data-action="show-metric" data-metric="${attr(row.key)}" data-tip="${attr(tip)}"`
          + ' role="button" tabindex="0" aria-haspopup="dialog"'
        : '';

    return `
        <div class="${classes}${tip ? ' is-explainable' : ''}" data-count-key="${attr(row.key)}"${explain}>
            <span class="ledger-op" aria-hidden="true">${esc(row.prefix || '')}</span>
            <span class="ledger-dot" aria-hidden="true"></span>
            <span class="ledger-label">${esc(row.label)}</span>
            <span class="ledger-sub">${row.sub ? esc(row.sub) : ''}</span>
            <!-- Empty, and display:none until paintLedgerTrends() has two
                 samples to draw. The slot costs no layout, so the trend can
                 arrive 30s in without moving a figure (#18). -->
            <span class="ledger-spark"${row.spark ? ` data-spark="${attr(row.key)}"` : ''}></span>
            <!-- Empty in live mode, and empty costs no layout. #20 fills it
                 while time travelling with the change measured to the NEXT
                 reading (live, at the end of the series) -- what happened
                 after this sample, which is the actual insight up there. -->
            <span class="ledger-delta" data-delta-key="${attr(row.key)}"></span>
            <span class="ledger-num" data-num-key="${attr(row.key)}" data-label="${attr(row.label)}"
                  data-value="${attr(value)}">${num(value)}</span>
        </div>
    `;
}

// The stacked proportion bar. Same five tones as the sum rows, in the same
// order, so the eye can map a stripe back to a line without a legend.
function ledgerBar(counts, total) {
    if (!total) {
        return '<div class="ledger-bar is-empty" role="img" aria-label="Nothing to do"></div>';
    }
    const segments = LEDGER_SUM_ROWS.map(row => {
        const value = Number(counts[row.key]) || 0;
        if (!value) return '';
        const pct = (value / total) * 100;
        return `<span class="ledger-seg tone-${row.tone}" style="width: ${pct.toFixed(4)}%"
                      title="${attr(row.label)}: ${attr(num(value))}"></span>`;
    }).join('');
    const label = LEDGER_SUM_ROWS
        .filter(row => Number(counts[row.key]) > 0)
        .map(row => `${row.label} ${num(counts[row.key])}`)
        .join(', ');
    return `<div class="ledger-bar" role="img" aria-label="${attr(label)}">${segments}</div>`;
}

// `opts.flash: false` tweens the numbers without the change-flash or the
// screen-reader announcement. Stepping through history moves every figure on
// screen, and a flash there would say "the hub just moved" -- which is exactly
// the misreading #20 is built to prevent.
function renderLedger(counts, opts) {
    const container = document.getElementById('ledger');
    if (!container) return;
    counts = counts || {};

    // The refresh is about to detach whatever row a tooltip is pointing at.
    if (tipAnchor && container.contains(tipAnchor)) hideTip();

    const total = Number(counts.total) || 0;
    const done = Number(counts.done) || 0;
    const pct = total ? (done / total) * 100 : 0;

    const sumRows = LEDGER_SUM_ROWS.map(row => ledgerRow({
        ...row,
        prefix: '+',
        value: counts[row.key],
        alarm: row.key === 'errors',
        spark: SPARK_ROWS.includes(row.key),
    })).join('');

    // Subsets of the sum. Bracketed and indented so they can never be read as
    // additive: `delayed` lives inside Pending, `handbacks` inside Pending +
    // On Hold. See BUGS.md #35 / #50.
    const subsetRows = [
        ledgerRow({
            key: 'delayed',
            label: 'of which delayed',
            tone: 'warn',
            prefix: '↳',
            value: counts.delayed,
        }),
        ledgerRow({
            key: 'handbacks',
            label: 'of which handed back',
            tone: 'warn',
            prefix: '↳',
            value: counts.handbacks,
            sub: counts.handbacks_max ? `max ${num(counts.handbacks_max)}×` : '',
            alarm: Number(counts.handbacks_max) >= HANDBACK_ALARM,
        }),
    ].join('');

    // Not jobs at all -- retained results (BUGS.md #36) and chunks of an
    // uncommitted upload (BUGS.md #49). Outside the sum entirely.
    const asideRows = [
        ledgerRow({
            key: 'results',
            label: 'Results held',
            tone: 'idle',
            value: counts.results,
            sub: counts.results_bytes ? `~${formatBytes(counts.results_bytes)}` : '',
        }),
        ledgerRow({
            key: 'staged',
            label: 'Staged uploads',
            tone: 'idle',
            value: counts.staged,
            sub: counts.staged_uploads
                ? `${num(counts.staged_uploads)} upload${Number(counts.staged_uploads) === 1 ? '' : 's'}`
                : '',
            alarm: true,
        }),
    ].join('');

    container.innerHTML = `
        <div class="ledger-card card">
            <div class="ledger-headline">
                <div class="ledger-headline-main">
                    <span class="ledger-headline-label">Total jobs</span>
                    <span class="ledger-headline-value" data-num-key="total" data-label="Total jobs"
                          data-value="${attr(total)}">${num(total)}</span>
                </div>
                <!-- Grows into the gap the headline already had between the
                     total and the remaining/complete pair, so a trend arriving
                     after the second poll shifts nothing (#18). -->
                <div class="ledger-headline-trend">
                    <span class="ledger-spark" data-spark="total" data-spark-size="head"></span>
                    <span class="spark-delta"></span>
                </div>
                <div class="ledger-headline-meta">
                    <span class="ledger-meta-item">
                        <span class="ledger-meta-value" data-num-key="remaining" data-label="Remaining"
                              data-value="${attr(counts.remaining)}">${num(counts.remaining)}</span>
                        <span class="ledger-meta-label">remaining</span>
                    </span>
                    <span class="ledger-meta-item">
                        <span class="ledger-meta-value">${total ? pct.toFixed(total >= 1000 ? 1 : 0) : '—'}${total ? '%' : ''}</span>
                        <span class="ledger-meta-label">complete</span>
                    </span>
                </div>
            </div>
            ${ledgerBar(counts, total)}
            <div class="ledger-body">
                <div class="ledger-rows" role="group" aria-label="Job states, summing to the total">
                    ${sumRows}
                    ${ledgerRow({ key: 'total', label: 'Total', tone: 'accent', prefix: 'Σ', value: total, total: true })}
                </div>
                <div class="ledger-aside">
                    <!-- role=group + aria-labelledby: these titles were styled
                         headings that named nothing programmatically, so the
                         "included above" / "not jobs" distinction -- the whole
                         point of the aside -- was invisible to a screen
                         reader (#17). -->
                    <div class="ledger-aside-block" role="group" aria-labelledby="ledgerSubsetTitle">
                        <div class="ledger-aside-title" id="ledgerSubsetTitle">Included above</div>
                        ${subsetRows}
                    </div>
                    <div class="ledger-aside-block" role="group" aria-labelledby="ledgerNotJobsTitle">
                        <div class="ledger-aside-title" id="ledgerNotJobsTitle">Not jobs</div>
                        ${asideRows}
                    </div>
                </div>
            </div>
        </div>
    `;

    paintLedgerNumbers(container, counts, opts);
}

// Tween every number from what it was on the previous render, and flash the
// rows that actually moved. A live dashboard should show change, not just new
// text appearing where old text was.
function paintLedgerNumbers(container, counts, opts) {
    const flash = !opts || opts.flash !== false;
    const prev = ledgerPrev;
    const next = {};
    const moved = [];
    container.querySelectorAll('[data-num-key]').forEach(el => {
        const key = el.dataset.numKey;
        const to = Number(el.dataset.value) || 0;
        next[key] = to;
        const from = prev && key in prev ? prev[key] : to;
        if (from === to) return;
        moved.push({ key, from, to, label: el.dataset.label || key });
        tweenNumber(el, from, to);
        const row = flash && (el.closest('.ledger-row') || el.closest('.ledger-headline'));
        if (row) {
            row.classList.remove('is-changed');
            void row.offsetWidth;  // restart the animation on a re-render
            row.classList.add('is-changed');
        }
    });
    ledgerPrev = next;
    // Only on a *change*, and only from the second render on: `prev` is null on
    // the first paint, when everything "moved" from nothing.
    if (flash && prev && moved.length) announceLedger(moved);
    // `prev === null` is the first paint, and the first paint is the only one
    // that gets the cascade. A ledger whose rows fly in every 30 seconds is
    // unreadable (#18).
    if (!prev) staggerIn(container.querySelectorAll('.ledger-rows > .ledger-row'));
}

// Adds the one-shot entrance to a list of elements, numbering them so the CSS
// can delay each one a little further. Skipped wholesale under reduced motion:
// the class carries a delay, and a delay is not something the global
// `animation-duration: 0` override can cancel.
function staggerIn(nodes, options) {
    if (prefersReducedMotion()) return;
    const start = (options && options.start) || 0;
    // Past ~12 the cascade stops reading as one gesture and starts being a
    // wait, so everything after that arrives together.
    const cap = (options && options.cap) || 12;
    Array.prototype.forEach.call(nodes, (el, i) => {
        el.style.setProperty('--i', String(Math.min(start + i, cap)));
        el.classList.add('stagger-in');
        el.addEventListener('animationend', () => {
            el.classList.remove('stagger-in');
            el.style.removeProperty('--i');
        }, { once: true });
    });
}

// The flash-and-tween that reports a change (#2) is pure sight. A screen reader
// heard nothing at all when the numbers moved -- the dashboard just went quiet
// for as long as you left it open.
//
// This is deliberately NOT `aria-live` on the numbers themselves: ten polite
// regions all updating inside one 30s tick is a queue of ten interruptions, and
// a tweened number would fire on every animation frame. One region, one
// sentence, only what actually moved.
const LEDGER_ANNOUNCE_MAX = 4;

function ledgerLiveRegion() {
    let el = document.getElementById('ledgerLive');
    if (el) return el;
    const host = document.getElementById('statusSection');
    if (!host) return null;
    el = document.createElement('div');
    el.id = 'ledgerLive';
    el.className = 'sr-only';
    el.setAttribute('role', 'status');
    el.setAttribute('aria-atomic', 'true');
    host.appendChild(el);
    return el;
}

function announceLedger(moved) {
    const el = ledgerLiveRegion();
    if (!el) return;
    // Errors first, then the biggest movers: if the hub is churning through
    // thousands of jobs, "2 new errors" is the sentence that matters.
    const ranked = [...moved].sort((a, b) => {
        if ((a.key === 'errors') !== (b.key === 'errors')) return a.key === 'errors' ? -1 : 1;
        return Math.abs(b.to - b.from) - Math.abs(a.to - a.from);
    });
    const parts = ranked.slice(0, LEDGER_ANNOUNCE_MAX).map(m => {
        const delta = m.to - m.from;
        const direction = delta > 0 ? 'up' : 'down';
        return `${m.label} ${num(m.to)}, ${direction} ${num(Math.abs(delta))}`;
    });
    const rest = ranked.length - parts.length;
    if (rest > 0) parts.push(`and ${num(rest)} more changed`);
    // Re-setting a live region to the string it already holds announces
    // nothing, and two identical ticks in a row is a real thing here.
    const text = parts.join('. ') + '.';
    el.textContent = el.textContent === text ? text + ' ' : text;
}

function tweenNumber(el, from, to) {
    if (prefersReducedMotion()) {
        el.textContent = num(to);
        return;
    }
    const duration = 520;
    const start = performance.now();
    const step = now => {
        if (!el.isConnected) return;
        const t = Math.min(1, (now - start) / duration);
        const eased = 1 - Math.pow(1 - t, 3);
        el.textContent = num(Math.round(from + (to - from) * eased));
        if (t < 1) requestAnimationFrame(step);
    };
    requestAnimationFrame(step);
}

// --- Errors section ---------------------------------------------------------
// Failures used to be smuggled into the worker grid as a synthetic worker
// called `buelon_errors`. They are not a worker, so they get their own
// section -- and it exists *only* while `counts.errors > 0`. No empty card,
// no "0 errors" placeholder: a healthy hub shows nothing here at all.
//
// Rows are grouped by the first line of the error message, because one bug
// hitting 47 jobs is one problem, not 47. A row expands in place rather than
// navigating: triage means reading several tracebacks in a row and comparing
// them, and a page hop per error loses your place. The job page is still one
// click away from inside the expanded row.

// Collapse to this many groups before offering "show all".
const ERROR_GROUP_PREVIEW = 10;

// Failed jobs, merged with their index-aligned {error, trace, worker_name}.
// Kept out of `currentData.workers` deliberately -- see above.
let errorJobs = [];
// Group keys the operator has opened, kept across refreshes.
let expandedErrors = new Set();
let errorsShowAll = false;
// 'idle' -> 'confirm' -> 'busy' -> 'done'. Reset is destructive and used to be
// a single misclick in the global header.
let errorResetState = 'idle';
// Non-empty while `counts.errors > 0` but `/errors` could not be fetched: the
// reason, for the failed state that stands in for the list (#16).
let errorsFailure = '';

function firstLine(text) {
    const line = String(text ?? '').split('\n').find(l => l.trim());
    return (line || '').trim();
}

// Group failures by their headline message so a stampede of one bug reads as
// one row with a `× 47` badge.
function groupErrors(jobs) {
    const groups = new Map();
    jobs.forEach(job => {
        const message = firstLine(job.error) || 'Unknown error';
        let group = groups.get(message);
        if (!group) {
            group = { key: message, message, jobs: [] };
            groups.set(message, group);
        }
        group.jobs.push(job);
    });
    // Biggest blast radius first.
    return [...groups.values()].sort((a, b) => b.jobs.length - a.jobs.length);
}

// The reset control, as a three-state affordance instead of an alert().
function errorResetControl() {
    if (errorResetState === 'confirm') {
        return `
            <div class="error-reset is-confirm" role="group" aria-label="Confirm reset">
                <span class="error-reset-ask">Requeue every failed job for another attempt?</span>
                <button class="btn btn-primary" data-action="reset-errors-confirm">Yes, requeue</button>
                <button class="btn btn-secondary" data-action="reset-errors-cancel">Cancel</button>
            </div>
        `;
    }
    if (errorResetState === 'busy') {
        return `
            <div class="error-reset">
                <button class="btn btn-secondary" disabled>
                    <span class="spinner spinner-inline" aria-hidden="true"></span>Resetting…
                </button>
            </div>
        `;
    }
    return `
        <div class="error-reset">
            <button class="btn btn-secondary" data-action="reset-errors">Reset errors</button>
        </div>
    `;
}

function errorGroupRow(group, index) {
    const lead = group.jobs[0];
    const count = group.jobs.length;
    const open = expandedErrors.has(group.key);
    const panelId = `errorPanel${index}`;
    const meta = [
        lead.worker_name ? `on ${lead.worker_name}` : '',
        lead.type ? lead.type : '',
        lead.scope ? lead.scope : '',
        Number(lead.attempts) > 0 ? `attempt ${num(lead.attempts)}` : '',
        timeAgo(lead.created),
    ].filter(Boolean).join(' · ');

    return `
        <div class="error-group${open ? ' is-open' : ''}">
            <button class="error-row" type="button" data-action="toggle-error"
                    data-error-key="${attr(group.key)}"
                    aria-expanded="${open ? 'true' : 'false'}" aria-controls="${attr(panelId)}">
                <span class="error-chevron" aria-hidden="true">▸</span>
                <span class="error-row-main">
                    <span class="error-row-name">${esc(lead.name)}${
                    count > 1 ? `<span class="error-row-more">+${num(count - 1)} more</span>` : ''}</span>
                    <span class="error-row-message">${esc(group.message)}</span>
                    ${meta ? `<span class="error-row-meta">${esc(meta)}</span>` : ''}
                </span>
                ${count > 1 ? `<span class="error-count" title="${attr(num(count))} jobs failed this way">× ${num(count)}</span>` : ''}
            </button>
            <div class="error-panel" id="${attr(panelId)}" ${open ? '' : 'hidden'}>
                ${open ? errorPanel(group, index) : ''}
            </div>
        </div>
    `;
}

// Full message + traceback. Both land via setText() -- never innerHTML.
function errorPanel(group, index) {
    const lead = group.jobs[0];
    const jobLinks = group.jobs.map(job => `
        <button class="error-job-link" type="button" data-action="select-error-job"
                data-job-id="${attr(job.id)}">
            <span class="error-job-name">${esc(job.name)}</span>
            <span class="error-job-id">${esc(job.id)}</span>
        </button>
    `).join('');

    return `
        <div class="error-panel-inner">
            <div class="error-panel-block">
                <div class="error-panel-label">Error</div>
                <pre class="code-block" id="errorMessage${index}"></pre>
            </div>
            ${lead.trace ? `
            <div class="error-panel-block">
                <div class="error-panel-label">Traceback</div>
                <pre class="code-block" id="errorTrace${index}"></pre>
            </div>` : ''}
            <div class="error-panel-block">
                <div class="error-panel-label">${group.jobs.length > 1 ? `${num(group.jobs.length)} affected jobs` : 'Job'}</div>
                <div class="error-job-links">${jobLinks}</div>
            </div>
        </div>
    `;
}

// Shared by the list and by the "could not fetch them" state below, so the
// section looks like the same section either way.
const ERROR_SECTION_ICON = `
    <span class="section-icon error-icon" aria-hidden="true">
        <svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor"
             stroke-width="2" stroke-linecap="round" stroke-linejoin="round" focusable="false">
            <path d="M10.3 3.9 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0Z"/>
            <path d="M12 9v4"/><path d="M12 17h.01"/>
        </svg>
    </span>`;

// The hub says there are failures and we could not fetch them. This is a
// THIRD state for the section, distinct from "no errors" (hidden) and "here
// they are" (the list): the count is trustworthy, the detail is missing.
function renderErrorsFailure(count, detail) {
    const section = document.getElementById('errorsSection');
    if (!section) return;
    expandedErrors = new Set();
    errorsShowAll = false;
    section.hidden = false;
    section.innerHTML = `
        ${sectionHeader('errors', {
            title: `${num(count)} failed job${count === 1 ? '' : 's'}`,
            meta: 'detail unavailable',
            icon: ERROR_SECTION_ICON,
        })}
        ${blankState({
            kind: 'failed',
            title: 'Could not load the failures',
            body: `The status counts report ${num(count)} failed job${count === 1 ? '' : 's'},`
                + ' but the request for their messages and tracebacks did not come back.'
                + ' The count is still good — the detail is missing, not the errors.',
            detail,
            // No frame: `.errors-section` already draws one, and a red panel
            // inside a red panel is just noise.
            actions: [{label: 'Try again', name: 'refresh', primary: true}],
        })}
    `;
}

function renderErrors(jobs) {
    const section = document.getElementById('errorsSection');
    if (!section) return;

    // No errors -> the section does not exist. Also true right after a reset,
    // with no manual refresh needed.
    if (!jobs || !jobs.length) {
        section.hidden = true;
        section.innerHTML = '';
        expandedErrors = new Set();
        errorsShowAll = false;
        return;
    }

    const groups = groupErrors(jobs);
    const hidden = errorsShowAll ? 0 : Math.max(0, groups.length - ERROR_GROUP_PREVIEW);
    const shown = errorsShowAll ? groups : groups.slice(0, ERROR_GROUP_PREVIEW);
    const distinct = groups.length === jobs.length
        ? ''
        : `${num(groups.length)} distinct error${groups.length === 1 ? '' : 's'}`;

    // The generic #6 header, with the count in the title (it is the headline
    // an operator reads), the distinct-error count as inline meta, and the
    // reset control sharing the actions slot with the (i) button.
    section.hidden = false;
    section.innerHTML = `
        ${sectionHeader('errors', {
            title: `${num(jobs.length)} failed job${jobs.length === 1 ? '' : 's'}`,
            meta: distinct,
            icon: ERROR_SECTION_ICON,
            extra: errorResetControl(),
        })}
        <div class="error-list">
            ${shown.map((group, i) => errorGroupRow(group, i)).join('')}
        </div>
        ${hidden ? `
        <button class="error-more" type="button" data-action="show-all-errors">
            Show all ${num(groups.length)} errors (${num(hidden)} more)
        </button>` : ''}
    `;

    // Untrusted, newline-bearing text goes in as text, never as markup.
    shown.forEach((group, i) => {
        if (!expandedErrors.has(group.key)) return;
        setText(section, `#errorMessage${i}`, group.jobs[0].error || group.message);
        if (group.jobs[0].trace) setText(section, `#errorTrace${i}`, group.jobs[0].trace);
    });
}

function toggleErrorGroup(key) {
    if (expandedErrors.has(key)) expandedErrors.delete(key);
    else expandedErrors.add(key);
    renderErrors(errorJobs);
    // The re-render replaced the button that was just activated; put focus
    // back on its replacement so keyboard users do not get dumped to <body>.
    const row = [...document.querySelectorAll('.error-row')]
        .find(el => el.dataset.errorKey === key);
    if (row) row.focus();
}

// Reset is destructive: arm, confirm, then show the section vanish.
function armErrorReset() {
    errorResetState = 'confirm';
    renderErrors(errorJobs);
}

function cancelErrorReset() {
    errorResetState = 'idle';
    renderErrors(errorJobs);
}

async function confirmErrorReset() {
    const count = errorJobs.length;
    errorResetState = 'busy';
    renderErrors(errorJobs);
    const result = await resetErrors();
    errorResetState = 'idle';
    if (result === null) {
        // Used to append a <div> that was never removed, so a second failure
        // stacked a second copy and a screen reader heard neither (#16).
        renderErrors(errorJobs);
        toast('Could not requeue the failed jobs', {
            tone: 'danger',
            key: 'reset-errors',
            detail: 'The request to the hub failed. Nothing was requeued — check the web server logs.',
            action: {label: 'Try again', name: 'reset-errors-confirm'},
        });
        return;
    }
    // The section is about to vanish, which on its own is ambiguous: did it
    // work, or did the errors just stop being reported? Say which.
    toast(`Requeued ${num(count)} failed job${count === 1 ? '' : 's'}`, {
        tone: 'ok',
        key: 'reset-errors',
        detail: 'They are pending again. A job that fails once more comes straight back here.',
    });
    // Optimistically drop the section, then reconcile with the hub. Going
    // through refreshDashboard() (not initApp()) keeps the poll clock and the
    // header's "updated Ns ago" honest about this fetch.
    errorJobs = [];
    renderErrors(errorJobs);
    await refreshDashboard();
}

// --- Workers section (#8) ---------------------------------------------------
// Refined, not redesigned: the grid stays -- a connected worker is genuinely
// card-shaped data -- but each card now answers three questions instead of
// showing two bare numbers: is it alive, how much of the fleet's work is
// sitting on it, and what is it running.
//
// Cards are DOM-preserved across the 30s refresh. `workerCards` maps client id
// -> element; a refresh updates text in place and only re-inserts a node when
// the sort order actually changed. Re-rendering `innerHTML` is what made the
// old grid feel unstable, and it also dropped hover, focus and any open
// tooltip every 30 seconds.

// Scope chips shown before collapsing the rest into `+N`.
const WORKER_SCOPE_CHIPS = 3;

// Cards rendered before offering "show all". A 200-worker fleet is a 13,000px
// wall of cards, and busiest-first ordering means the ones worth looking at
// are already at the top. Expanding is one click and sticks for the session.
const WORKER_PREVIEW = 24;
let workersShowAll = false;

// Live card elements, keyed by client id. Rebuilt only for workers that are
// new to the grid.
let workerCards = new Map();

// `holds` and `jobs` appear only while something is checked out
// (`bi_get_all_worker_info` sets them together), so neither key is assumed.
// `holds` is the number to trust for "is it busy"; `jobs` can lag it.
function workerHolds(worker) {
    const holds = Number((worker || {}).holds);
    if (Number.isFinite(holds) && holds > 0) return holds;
    const jobs = (worker || {}).jobs;
    return Array.isArray(jobs) ? jobs.length : 0;
}

// Scopes are not a field on the wire: a worker sends `settings.worker.info`,
// which only guarantees `name`. What IS available is the scope of every job it
// is holding -- the more honest answer anyway, since it is what the worker is
// actually running rather than what it advertised. A deployment that puts
// `scopes` in `worker.info` (string or list) gets that used verbatim.
function workerScopes(worker, jobs) {
    const declared = (worker || {}).scopes;
    if (typeof declared === 'string') {
        return declared.split(',').map(s => s.trim()).filter(Boolean);
    }
    if (Array.isArray(declared)) {
        return declared.map(s => String(s).trim()).filter(Boolean);
    }
    return [...new Set(jobs.map(job => job && job.scope).filter(Boolean))].sort();
}

function workerModel(id, worker, fleetHolds) {
    const w = worker || {};
    const jobs = Array.isArray(w.jobs) ? w.jobs : [];
    const holds = workerHolds(w);
    return {
        id,
        name: String(w.name || 'Unnamed worker'),
        holds,
        jobCount: jobs.length,
        scopes: workerScopes(w, jobs),
        share: fleetHolds > 0 ? holds / fleetHolds : 0,
    };
}

// Busiest first, then by reported jobs, then name, then client id. Fully
// deterministic on purpose: cards that reshuffle between refreshes are what
// made the dashboard feel like it was guessing.
function sortWorkers(models) {
    return models.sort((a, b) =>
        b.holds - a.holds
        || b.jobCount - a.jobCount
        || a.name.localeCompare(b.name)
        || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
}

// `0.07` -> `'7%'`. A worker in a 200-worker fleet holds well under 1% and
// should read as "barely any", not as `0%`.
function sharePct(share) {
    const pct = share * 100;
    if (pct <= 0) return '0%';
    if (pct < 1) return '<1%';
    return `${Math.round(pct)}%`;
}

// Static markup only -- every untrusted value (name, client id, scope) is
// written with textContent by `updateWorkerCard`, never interpolated.
function workerCardEl(id) {
    const el = document.createElement('button');
    el.type = 'button';
    el.className = 'card card-clickable worker-card';
    el.dataset.action = 'select-worker';
    el.dataset.workerId = id;
    el.innerHTML = `
        <span class="worker-top">
            <span class="worker-ident">
                <span class="worker-name"></span>
                <span class="worker-id"></span>
            </span>
            <span class="worker-state">
                <span class="worker-dot" aria-hidden="true"></span>
                <span class="worker-state-text"></span>
            </span>
        </span>
        <span class="worker-metrics">
            <span class="worker-holds">
                <span class="worker-holds-value"></span>
                <span class="worker-holds-label"></span>
            </span>
            <span class="worker-share-text" hidden></span>
        </span>
        <span class="worker-share-bar" hidden aria-hidden="true">
            <span class="worker-share-fill"></span>
        </span>
        <span class="worker-note" hidden></span>
        <span class="worker-scopes" hidden>
            <span class="worker-scopes-label">running</span>
        </span>
    `;
    return el;
}

function updateWorkerCard(el, model, fleetHolds) {
    const live = model.holds > 0;
    const plural = model.holds === 1 ? '' : 's';
    const pct = sharePct(model.share);

    el.classList.toggle('is-live', live);
    // The whole identity, for a name or client id the card had to clip.
    el.dataset.tip = `${model.name} · ${model.id}`;
    el.setAttribute('aria-label', live
        ? `${model.name}: live, holding ${num(model.holds)} job${plural}, `
          + `${pct} of the work held across the fleet`
        : `${model.name}: idle, holding nothing`);

    setText(el, '.worker-name', model.name);
    setText(el, '.worker-id', model.id);
    setText(el, '.worker-state-text', live ? 'Live' : 'Idle');
    el.querySelector('.worker-state').className = `worker-state tone-${live ? 'ok' : 'idle'}`;

    setText(el, '.worker-holds-value', num(model.holds));
    setText(el, '.worker-holds-label', `hold${plural}`);

    // Share of the fleet's held work, as a bar under the count -- the same
    // idiom as the ledger's proportion bar. With nothing held anywhere there
    // is no share to show, so it goes rather than reading 0% on every card.
    const bar = el.querySelector('.worker-share-bar');
    const shareText = el.querySelector('.worker-share-text');
    bar.hidden = shareText.hidden = fleetHolds <= 0;
    el.querySelector('.worker-share-fill').style.width = `${(model.share * 100).toFixed(3)}%`;
    shareText.textContent = fleetHolds > 0 ? `${pct} of ${num(fleetHolds)} held` : '';

    // Holds and jobs can legitimately disagree for a moment -- a just-taken
    // hold may not have been reported in detail yet. Say so instead of
    // showing two numbers that look wrong.
    const note = el.querySelector('.worker-note');
    let noteText = '';
    if (live && model.jobCount === 0) noteText = 'jobs not reported yet';
    else if (model.jobCount !== model.holds) {
        noteText = `${num(model.jobCount)} of ${num(model.holds)} jobs reported`;
    }
    note.textContent = noteText;
    note.hidden = !noteText;

    updateWorkerScopes(el, model);
}

function updateWorkerScopes(el, model) {
    const host = el.querySelector('.worker-scopes');
    const label = host.querySelector('.worker-scopes-label');
    // A worker holding jobs always has scopes; a worker holding none has
    // nothing to report, and hiding the whole row is the honest answer there.
    // Holding jobs with no scope reported is a real oddity, so say so rather
    // than showing a bare "running" label with nothing after it (#16).
    host.hidden = !model.scopes.length && !model.holds;

    // The chips are rebuilt every refresh; a tooltip anchored to one of them
    // would otherwise point at a detached node.
    if (tipAnchor && host.contains(tipAnchor)) hideTip();
    host.textContent = '';
    host.appendChild(label);

    if (!model.scopes.length) {
        const none = document.createElement('span');
        none.className = 'worker-scope is-none';
        none.textContent = 'no scope reported';
        host.appendChild(none);
        return;
    }

    model.scopes.slice(0, WORKER_SCOPE_CHIPS).forEach(scope => {
        const chip = document.createElement('span');
        chip.className = 'worker-scope';
        chip.textContent = scope;
        host.appendChild(chip);
    });

    const rest = model.scopes.slice(WORKER_SCOPE_CHIPS);
    if (!rest.length) return;
    const more = document.createElement('span');
    more.className = 'worker-scope is-more';
    more.textContent = `+${num(rest.length)}`;
    more.dataset.tip = rest.join(', ');
    host.appendChild(more);
}

// No workers is a real state, never a loading state -- a worker exists here
// only while its connection is open. Whether it is *alarming* depends on
// whether there is dispatchable work with nowhere to go, so the copy says
// which of the two this is.
// An empty fleet is not automatically a problem -- but it is one the moment
// anything is pending, and the two read differently (#16).
function workersEmptyState(counts) {
    const pending = Number((counts || {}).jobs) || 0;
    return blankState({
        kind: 'empty',
        icon: pending > 0 ? 'warn' : 'empty',
        tone: pending > 0 ? 'warn' : 'idle',
        title: 'No workers connected',
        body: pending > 0
            ? `${num(pending)} pending job${pending === 1 ? '' : 's'} could be dispatched and there`
              + ' is nothing to dispatch to. Nothing will move until a worker connects.'
            : 'Nothing is pending either, so nothing is stuck — but no work can start.',
        hint: 'Start one with <code>bue worker</code> on any machine that can reach the hub.',
        frame: true,
    });
}

function renderWorkers(workers, counts) {
    const grid = document.getElementById('workersGrid');
    if (!grid) return;
    // Puts the registry's "right now" copy back after history mode swapped it.
    setSectionDescription('workers', null);

    const entries = Object.entries(workers || {});
    const fleetHolds = entries.reduce((sum, [, worker]) => sum + workerHolds(worker), 0);
    const models = sortWorkers(entries.map(([id, worker]) => workerModel(id, worker, fleetHolds)));
    const busy = models.filter(model => model.holds > 0).length;

    // The count belongs in the #6 section header, not in a card.
    setSectionMeta('workers', models.length
        ? `${num(models.length)} connected · ${busy ? `${num(busy)} busy` : 'all idle'}`
        : 'none connected');

    if (!models.length) {
        if (tipAnchor && grid.contains(tipAnchor)) hideTip();
        workerCards.clear();
        grid.innerHTML = workersEmptyState(counts);
        return;
    }

    // Drop the empty state, if that is what is standing there.
    if (!workerCards.size && grid.firstChild) grid.textContent = '';

    // First fill of the grid gets the one-shot cascade (#18). A refresh reuses
    // the same card nodes, so this is false from then on and a poll never
    // re-animates a fleet the operator is reading.
    const firstFill = !workerCards.size;
    const fresh = [];

    const visible = workersShowAll ? models : models.slice(0, WORKER_PREVIEW);
    const present = new Set(visible.map(model => model.id));
    workerCards.forEach((el, id) => {
        if (present.has(id)) return;
        if (tipAnchor && el.contains(tipAnchor)) hideTip();
        el.remove();
        workerCards.delete(id);
    });

    visible.forEach((model, index) => {
        let el = workerCards.get(model.id);
        if (!el) {
            el = workerCardEl(model.id);
            workerCards.set(model.id, el);
            fresh.push(el);
        }
        updateWorkerCard(el, model, fleetHolds);
        // Only touch the DOM position when the order really changed: moving a
        // node blurs it, and the operator may be tabbing through the grid.
        if (grid.children[index] !== el) grid.insertBefore(el, grid.children[index] || null);
    });

    // Deliberately only the first fill: a worker that connects on poll 40
    // appears, it does not perform. Its arrival is already legible because
    // nothing around it moved.
    if (firstFill) staggerIn(fresh);

    renderWorkersMore(grid, models.length - visible.length, models.length);
}

// The "show all" control, kept as one node across refreshes so a refresh
// cannot move it out from under the pointer or the focus ring.
function renderWorkersMore(grid, hidden, total) {
    let more = grid.querySelector('.workers-more');
    if (hidden <= 0) {
        if (more) more.remove();
        return;
    }
    if (!more) {
        more = document.createElement('button');
        more.type = 'button';
        more.className = 'workers-more';
        more.dataset.action = 'show-all-workers';
        grid.appendChild(more);
    }
    more.textContent = `Show all ${num(total)} workers (${num(hidden)} more)`;
    if (grid.lastElementChild !== more) grid.appendChild(more);
}

// Expanding replaces the button with cards; land focus on the first one that
// was not there before rather than dumping the keyboard back on <body>.
function showAllWorkers() {
    workersShowAll = true;
    renderWorkers(currentData && currentData.workers, currentData && currentData.counts);
    const first = document.querySelectorAll('.worker-card')[WORKER_PREVIEW];
    if (first) first.focus();
}

// Worker page rendering
function selectWorker(workerId) {
    currentWorker = {
        id: workerId,
        ...(currentData?.workers?.[workerId] || {})
    };
    renderWorkerJobs();
    showPage(2);
}

// --- Worker jobs table (#9) -------------------------------------------------
// page2 was a flat list of every held job. It is now a dense sortable table.
// Two rendering layers, deliberately:
//
//   renderWorkerJobs()  builds the shell (section header, toolbar, thead,
//                       pager) once per worker selection.
//   paintJobRows()      re-renders only <tbody> plus the count/sort/pager
//                       text on every filter keystroke, sort and page turn.
//
// That split is what lets the filter box keep focus and caret while you type,
// and what keeps the pager buttons from being pulled out from under the
// pointer. Rows are built with createElement + textContent -- no untrusted
// value is ever interpolated into markup, so there is nothing to escape.

const JOBS_PAGE_SIZE = 100;

// `cell` picks the renderer, `sort` the comparator, `first` the direction a
// first click on that heading should choose (the interesting end of the data:
// highest priority, oldest job, A first).
const JOB_COLUMNS = [
    { key: 'name', label: 'Name', cell: 'name', sort: 'text', first: 'asc' },
    { key: 'id', label: 'ID', cell: 'id', sort: 'text', first: 'asc' },
    { key: 'type', label: 'Type', cell: 'chip', sort: 'text', first: 'asc' },
    { key: 'priority', label: 'Priority', cell: 'num', sort: 'number', first: 'desc' },
    { key: 'scope', label: 'Scope', cell: 'text', sort: 'text', first: 'asc' },
    { key: 'timeout', label: 'Timeout', cell: 'secs', sort: 'number', first: 'desc' },
    { key: 'retries', label: 'Retries', cell: 'num', sort: 'number', first: 'desc' },
    { key: 'created', label: 'Age', cell: 'age', sort: 'number', first: 'asc' },
];

let jobsFilter = '';
let jobsSort = { key: 'name', dir: 'asc' };
let jobsPage = 0;

// 600 -> "10m". 0 means no ceiling at all (`Step.timeout` default), which is
// worth saying in words rather than printing a bare 0.
function formatSeconds(value) {
    const n = Number(value) || 0;
    if (n <= 0) return 'none';
    if (n < 60) return `${round1(n)}s`;
    if (n < 3600) return `${round1(n / 60)}m`;
    return `${round1(n / 3600)}h`;
}

function round1(n) {
    return String(Math.round(n * 10) / 10);
}

function jobColumn(key) {
    return JOB_COLUMNS.find(column => column.key === key);
}

function filteredJobs(jobs) {
    const query = jobsFilter.trim().toLowerCase();
    if (!query) return jobs;
    return jobs.filter(job =>
        String(job.name ?? '').toLowerCase().includes(query) ||
        String(job.id ?? '').toLowerCase().includes(query));
}

function sortedJobs(jobs) {
    const column = jobColumn(jobsSort.key) || JOB_COLUMNS[0];
    const sign = jobsSort.dir === 'desc' ? -1 : 1;
    const value = job => column.sort === 'number'
        ? (Number(job[column.key]) || 0)
        : String(job[column.key] ?? '').toLowerCase();
    return [...jobs].sort((a, b) => {
        const av = value(a);
        const bv = value(b);
        if (av < bv) return -sign;
        if (av > bv) return sign;
        // Tie-break on the id so equal rows never swap places between paints.
        return String(a.id ?? '').localeCompare(String(b.id ?? ''));
    });
}

// The errors section reuses this page for its own job list (a failed job
// belongs to no worker), so a couple of things read differently there.
function jobsAreErrors() {
    return !!currentWorker && currentWorker.id === 'errors';
}

function renderWorkerJobs() {
    if (!currentWorker) return;
    const section = document.getElementById('jobsSection');
    if (!section) return;

    // A fresh worker starts from a clean view rather than inheriting the last
    // one's filter.
    jobsFilter = '';
    jobsPage = 0;
    jobsSort = { key: 'name', dir: 'asc' };

    // A connected-but-idle worker has no `jobs` key at all (bi_get_all_worker_info
    // only sets it when there is something checked out), so never assume an array.
    const jobs = currentWorker.jobs || [];
    const errors = jobsAreErrors();
    const holds = Number(currentWorker.holds) || 0;

    // The page's only h1 (#17). It is sr-only, so this is the one place a
    // screen reader learns which worker this page is about before the
    // section headings start.
    setPageTitle(errors ? 'Failed jobs' : `Worker ${currentWorker.name || ''}`.trim());

    section.innerHTML = `
        ${sectionHeader('jobs', {
            title: errors ? 'Failed jobs' : (currentWorker.name || 'Worker'),
            meta: errors
                ? `${num(jobs.length)} parked`
                : (jobs.length
                    ? `holding ${num(jobs.length)} job${jobs.length === 1 ? '' : 's'}`
                    : 'holding nothing'),
            description: errors
                ? 'Jobs that failed and are parked. Nothing here is running.'
                : undefined,
        })}
        ${errors ? '' : jobsIdent(currentWorker.id)}
        ${!errors && holds > jobs.length ? `
        <p class="jobs-note">The hub reports ${num(holds)} holds but has details for
            ${num(jobs.length)}. The rest were taken too recently to be reported —
            Holds is the number to trust for “is it busy”.</p>` : ''}
        ${jobs.length ? jobsTableShell() : jobsEmpty(errors)}
    `;

    if (!jobs.length) return;

    // The only listener that is not delegated: `input` does not bubble to the
    // page container in a way the action table can express, and the node is
    // replaced wholesale on the next worker selection, so nothing leaks.
    const input = section.querySelector('.jobs-filter-input');
    if (input) {
        input.addEventListener('input', () => {
            jobsFilter = input.value;
            jobsPage = 0;
            paintJobRows();
        });
    }
    paintJobRows();
}

// Identity of the machine, under its name: the client id is what you match
// against a worker's own logs, so it gets a copy button too.
function jobsIdent(clientId) {
    return `
        <div class="jobs-ident">
            <span class="jobs-ident-label">client id</span>
            <span class="jobs-ident-id" title="${attr(clientId)}">${esc(clientId)}</span>
            ${copyButtonHTML(clientId, 'client id')}
        </div>
    `;
}

// Static markup only: every column label is our own constant.
function jobsTableShell() {
    const caption = jobsAreErrors()
        ? 'Failed jobs, one row per job'
        : 'Jobs held by this worker, one row per job';
    const heads = JOB_COLUMNS.map(column => `
        <th scope="col" class="head-${column.cell}" data-key="${column.key}"
            data-label="${column.label}" aria-sort="none">
            <button class="jobs-sort" type="button" data-action="sort-jobs"
                    data-key="${column.key}">
                <span>${column.label}</span>
                <span class="jobs-sort-arrow" aria-hidden="true">⇅</span>
            </button>
        </th>`).join('');

    return `
        <div class="jobs-toolbar">
            <div class="jobs-filter">
                <svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor"
                     stroke-width="2" stroke-linecap="round" aria-hidden="true">
                    <circle cx="11" cy="11" r="7"/><path d="m20 20-3.6-3.6"/>
                </svg>
                <input class="jobs-filter-input" type="search" autocomplete="off"
                       spellcheck="false" placeholder="Filter by name or id"
                       aria-label="Filter jobs by name or id">
            </div>
            <!-- No aria-live here (#17): this is rewritten on every keystroke
                 in the filter box, and a polite region that fires per character
                 reads the running total over the top of your own typing. The
                 result is announced once, debounced, by announceJobsCount(). -->
            <div class="jobs-count"></div>
        </div>
        <!-- The table is deliberately wider than a phone, so its wrap is the one
             box in the app that scrolls sideways. tabindex="0" + role=region is
             what makes that scroll reachable without a mouse (#17). -->
        <div class="jobs-table-wrap" tabindex="0" role="region"
             aria-label="${attr(caption)} (scrollable)">
            <table class="jobs-table">
                <caption class="sr-only">${esc(caption)}</caption>
                <thead><tr>${heads}</tr></thead>
                <tbody class="jobs-body"></tbody>
            </table>
        </div>
        <div class="jobs-pager" hidden>
            <button class="btn btn-secondary jobs-pager-btn" type="button"
                    data-action="jobs-page" data-page="prev">‹ Prev</button>
            <span class="jobs-pager-label"></span>
            <button class="btn btn-secondary jobs-pager-btn" type="button"
                    data-action="jobs-page" data-page="next">Next ›</button>
        </div>
    `;
}

// Holding nothing is a real state, not a blank area -- and whether it is fine
// depends on whether anything is pending, which the dashboard already says.
function jobsEmpty(errors) {
    if (errors) {
        return blankState({
            kind: 'empty',
            title: 'No failed jobs',
            body: 'Nothing is parked in the error state. This list fills up when a job'
                + ' exhausts its retries.',
            actions: [{label: 'Back to Dashboard', name: 'nav-page', data: {page: 1}}],
            frame: true,
        });
    }
    return blankState({
        kind: 'empty',
        title: 'Nothing checked out',
        body: 'This worker is connected and asking the hub for work, but is holding no'
            + ' jobs. That is healthy when Pending is 0 — and a scope mismatch when it is not.',
        hint: 'Only jobs in flight are listed here. Work this worker has already finished'
            + ' is counted in Completed, not shown.',
        frame: true,
    });
}

// Everything below re-renders on every keystroke. Bounded by JOBS_PAGE_SIZE:
// a worker holding 5,000 jobs still only ever builds 100 rows.
function paintJobRows() {
    const section = document.getElementById('jobsSection');
    const body = section && section.querySelector('.jobs-body');
    if (!body) return;

    const all = (currentWorker && currentWorker.jobs) || [];
    const matched = sortedJobs(filteredJobs(all));
    const pages = Math.max(1, Math.ceil(matched.length / JOBS_PAGE_SIZE));
    jobsPage = Math.min(Math.max(0, jobsPage), pages - 1);
    const start = jobsPage * JOBS_PAGE_SIZE;
    const shown = matched.slice(start, start + JOBS_PAGE_SIZE);

    body.textContent = '';
    if (!shown.length) {
        body.appendChild(jobsNoMatchRow());
    } else {
        const frag = document.createDocumentFragment();
        shown.forEach(job => frag.appendChild(jobRowEl(job)));
        body.appendChild(frag);
    }

    paintJobsCount(section, matched.length, all.length, start, shown.length);
    paintJobsSort(section);
    paintJobsPager(section, pages);

    const wrap = section.querySelector('.jobs-table-wrap');
    if (wrap) wrap.scrollTop = 0;
}

function jobRowEl(job) {
    const tr = document.createElement('tr');
    tr.className = 'jobs-row';
    // A failed job is not on any worker, so it is looked up in `errorJobs`.
    const action = jobsAreErrors() ? 'select-error-job' : 'select-job';
    const jobId = String(job.id ?? '');
    // The row keeps its click target for the mouse, but it is NOT focusable any
    // more (#17). `tr[tabindex=0]` put 100 tab stops on this page, each one a
    // role="row" that announced no affordance -- and the way out of the table
    // was 100 presses of Tab. The keyboard path is the button in the name cell.
    tr.dataset.action = action;
    tr.dataset.jobId = jobId;

    JOB_COLUMNS.forEach(column => {
        // The job name identifies the row, which is what `th scope=row` means:
        // it lets a screen reader prefix every cell it reads with the job.
        const isHeader = column.cell === 'name';
        const cell = document.createElement(isHeader ? 'th' : 'td');
        if (isHeader) cell.setAttribute('scope', 'row');
        cell.className = `cell-${column.cell}`;
        cell.appendChild(jobCell(column, job, { action, jobId }));
        tr.appendChild(cell);
    });
    return tr;
}

function jobCell(column, job, rowAction) {
    const value = job[column.key];

    if (column.cell === 'id') return jobIdCell(String(value ?? ''));

    if (column.cell === 'name') {
        const wrap = document.createElement('span');
        wrap.className = 'jobs-name-wrap';
        // The one focusable thing in the row, and the row's whole purpose:
        // "open this job". A button, so Enter and Space work for free and a
        // screen reader announces it as something you can activate (#17).
        const name = document.createElement('button');
        name.type = 'button';
        name.className = 'jobs-name';
        name.textContent = String(value ?? '');
        name.title = String(value ?? '');
        if (rowAction) {
            name.dataset.action = rowAction.action;
            name.dataset.jobId = rowAction.jobId;
        }
        wrap.appendChild(name);
        // A job that keeps answering `pending` is the handback-storm signal,
        // and this is the one screen where you can see which job it is.
        const handbacks = Number(job.handbacks) || 0;
        if (handbacks > 0) {
            const badge = document.createElement('span');
            badge.className = 'jobs-handbacks';
            badge.textContent = `↩ ${num(handbacks)}`;
            badge.title = `Handed itself back ${num(handbacks)} time${handbacks === 1 ? '' : 's'}`;
            wrap.appendChild(badge);
        }
        return wrap;
    }

    if (column.cell === 'chip') {
        const chip = document.createElement('span');
        chip.className = 'jobs-chip';
        chip.textContent = String(value ?? '') || '—';
        return chip;
    }

    const text = document.createElement('span');
    if (column.cell === 'num') {
        text.textContent = num(value);
    } else if (column.cell === 'secs') {
        text.textContent = formatSeconds(value);
        if (!(Number(value) > 0)) text.className = 'is-none';
    } else if (column.cell === 'age') {
        const age = timeAgo(value);
        text.textContent = age || '—';
        if (!age) text.className = 'is-none';
    } else {
        text.textContent = String(value ?? '') || '—';
        if (!String(value ?? '')) text.className = 'is-none';
    }
    return text;
}

function jobIdCell(id) {
    const wrap = document.createElement('span');
    wrap.className = 'jobs-id';
    const text = document.createElement('span');
    text.className = 'jobs-id-text';
    text.textContent = id;
    text.title = id;
    wrap.appendChild(text);
    wrap.appendChild(copyButtonEl(id, 'job id'));
    return wrap;
}

function jobsNoMatchRow() {
    const tr = document.createElement('tr');
    tr.className = 'jobs-none';
    const td = document.createElement('td');
    td.colSpan = JOB_COLUMNS.length;
    const text = document.createElement('span');
    text.textContent = `No job matches “${jobsFilter.trim()}”`;
    const clear = document.createElement('button');
    clear.type = 'button';
    clear.className = 'jobs-none-clear';
    clear.dataset.action = 'clear-jobs-filter';
    clear.textContent = 'Clear filter';
    td.appendChild(text);
    td.appendChild(clear);
    tr.appendChild(td);
    return tr;
}

function paintJobsCount(section, matched, total, start, shown) {
    const el = section.querySelector('.jobs-count');
    if (!el) return;
    const parts = [matched === total
        ? `${num(total)} job${total === 1 ? '' : 's'}`
        : `${num(matched)} of ${num(total)} match`];
    if (shown && matched > shown) {
        parts.push(`showing ${num(start + 1)}–${num(start + shown)}`);
    }
    el.textContent = parts.join(' · ');
    announceJobsCount(el.textContent);
}

// Debounced because the caller runs on every keystroke. 600ms is long enough
// that a normal typing rate produces one announcement for the word, not one per
// letter, and short enough to arrive before you reach for the results.
let jobsCountTimer = null;

function announceJobsCount(text) {
    if (jobsCountTimer) clearTimeout(jobsCountTimer);
    jobsCountTimer = setTimeout(() => {
        jobsCountTimer = null;
        announce(text);
    }, 600);
}

function paintJobsSort(section) {
    section.querySelectorAll('.jobs-table th[data-key]').forEach(th => {
        const active = th.dataset.key === jobsSort.key;
        const asc = jobsSort.dir === 'asc';
        th.setAttribute('aria-sort', active ? (asc ? 'ascending' : 'descending') : 'none');
        const arrow = th.querySelector('.jobs-sort-arrow');
        if (arrow) arrow.textContent = active ? (asc ? '↑' : '↓') : '⇅';
        const button = th.querySelector('.jobs-sort');
        if (button) {
            const next = active && asc ? 'descending' : 'ascending';
            // The label has to *contain* the visible column text, or the
            // accessible name stops matching what is on screen (WCAG 2.5.3).
            const state = active ? `, sorted ${asc ? 'ascending' : 'descending'}` : '';
            button.setAttribute('aria-label',
                `${th.dataset.label}${state} — activate to sort ${next}`);
        }
    });
}

// The pager nodes live in the shell and are only ever updated, so a page turn
// cannot move the button out from under the pointer or drop the focus ring.
function paintJobsPager(section, pages) {
    const pager = section.querySelector('.jobs-pager');
    if (!pager) return;
    pager.hidden = pages <= 1;
    const label = pager.querySelector('.jobs-pager-label');
    if (label) label.textContent = `Page ${num(jobsPage + 1)} of ${num(pages)}`;
    pager.querySelectorAll('.jobs-pager-btn').forEach(button => {
        button.disabled = button.dataset.page === 'prev'
            ? jobsPage === 0
            : jobsPage >= pages - 1;
    });
}

function sortJobs(key) {
    const column = jobColumn(key);
    if (!column) return;
    jobsSort = jobsSort.key === key
        ? { key, dir: jobsSort.dir === 'asc' ? 'desc' : 'asc' }
        : { key, dir: column.first || 'asc' };
    jobsPage = 0;
    paintJobRows();
}

function turnJobsPage(direction) {
    jobsPage += direction === 'next' ? 1 : -1;
    paintJobRows();
    // Reaching the last page disables the button you just pressed; keep the
    // keyboard in the pager instead of dumping it on <body>.
    const active = document.activeElement;
    if (active && active.classList.contains('jobs-pager-btn') && active.disabled) {
        const other = [...document.querySelectorAll('.jobs-pager-btn')]
            .find(button => !button.disabled);
        if (other) other.focus();
    }
}

function clearJobsFilter() {
    jobsFilter = '';
    jobsPage = 0;
    const input = document.querySelector('.jobs-filter-input');
    if (input) {
        input.value = '';
        input.focus();
    }
    paintJobRows();
}

// --- Copy to clipboard ------------------------------------------------------
// Ids are what you paste into a log grep, so they get a copy button. Both the
// markup and the element form exist because the shell is a template string and
// the rows are DOM nodes.

const COPY_ICONS = `
    <svg class="copy-icon" viewBox="0 0 24 24" width="14" height="14" fill="none"
         stroke="currentColor" stroke-width="2" stroke-linecap="round"
         stroke-linejoin="round" aria-hidden="true">
        <rect x="9" y="9" width="12" height="12" rx="2"/>
        <path d="M6 15H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h8a2 2 0 0 1 2 2v1"/>
    </svg>
    <svg class="copy-done-icon" viewBox="0 0 24 24" width="14" height="14" fill="none"
         stroke="currentColor" stroke-width="2.4" stroke-linecap="round"
         stroke-linejoin="round" aria-hidden="true">
        <path d="m5 13 4.5 4.5L19 7"/>
    </svg>`;

function copyButtonHTML(value, what) {
    return `
        <button class="copy-btn" type="button" data-action="copy"
                data-copy="${attr(value)}" data-copy-what="${attr(what)}"
                aria-label="Copy ${attr(what)}">${COPY_ICONS}</button>
    `;
}

function copyButtonEl(value, what) {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'copy-btn';
    button.dataset.action = 'copy';
    button.dataset.copy = value;
    button.dataset.copyWhat = what;
    button.setAttribute('aria-label', `Copy ${what}`);
    button.innerHTML = COPY_ICONS;  // constant markup, no interpolation
    return button;
}

async function copyValue(button) {
    const what = button.dataset.copyWhat || 'value';
    flashCopy(button, await writeClipboard(button.dataset.copy || ''), what);
}

// The state flash, shared with `lazyCopyButton` (#12), which cannot park a
// multi-megabyte payload in a `data-copy` attribute and computes it on click.
function flashCopy(button, ok, what) {
    button.classList.remove('is-done', 'is-fail');
    button.classList.add(ok ? 'is-done' : 'is-fail');
    // A success needs no words: the icon becomes a tick right where you
    // clicked. A failure is invisible without them, because the thing you
    // were about to paste is not on your clipboard (#16).
    if (ok) announce(`Copied ${what}`);
    else toast(`Could not copy the ${what}`, {
        tone: 'warn',
        key: 'copy-failed',
        detail: 'The browser refused clipboard access. Select the text and copy it by hand.',
    });
    clearTimeout(button.copyTimer);
    button.copyTimer = setTimeout(
        () => button.classList.remove('is-done', 'is-fail'), 1400);
}

async function writeClipboard(text) {
    try {
        if (navigator.clipboard && window.isSecureContext) {
            await navigator.clipboard.writeText(text);
            return true;
        }
    } catch (error) {
        // Fall through: permission denied, or no clipboard at all.
    }
    // The dashboard is usually served over plain http on a LAN, where
    // `navigator.clipboard` does not exist at all. execCommand still works.
    try {
        const area = document.createElement('textarea');
        area.value = text;
        area.setAttribute('readonly', '');
        // It exists for one execCommand tick and is focused by .select(), so
        // keep it out of the accessibility tree entirely (#17).
        area.setAttribute('aria-hidden', 'true');
        area.tabIndex = -1;
        area.style.position = 'fixed';
        area.style.top = '-1000px';
        area.style.opacity = '0';
        document.body.appendChild(area);
        area.select();
        const ok = document.execCommand('copy');
        area.remove();
        return ok;
    } catch (error) {
        return false;
    }
}

// One shared live region: a copy has no visible-to-a-screen-reader result.
function announce(text) {
    let el = document.getElementById('liveStatus');
    if (!el) {
        el = document.createElement('div');
        el.id = 'liveStatus';
        el.className = 'sr-only';
        el.setAttribute('role', 'status');
        el.setAttribute('aria-live', 'polite');
        document.body.appendChild(el);
    }
    el.textContent = text;
}

// Job details rendering
function selectJob(jobId) {
    // Find the job on whichever worker has it checked out.
    for (const workerId in (currentData?.workers || {})) {
        const worker = currentData.workers[workerId];
        const job = (worker.jobs || []).find(j => j.id === jobId);
        if (job) {
            currentJob = job;
            currentWorker = { id: workerId, ...worker }; // Set current worker context
            break;
        }
    }
    renderJobDetails();
    showPage(3);
}

// A failed job belongs to no worker, so it is looked up in `errorJobs` and the
// breadcrumb's middle crumb points back at the Errors section instead.
function selectErrorJob(jobId) {
    const job = errorJobs.find(j => j.id === jobId);
    if (!job) return;
    currentJob = job;
    currentWorker = { id: 'errors', name: 'Errors', jobs: errorJobs };
    renderWorkerJobs();
    renderJobDetails();
    showPage(3);
}

// --- Job detail page (#11) --------------------------------------------------
// The page answers, in order: what is this job, is it broken, where does it sit
// in the pipeline, what does it run. Everything rarely read (timeout, local,
// func, created, attempts) is demoted to one Configuration grid at the bottom.
//
// The hub does not send a job's state -- `counts` is aggregate and a job JSON
// has no status field -- so `jobState()` DERIVES it from the only two honest
// signals we have: which list the job was opened from, and `not_before`. It
// says "Unknown" rather than guessing when neither applies, and the badge's
// tooltip says where the answer came from. Do not invent a state field here;
// if this page ever needs a real one it comes from `hub.py` in its own task.

const JOB_STATES = {
    error: {
        label: 'Error',
        tone: 'danger',
        tip: 'Failed and parked. It will not run again until someone resets it.',
    },
    delayed: {
        label: 'Delayed',
        tone: 'warn',
        tip: 'Pending, but the hub is holding it back until its not-before time passes.',
    },
    held: {
        label: 'On hold',
        tone: 'warn',
        tip: 'Checked out by a worker right now. This is a job that is running.',
    },
    unknown: {
        label: 'State unknown',
        tone: 'idle',
        tip: 'Opened without a list to place it in — the hub does not send a per-job state.',
    },
};

function jobNotBefore(job) {
    const t = Number(job && job.not_before);
    return Number.isFinite(t) && t > 0 ? t : 0;
}

function jobState(job) {
    if (!job) return 'unknown';
    if (job.error || job.trace) return 'error';
    if (currentWorker && currentWorker.id && currentWorker.id !== 'errors') return 'held';
    if (jobNotBefore(job) > Date.now() / 1000) return 'delayed';
    return 'unknown';
}

// Where the job was found, in words. This is the provenance behind the badge,
// so it has to stay truthful about the "we were told nothing" case.
function jobLocation(job) {
    const state = jobState(job);
    if (state === 'error') {
        const worker = job.worker_name;
        return worker
            ? `Parked in Errors — last failed on ${worker}`
            : 'Parked in the hub’s error list';
    }
    if (state === 'held' && currentWorker) return `Held by ${currentWorker.name}`;
    if (state === 'delayed') {
        const wait = Math.max(0, Math.round(jobNotBefore(job) - Date.now() / 1000));
        return `Waiting ${formatSeconds(wait)} before the hub will dispatch it`;
    }
    return 'Not in any list this page can see';
}

// A job id resolves to a page only if the job is in hand: held by a connected
// worker, or parked in Errors. A parent that has already completed is in
// neither, and pretending otherwise would give us chips that navigate nowhere.
function lookupJob(id) {
    const wanted = String(id ?? '');
    if (!wanted) return null;
    for (const workerId in (currentData?.workers || {})) {
        const job = (currentData.workers[workerId].jobs || []).find(j => j.id === wanted);
        if (job) return { job, workerId, action: 'select-job' };
    }
    const failed = errorJobs.find(j => j.id === wanted);
    if (failed) return { job: failed, workerId: 'errors', action: 'select-error-job' };
    return null;
}

function shortId(id) {
    const text = String(id ?? '');
    return text.length > 10 ? `${text.slice(0, 8)}…` : text;
}

// --- Syntax highlighting ----------------------------------------------------
// A job's code is python or one of the SQL dialects, so two token sets are
// enough. Hand-rolled on purpose: no CDN, and the whole point is a hint, not a
// parser -- it never has to be right about anything but comments and strings.
// Every token's raw text goes through esc() before it is wrapped, so untrusted
// program text still cannot reach the DOM as markup.

const PY_KEYWORDS = new Set(['and', 'as', 'assert', 'async', 'await', 'break', 'class',
    'continue', 'def', 'del', 'elif', 'else', 'except', 'finally', 'for', 'from',
    'global', 'if', 'import', 'in', 'is', 'lambda', 'nonlocal', 'not', 'or', 'pass',
    'raise', 'return', 'try', 'while', 'with', 'yield', 'match', 'case']);

const PY_CONSTANTS = new Set(['True', 'False', 'None', 'self', 'cls']);

const SQL_KEYWORDS = new Set(['select', 'from', 'where', 'group', 'by', 'order', 'having',
    'limit', 'offset', 'join', 'left', 'right', 'inner', 'outer', 'full', 'cross', 'on',
    'as', 'and', 'or', 'not', 'in', 'is', 'null', 'like', 'ilike', 'between', 'case',
    'when', 'then', 'else', 'end', 'with', 'union', 'all', 'distinct', 'insert', 'into',
    'values', 'update', 'set', 'delete', 'create', 'table', 'temp', 'temporary', 'view',
    'drop', 'alter', 'add', 'primary', 'key', 'foreign', 'references', 'default',
    'constraint', 'index', 'exists', 'asc', 'desc', 'over', 'partition', 'returning',
    'coalesce', 'cast', 'count', 'sum', 'avg', 'min', 'max', 'date', 'interval', 'using']);

const PY_SCANNER = new RegExp([
    '(?<comment>#[^\\n]*)',
    '(?<string>[rbfuRBFU]{0,2}(?:"""[\\s\\S]*?"""|\'\'\'[\\s\\S]*?\'\'\'|"(?:\\\\.|[^"\\\\\\n])*"?|\'(?:\\\\.|[^\'\\\\\\n])*\'?))',
    '(?<decorator>@[A-Za-z_][\\w.]*)',
    '(?<number>\\b\\d[\\d_]*(?:\\.\\d*)?(?:[eE][+-]?\\d+)?\\b)',
    '(?<word>[A-Za-z_]\\w*)',
].join('|'), 'g');

const SQL_SCANNER = new RegExp([
    '(?<comment>--[^\\n]*|/\\*[\\s\\S]*?\\*/)',
    '(?<string>\'(?:\'\'|[^\'])*\'?|"(?:[^"])*"?)',
    '(?<param>%\\([A-Za-z_]\\w*\\)s|%s|\\?|\\$\\d+)',
    '(?<number>\\b\\d[\\d_]*(?:\\.\\d*)?\\b)',
    '(?<word>[A-Za-z_]\\w*)',
].join('|'), 'g');

function codeLanguage(type) {
    const t = String(type ?? '').toLowerCase();
    if (!t) return 'plain';
    if (t.includes('py')) return 'python';
    return 'sql';
}

// One token class per match, decided after the scan so keyword lookup stays a
// set membership test rather than a 40-branch regex.
function tokenClass(lang, groups) {
    if (groups.comment) return 'comment';
    if (groups.string) return 'string';
    if (groups.number) return 'number';
    if (groups.decorator) return 'meta';
    if (groups.param) return 'meta';
    const word = groups.word;
    if (!word) return '';
    if (lang === 'python') {
        if (PY_KEYWORDS.has(word)) return 'keyword';
        if (PY_CONSTANTS.has(word)) return 'constant';
        return '';
    }
    if (SQL_KEYWORDS.has(word.toLowerCase())) return 'keyword';
    return '';
}

// Returns an array of per-line HTML strings. Tokens that span newlines (triple
// strings, /* */ comments) are split across lines so each line can carry its
// own number without the gutter drifting.
function highlightLines(text, lang) {
    const source = String(text ?? '');
    const lines = [[]];

    const push = (cls, raw) => {
        const parts = String(raw).split('\n');
        parts.forEach((part, i) => {
            if (i) lines.push([]);
            if (!part) return;
            lines[lines.length - 1].push(
                cls ? `<span class="tok-${cls}">${esc(part)}</span>` : esc(part));
        });
    };

    const scanner = lang === 'python' ? PY_SCANNER : lang === 'sql' ? SQL_SCANNER : null;
    if (!scanner) {
        push('', source);
    } else {
        scanner.lastIndex = 0;
        let last = 0;
        let match;
        while ((match = scanner.exec(source)) !== null) {
            if (match.index > last) push('', source.slice(last, match.index));
            push(tokenClass(lang, match.groups), match[0]);
            last = match.index + match[0].length;
            // A zero-length match would spin forever; the scanner has no
            // empty alternatives today, but the guard is one line.
            if (match[0].length === 0) scanner.lastIndex++;
        }
        if (last < source.length) push('', source.slice(last));
    }

    return lines.map(pieces => pieces.join(''));
}

function codeBlock(text, lang, label) {
    const lines = highlightLines(text, lang);
    // NOT indented markup: this lands inside a <pre>, so any whitespace
    // between the tags is content and every line grows a blank line.
    const rows = lines.map((html, i) =>
        `<span class="code-line">` +
        `<span class="code-ln" aria-hidden="true">${i + 1}</span>` +
        `<span class="code-src">${html || ''}</span>` +
        `</span>`).join('');
    return `
        <div class="code-scroll" tabindex="0" role="group" aria-label="${attr(label)}">
            <pre class="code-listing"><code>${rows}</code></pre>
        </div>`;
}

// --- The page ---------------------------------------------------------------

function jobFact(label, value, options) {
    const opts = options || {};
    if (value === '' || value === null || value === undefined) return '';
    return `
        <div class="job-fact${opts.tone ? ` tone-${attr(opts.tone)}` : ''}">
            <dt>${esc(label)}</dt>
            <dd>${esc(value)}${opts.sub ? `<span class="job-fact-sub">${esc(opts.sub)}</span>` : ''}</dd>
        </div>`;
}

function jobStateBadge(job) {
    const state = jobState(job);
    const meta = JOB_STATES[state];
    return `
        <span class="job-state tone-${attr(meta.tone)}" data-tip="${attr(meta.tip)}" tabindex="0">
            <span class="job-state-dot" aria-hidden="true"></span>${esc(meta.label)}
        </span>`;
}

function jobHero(job) {
    const handbacks = Number(job.handbacks) || 0;
    const retries = Number(job.retries) || 0;
    const attempts = Number(job.attempts) || 0;
    const age = timeAgo(job.created);

    return `
        <section class="section job-hero" aria-labelledby="jobSectionTitle">
            <div class="job-hero-top">
                ${jobStateBadge(job)}
                <span class="jobs-chip job-type-chip" title="${attr(job.type || '')}">${esc(job.type || 'unknown type')}</span>
                ${handbacks > 0 ? `
                <span class="job-handbacks" data-tip="Handed itself back ${attr(num(handbacks))} time${handbacks === 1 ? '' : 's'} — it keeps answering “not ready yet” and going round again." tabindex="0">
                    ↩ ${esc(num(handbacks))}
                </span>` : ''}
                <span class="job-hero-spacer"></span>
                ${infoButton('job')}
            </div>
            <h1 class="job-hero-name" id="jobSectionTitle">${esc(job.name || 'Unnamed job')}</h1>
            <p class="job-hero-where">${esc(jobLocation(job))}</p>
            <div class="job-hero-id">
                <span class="job-hero-id-text" title="${attr(job.id)}">${esc(job.id)}</span>
                <span class="job-copy-slot" data-slot="id"></span>
            </div>
            <dl class="job-facts">
                ${jobFact('Priority', num(job.priority), { sub: 'high runs first' })}
                ${jobFact('Retries', num(retries), {
                    sub: retries === 0
                        ? 'no second chance'
                        : (attempts ? `${num(attempts)} spent` : 'error budget'),
                    tone: attempts && attempts >= retries ? 'danger' : '',
                })}
                ${jobFact('Age', age || 'unknown', { sub: age ? 'since created' : 'hub sent no timestamp' })}
                ${jobFact('Scope', job.scope || '—', { sub: 'worker pool' })}
            </dl>
        </section>`;
}

// A step with `!retries 0` has no budget at all, and "1 of 0 retries spent"
// reads as a bug rather than as the fact that it never had a second chance.
function retryBudget(job) {
    const retries = Number(job.retries) || 0;
    const attempts = Number(job.attempts) || 0;
    if (!retries) return 'no retry budget — one attempt was all it had';
    return `${num(attempts)} of ${num(retries)} attempt${retries === 1 ? '' : 's'} spent`;
}

function jobFailureSection(job) {
    if (!job.error && !job.trace) return '';
    return `
        <section class="section job-fail" aria-labelledby="jobFailureSectionTitle">
            ${sectionHeader('jobFailure', { meta: retryBudget(job) })}
            <div class="job-fail-message">
                <span class="job-fail-icon" aria-hidden="true">
                    <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor"
                         stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <circle cx="12" cy="12" r="9"/><path d="M12 8v4.5"/><path d="M12 16h.01"/>
                    </svg>
                </span>
                <p class="job-fail-text" id="jobErrorMessage"></p>
                <span class="job-copy-slot" data-slot="error"></span>
            </div>
            ${job.trace ? `
            <div class="job-trace">
                <div class="job-trace-bar">
                    <span class="job-trace-label">Traceback</span>
                    <span class="job-trace-hint">last frame highlighted</span>
                    <span class="job-copy-slot" data-slot="trace"></span>
                </div>
                <!-- tabindex so the traceback can be scrolled without a mouse;
                     role+label so it is not an anonymous tab stop (#17). -->
                <pre class="job-trace-body" id="jobTrace" tabindex="0" role="group"
                     aria-label="Python traceback"></pre>
            </div>` : ''}
        </section>`;
}

// The traceback is untrusted, newline-bearing text, so every line is a text
// node. The tail -- the last `File "..."` frame and everything after it -- is
// the part you actually read, so it is the part that is lit.
function paintTrace(host, trace) {
    const pre = host.querySelector('#jobTrace');
    if (!pre) return;
    const lines = String(trace ?? '').replace(/\s+$/, '').split('\n');
    let tail = -1;
    lines.forEach((line, i) => {
        if (/^\s*File "/.test(line)) tail = i;
    });
    let lastText = -1;
    lines.forEach((line, i) => { if (line.trim()) lastText = i; });

    pre.textContent = '';
    lines.forEach((line, i) => {
        const row = document.createElement('span');
        row.className = 'trace-line';
        if (tail >= 0 && i >= tail) row.classList.add('is-tail');
        if (i === lastText && lastText > 0) row.classList.add('is-exception');
        row.textContent = line || ' ';
        pre.appendChild(row);
    });
}

function jobChip(id) {
    const found = lookupJob(id);
    const short = shortId(id);
    if (!found) {
        return `
            <span class="job-chip is-unresolved"
                  data-tip="${attr(id)} — not held by a worker and not in Errors, so there is no page for it. It has already completed, or it is waiting its turn."
                  tabindex="0">
                <span class="job-chip-id">${esc(short)}</span>
                <span class="job-copy-slot" data-slot="chip" data-value="${attr(id)}"></span>
            </span>`;
    }
    return `
        <button class="job-chip" type="button"
                data-action="${attr(found.action)}" data-job-id="${attr(id)}"
                data-tip="${attr(`${found.job.name || 'job'} — ${id}`)}">
            <span class="job-chip-name">${esc(found.job.name || 'job')}</span>
            <span class="job-chip-id">${esc(short)}</span>
        </button>`;
}

function jobChipList(ids, empty) {
    const list = (ids || []).filter(id => id !== null && id !== undefined && id !== '');
    if (!list.length) return `<p class="job-lineage-empty">${esc(empty)}</p>`;
    return `<div class="job-chips">${list.map(id => jobChip(id)).join('')}</div>`;
}

function jobLineageSection(job) {
    const parents = job.parents || [];
    const children = job.children || [];
    return `
        <section class="section" aria-labelledby="jobLineageSectionTitle">
            ${sectionHeader('jobLineage', {
                meta: `${num(parents.length)} in · ${num(children.length)} out`,
                extra: `
                    <button class="btn btn-secondary" data-action="show-parent-tree">
                        Parent tree &amp; results
                    </button>`,
            })}
            <div class="job-lineage">
                <div class="job-lineage-side">
                    <h3 class="job-lineage-title">Parents — this job waits for these</h3>
                    ${jobChipList(parents, 'No parents. This job starts a branch of the pipeline.')}
                </div>
                <div class="job-lineage-side">
                    <h3 class="job-lineage-title">Children — these wait for this job</h3>
                    ${jobChipList(children, 'No children. This job ends a branch of the pipeline.')}
                </div>
            </div>
        </section>`;
}

function jobCodeSection(job) {
    const lang = codeLanguage(job.type);
    const text = String(job.code ?? '');
    const lineCount = text ? text.split('\n').length : 0;
    return `
        <section class="section" aria-labelledby="jobCodeSectionTitle">
            ${sectionHeader('jobCode', {
                meta: lineCount ? `${num(lineCount)} line${lineCount === 1 ? '' : 's'}` : '',
                extra: `
                    <span class="jobs-chip job-type-chip" title="${attr(job.type || '')}">${esc(job.type || 'unknown')}</span>
                    <span class="job-copy-slot" data-slot="code"></span>`,
            })}
            ${text
                ? codeBlock(text, lang, `Code for ${job.name || 'this job'}`)
                : '<p class="job-lineage-empty">The hub sent no code for this job.</p>'}
        </section>`;
}

function jobConfigSection(job) {
    const notBefore = jobNotBefore(job);
    const wait = notBefore > Date.now() / 1000
        ? formatSeconds(Math.round(notBefore - Date.now() / 1000))
        : '';
    return `
        <section class="section job-config" aria-labelledby="jobConfigSectionTitle">
            ${sectionHeader('jobConfig')}
            <dl class="job-config-grid">
                ${jobFact('Function', job.func || '—')}
                ${jobFact('Type', job.type || '—')}
                ${jobFact('Timeout', formatSeconds(job.timeout))}
                ${jobFact('Runs on', job.local ? 'the hub (local)' : 'a worker')}
                ${jobFact('Attempts', num(Number(job.attempts) || 0))}
                ${jobFact('Hand-backs', num(Number(job.handbacks) || 0))}
                ${jobFact('Created', timeAgo(job.created) || 'unknown')}
                ${wait ? jobFact('Held back for', wait, { tone: 'warn' }) : ''}
                ${job.tag ? jobFact('Tag', job.tag) : ''}
            </dl>
        </section>`;
}

// --- Run-job console (#13) --------------------------------------------------
// The section renders two empty hosts; `mountRunConsole` fills them with DOM.
// Log output is arbitrary text from a real process (a traceback can quote
// anything, including markup) so every line goes in as a text node -- nothing
// here touches innerHTML with a value.
//
// The Stop button is honest: it aborts the fetch, and `web.py`'s
// `stream_subprocess_logs` now kills the subprocess when the client goes away
// (proved by `tmp/test_run_job_cancel.py`). Before that fix an abandoned run
// kept executing on the hub, so there was nothing truthful to put here.

const CONSOLE_MAX_LINES = 2000;      // ring buffer: a chatty job cannot hang the tab
const CONSOLE_TAIL_SLACK = 24;       // px from the end that still counts as "at the end"
const CONSOLE_LINE_CAP = 8192;       // a "line" with no newline in it, flushed anyway
const CONSOLE_STATES = {
    idle: { label: 'Not run yet', tone: 'idle' },
    running: { label: 'Running', tone: 'info' },
    ok: { label: 'Finished', tone: 'ok' },
    error: { label: 'Finished with errors', tone: 'danger' },
    stopped: { label: 'Stopped', tone: 'warn' },
    failed: { label: 'Stream failed', tone: 'danger' },
};

// The mounted console, or null. One job page at a time, so one of these.
let runState = null;

function jobRunSection(job) {
    return `
        <section class="section job-run" aria-labelledby="jobRunSectionTitle">
            ${sectionHeader('jobRun')}
            <div class="run-controls" id="runControls"></div>
            <div class="console" id="runConsole"></div>
        </section>`;
}

// Called after `renderJobDetails` writes the page. Tears down any previous
// run first: navigating away mid-run must not leave a stream (and therefore a
// subprocess on the hub) running against a console nobody can see.
function mountRunConsole(job) {
    stopRunConsole();
    const controls = document.getElementById('runControls');
    const host = document.getElementById('runConsole');
    if (!controls || !host) return;

    const st = {
        jobId: String(job.id ?? ''),
        jobName: job.name || job.id || 'this job',
        phase: 'idle',
        armed: false,
        controller: null,
        lines: [],          // {text, tone}
        pending: [],        // built but not yet in the DOM
        dropped: 0,
        errorLines: 0,
        inTrace: false,
        stuck: true,        // following the tail
        unseen: 0,
        frame: 0,
        timer: 0,
        startedAt: 0,
        elapsed: 0,
        controls,
        host,
    };
    runState = st;

    // role="log" without a live region: announcing 2,000 streamed lines would
    // flood a screen reader. State changes are announced instead.
    host.innerHTML = '';
    const bar = mk('div', 'console-bar');
    st.stateEl = mk('span', 'console-state');
    st.dotEl = mk('span', 'console-dot');
    st.stateText = mk('span', 'console-state-text');
    st.stateEl.append(st.dotEl, st.stateText);
    st.elapsedEl = mk('span', 'console-elapsed tnum');
    st.countEl = mk('span', 'console-count tnum');
    bar.append(st.stateEl, st.elapsedEl, st.countEl, mk('span', 'console-spacer'));

    st.copyBtn = lazyCopyButton(() => consoleText(st), 'the log');
    st.clearBtn = mk('button', 'btn btn-ghost btn-sm console-btn', 'Clear');
    st.clearBtn.type = 'button';
    st.clearBtn.addEventListener('click', () => clearRunConsole());
    bar.append(st.copyBtn, st.clearBtn);

    st.scroll = mk('div', 'console-scroll');
    st.scroll.tabIndex = 0;
    st.scroll.setAttribute('role', 'log');
    st.scroll.setAttribute('aria-label', `Output from running ${st.jobName}`);
    st.dropEl = mk('p', 'console-drop');
    st.dropEl.hidden = true;
    st.linesEl = mk('div', 'console-lines');
    st.emptyEl = mk('p', 'console-empty',
        'No output yet. Running this step executes its real code on the hub.');
    st.scroll.append(st.dropEl, st.linesEl, st.emptyEl);
    st.scroll.addEventListener('scroll', () => onConsoleScroll(st));

    st.jumpBtn = mk('button', 'console-jump');
    st.jumpBtn.type = 'button';
    st.jumpBtn.hidden = true;
    // paintConsoleJump() fills the visible text, but only once there is
    // something to jump to. Until then this is the accessible name (#17).
    st.jumpBtn.setAttribute('aria-label', 'Jump to the end of the log');
    st.jumpBtn.addEventListener('click', () => {
        st.stuck = true;
        st.unseen = 0;
        scrollConsoleToEnd(st);
        paintConsoleJump(st);
    });

    host.append(bar, st.scroll, st.jumpBtn);
    paintRunControls();
    paintConsoleState(st);
    paintConsoleCounts(st);
}

// Leaving the page, or re-rendering it, ends the run.
function stopRunConsole() {
    const st = runState;
    runState = null;
    if (!st) return;
    if (st.controller && st.phase === 'running') st.controller.abort();
    if (st.timer) clearInterval(st.timer);
    if (st.frame) cancelAnimationFrame(st.frame);
}

function consoleText(st) {
    return st.lines.map(line => line.text).join('\n');
}

// --- Controls: one primary action, in one place -----------------------------
function paintRunControls() {
    const st = runState;
    if (!st) return;
    // Every branch below replaces innerHTML, which destroys whatever button
    // has focus. If the operator drove this from the keyboard -- Run, then
    // Yes -- focus would land back on <body> the moment the run started, with
    // no ring anywhere on the page (#17). Remember, then re-place.
    const hadFocus = st.controls.contains(document.activeElement);
    const maze = `
        <button class="btn btn-ghost btn-sm run-maze" type="button" data-action="open-maze"
                aria-label="Bored? Play the maze" title="Bored? Play the maze">
            <span class="maze-loader" aria-hidden="true"></span>
        </button>`;

    if (st.armed) {
        st.controls.innerHTML = `
            <div class="run-arm" role="group" aria-label="Confirm run">
                <span class="run-ask">Run this step for real, now, on the hub?</span>
                <button class="btn btn-primary" type="button" data-action="run-job-confirm">Yes, run it</button>
                <button class="btn btn-secondary" type="button" data-action="run-job-cancel">Cancel</button>
            </div>`;
    } else if (st.phase === 'running') {
        st.controls.innerHTML = `
            <button class="btn btn-secondary" type="button" data-action="stop-job">Stop the run</button>
            ${maze}`;
    } else {
        const again = st.lines.length || st.dropped;
        st.controls.innerHTML = `
            <button class="btn btn-primary" type="button" data-action="run-job">
                ${again ? 'Run it again' : 'Run this job now'}
            </button>`;
    }
    // Arming always takes focus (the confirm button is the point of arming).
    // Otherwise focus only moves if it was in here to begin with, so a
    // background repaint never steals it from elsewhere on the page.
    const first = st.controls.querySelector('button:not(.run-maze)')
        || st.controls.querySelector('button');
    if ((st.armed || hadFocus) && first) first.focus();
}

// A single click used to launch real code against real credentials. The
// section's own (i) warns about that; the arm/confirm step (borrowed from the
// errors section) makes the warning operative.
function armRunJob() {
    const st = runState;
    if (!st || st.phase === 'running') return;
    st.armed = true;
    paintRunControls();
}

function cancelRunJob() {
    const st = runState;
    if (!st) return;
    st.armed = false;
    paintRunControls();
}

function stopRunJob() {
    const st = runState;
    if (!st || st.phase !== 'running' || !st.controller) return;
    pushConsoleLine(st, '--- stop requested, killing the run ---', 'meta');
    st.controller.abort();
}

function clearRunConsole() {
    const st = runState;
    if (!st) return;
    st.lines = [];
    st.pending = [];
    st.dropped = 0;
    st.errorLines = 0;
    st.inTrace = false;
    st.unseen = 0;
    st.stuck = true;
    st.linesEl.textContent = '';
    st.dropEl.hidden = true;
    st.emptyEl.hidden = false;
    if (st.phase !== 'running') {
        st.phase = 'idle';
        st.elapsed = 0;
        paintConsoleState(st);
        paintRunControls();
    }
    paintConsoleCounts(st);
    paintConsoleJump(st);
    announce('Console cleared');
}

// --- The run ----------------------------------------------------------------
async function runJob() {
    const st = runState;
    if (!st || st.phase === 'running' || !st.jobId) return;

    st.armed = false;
    st.phase = 'running';
    st.controller = new AbortController();
    st.lines = [];
    st.pending = [];
    st.dropped = 0;
    st.errorLines = 0;
    st.inTrace = false;
    st.stuck = true;
    st.unseen = 0;
    st.linesEl.textContent = '';
    st.dropEl.hidden = true;
    st.startedAt = performance.now();
    st.elapsed = 0;
    st.timer = setInterval(() => paintConsoleElapsed(st), 100);
    paintRunControls();
    paintConsoleState(st);
    pushConsoleLine(st, `$ run ${st.jobName}`, 'meta');

    try {
        const response = await fetch('/run-job', {
            method: 'POST',
            body: JSON.stringify({ id: st.jobId }),
            headers: { 'Content-Type': 'application/json' },
            signal: st.controller.signal,
        });
        if (runState !== st) return;                       // page changed under us
        if (!response.ok) throw new Error(`the hub answered ${response.status}`);
        if (!response.body) throw new Error('this browser cannot stream the response');

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let tail = '';
        while (true) {
            const { done, value } = await reader.read();
            if (runState !== st) return;
            if (done) break;
            tail += decoder.decode(value, { stream: true });
            const parts = tail.split('\n');
            tail = parts.pop();
            for (const line of parts) pushConsoleLine(st, line);
            // A process that never emits a newline must not grow one string
            // until the tab dies.
            if (tail.length > CONSOLE_LINE_CAP) {
                pushConsoleLine(st, tail);
                tail = '';
            }
        }
        if (tail) pushConsoleLine(st, tail);
        finishRun(st, st.errorLines ? 'error' : 'ok');
    } catch (error) {
        if (runState !== st) return;
        if (st.controller.signal.aborted) {
            finishRun(st, 'stopped');
            return;
        }
        console.error('Error running job:', error);
        pushConsoleLine(st, `--- ${String(error && error.message || error)} ---`, 'error');
        finishRun(st, 'failed');
    }
}

function finishRun(st, phase) {
    st.phase = phase;
    st.elapsed = performance.now() - st.startedAt;
    if (st.timer) clearInterval(st.timer);
    st.timer = 0;
    if (phase === 'stopped') pushConsoleLine(st, '--- stopped ---', 'meta');
    paintConsoleElapsed(st);
    paintConsoleState(st);
    paintRunControls();
    paintConsoleJump(st);

    // The console's own badge says all this, so a toast on top of it would be
    // noise -- unless you have scrolled away from the console, which on a long
    // job is the normal thing to do. Then the outcome has nowhere else to
    // appear (#16).
    const spoken = `${CONSOLE_STATES[phase].label} after ${elapsedText(st.elapsed)}`;
    if (consoleInView(st)) {
        announce(spoken);
    } else {
        const tone = phase === 'ok' ? 'ok' : (phase === 'stopped' ? 'warn' : 'danger');
        toast(`${st.jobName}: ${CONSOLE_STATES[phase].label.toLowerCase()}`, {
            tone,
            key: 'run-outcome',
            detail: `Took ${elapsedText(st.elapsed)}.`,
            action: {label: 'Show output', name: 'scroll-to-console'},
        });
    }
}

// Is any part of the terminal actually on screen?
function consoleInView(st) {
    const el = st && st.scroll;
    if (!el || !el.isConnected) return false;
    const box = el.getBoundingClientRect();
    return box.bottom > 0 && box.top < (window.innerHeight || 0);
}

// --- Lines ------------------------------------------------------------------
// Tone is a hint, like the code highlighter -- not a log parser. A traceback
// is tracked as a block so its indented frames read as one thing.
function lineTone(st, text) {
    if (st.inTrace) {
        if (text === '' || /^\s/.test(text)) return 'error';
        st.inTrace = false;                 // the exception line closes the block
        return 'error';
    }
    if (/^Traceback \(most recent call last\)/.test(text)) {
        st.inTrace = true;
        return 'error';
    }
    if (/\b(ERROR|CRITICAL|FATAL)\b|\b\w*(Error|Exception):|\bfailed\b/.test(text)) return 'error';
    if (/\b(WARN|WARNING|Warning)\b|\bretrying\b/.test(text)) return 'warn';
    if (/\b(success|succeeded|finished|complete[d]?)\b/i.test(text)) return 'ok';
    return '';
}

function pushConsoleLine(st, text, tone) {
    const line = { text: String(text), tone: tone || lineTone(st, String(text)) };
    if (line.tone === 'error') st.errorLines++;
    st.lines.push(line);
    st.pending.push(line);
    const over = st.lines.length - CONSOLE_MAX_LINES;
    if (over > 0) {
        st.lines.splice(0, over);
        st.dropped += over;
    }
    if (!st.frame) st.frame = requestAnimationFrame(() => flushConsole(st));
}

// One DOM write per frame: 500 lines/sec through `appendChild` per line is
// what made the old <pre> stutter.
function flushConsole(st) {
    st.frame = 0;
    if (runState !== st) return;
    if (st.pending.length) {
        const frag = document.createDocumentFragment();
        for (const line of st.pending) {
            const el = mk('div', 'console-line', line.text === '' ? ' ' : line.text);
            if (line.tone) el.dataset.tone = line.tone;
            frag.appendChild(el);
        }
        const added = st.pending.length;
        st.pending = [];
        st.linesEl.appendChild(frag);
        st.emptyEl.hidden = true;
        // The ring buffer, enforced on the DOM itself: `st.lines` is already
        // capped, so drop the same overflow off the top of the rendered list.
        let over = st.linesEl.childElementCount - CONSOLE_MAX_LINES;
        while (over-- > 0 && st.linesEl.firstElementChild) {
            st.linesEl.firstElementChild.remove();
        }
        // Lines that arrived while the operator was reading further up.
        if (!st.stuck) st.unseen += added;
    }
    if (st.dropped) {
        st.dropEl.hidden = false;
        st.dropEl.textContent =
            `${num(st.dropped)} earlier line${st.dropped === 1 ? '' : 's'} dropped — `
            + `the console keeps the last ${num(CONSOLE_MAX_LINES)}.`;
    }
    paintConsoleCounts(st);
    if (st.stuck) scrollConsoleToEnd(st);
    paintConsoleJump(st);
}

// --- Auto-scroll ------------------------------------------------------------
function scrollConsoleToEnd(st) {
    st.scroll.scrollTop = st.scroll.scrollHeight;
    st.unseen = 0;
}

function onConsoleScroll(st) {
    const el = st.scroll;
    const atEnd = el.scrollHeight - el.scrollTop - el.clientHeight <= CONSOLE_TAIL_SLACK;
    if (atEnd === st.stuck) return;
    st.stuck = atEnd;
    if (atEnd) st.unseen = 0;
    paintConsoleJump(st);
}

function paintConsoleJump(st) {
    const show = !st.stuck && (st.phase === 'running' || st.lines.length > 0);
    st.jumpBtn.hidden = !show;
    if (!show) return;
    st.jumpBtn.textContent = st.unseen
        ? `↓ ${num(st.unseen)} new line${st.unseen === 1 ? '' : 's'}`
        : '↓ Jump to the end';
}

// --- Painters ---------------------------------------------------------------
function elapsedText(ms) {
    const total = Math.max(0, ms) / 1000;
    if (total < 60) return `${total.toFixed(1)}s`;
    const mins = Math.floor(total / 60);
    return `${mins}m ${String(Math.floor(total % 60)).padStart(2, '0')}s`;
}

function paintConsoleElapsed(st) {
    const ms = st.phase === 'running' ? performance.now() - st.startedAt : st.elapsed;
    st.elapsedEl.textContent = st.phase === 'idle' ? '' : elapsedText(ms);
}

function paintConsoleState(st) {
    const state = CONSOLE_STATES[st.phase] || CONSOLE_STATES.idle;
    st.stateEl.dataset.tone = state.tone;
    st.stateEl.dataset.phase = st.phase;
    st.stateText.textContent = state.label;
    paintConsoleElapsed(st);
}

function paintConsoleCounts(st) {
    const kept = st.lines.length;
    st.countEl.textContent = kept
        ? `${num(kept)} line${kept === 1 ? '' : 's'}`
        : '';
    // Nothing to copy and nothing to clear on an empty console.
    st.copyBtn.disabled = st.clearBtn.disabled = kept === 0;
}

function renderJobDetails() {
    if (!currentJob) return;

    const job = currentJob;
    const host = document.getElementById('jobDetail');
    hideTip();

    // Failure first: on a broken job it is the only thing anyone reads.
    host.innerHTML = `
        ${jobHero(job)}
        ${jobFailureSection(job)}
        ${jobLineageSection(job)}
        ${jobCodeSection(job)}
        ${jobConfigSection(job)}
        ${jobRunSection(job)}
    `;

    // Untrusted text and long payloads never travel through markup: the error
    // message is a text node, and the copy buttons get their value assigned as
    // a property instead of an attribute holding the whole file.
    setText(host, '#jobErrorMessage', job.error || '');
    if (job.trace) paintTrace(host, job.trace);

    fillCopySlot(host, 'id', job.id, 'job id');
    fillCopySlot(host, 'error', job.error, 'error message');
    fillCopySlot(host, 'trace', job.trace, 'traceback');
    fillCopySlot(host, 'code', job.code, 'job code');
    host.querySelectorAll('.job-copy-slot[data-slot="chip"]').forEach(slot => {
        slot.replaceChildren(copyButtonEl(slot.dataset.value || '', 'job id'));
    });

    // The console is DOM-built, not markup: log text is arbitrary.
    mountRunConsole(job);
}

function fillCopySlot(host, slot, value, what) {
    const target = host.querySelector(`.job-copy-slot[data-slot="${slot}"]`);
    if (!target) return;
    if (value === null || value === undefined || value === '') {
        target.remove();
        return;
    }
    target.replaceChildren(copyButtonEl(String(value), what));
}

// --- Parent tree & results (#12) --------------------------------------------
// `/job-parents-and-results` hands back one nested node per job --
// `{job, result, parents: {<id>: node}}` -- and a node can be `null`: the hub has no
// record of that parent any more, or the cycle guard cut the walk. What follows from
// that shape:
//   * The root is the job whose page you came from. It is marked "you are here", and
//     everything nested under it is upstream of it: parents, not children.
//   * Nodes are built as DOM, never markup. A retained result is an arbitrary payload
//     (megabytes, hostile strings), so it must not travel through innerHTML.
//   * Children mount on first expand, wide fan-ins page, and an enormous result waits
//     to be asked for. A 5 MB result must not freeze the dialog.

const TREE_SMALL = 16;                 // a tree this small opens all the way
const TREE_AUTO_DEPTH = 2;             // a bigger one opens this many levels
const TREE_DEEP = 8;                   // indent stops growing at this depth
const TREE_FANOUT = 12;                // parents per node before "show the rest"
const TREE_EXPAND_ALL_LIMIT = 400;     // above this, "expand all" is refused
const JSON_OPEN_DEPTH = 0;             // levels of a result open on arrival
const JSON_PAGE = 100;                 // entries per level before "show all"
const JSON_SIZE_LIMIT = 256 * 1024;    // per-branch size hints stop above this
const JSON_EAGER_LIMIT = 512 * 1024;   // and the viewer itself waits to be asked
const JSON_STRING_CAP = 180;           // characters of a string shown inline
const JSON_RAW_CAP = 200 * 1024;       // characters of pretty JSON the raw view paints

const CHEVRON = `
    <svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor"
         stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
        <path d="m9 6 6 6-6 6"/>
    </svg>`;

// Small DOM builder. Text always goes in as a text node.
function mk(tag, cls, text) {
    const node = document.createElement(tag);
    if (cls) node.className = cls;
    if (text !== undefined && text !== null) node.textContent = String(text);
    return node;
}

function twisty(label, expanded) {
    const button = mk('button', 'twisty');
    button.type = 'button';
    button.innerHTML = CHEVRON;  // constant markup, no interpolation
    button.setAttribute('aria-expanded', expanded ? 'true' : 'false');
    button.setAttribute('aria-label', label);
    return button;
}

// --- Copy buttons for values too big for an attribute ----------------------
// `copyButtonEl` parks its value in `data-copy`; a 5 MB result cannot go there. This
// one takes a function and computes the text at click time, so nothing is stringified
// until someone actually asks for it.
function lazyCopyButton(getText, what) {
    const button = mk('button', 'copy-btn');
    button.type = 'button';
    button.setAttribute('aria-label', `Copy ${what}`);
    button.innerHTML = COPY_ICONS;  // constant markup
    button.addEventListener('click', async () => {
        let text = '';
        try {
            text = String(getText() ?? '');
        } catch (error) {
            console.error('copy failed to serialize:', error);
            flashCopy(button, false, what);
            return;
        }
        flashCopy(button, await writeClipboard(text), what);
    });
    return button;
}

// --- Fetch states -----------------------------------------------------------
function treeSkeleton() {
    const rows = [0, 1, 2, 3]
        .map(i => `<div class="skel ptree-skeleton-row" data-indent="${i}"></div>`)
        .join('');
    return `
        <div class="ptree-loading" role="status">
            <p class="ptree-loading-text">Walking this job's ancestry…</p>
            <div class="ptree-skeleton" aria-hidden="true">${rows}</div>
        </div>`;
}

// Two different nothings, and the difference matters: the hub was unreachable, or the
// hub answered that it has never heard of this job.
function treeFailure(kind, detail, jobId) {
    const unreachable = kind === 'unreachable';
    return blankState({
        kind: unreachable ? 'failed' : 'empty',
        icon: unreachable ? 'failed' : 'gone',
        title: unreachable ? 'Could not reach the hub' : 'No ancestry to show',
        body: unreachable
            ? 'The dashboard asked the hub for this job’s parents and the request failed.'
              + ' The hub may be restarting, or the web app may have lost its connection to it.'
            : 'The hub has no record of this job any more. When the last step of a pipeline'
              + ' succeeds the hub clears the whole thing — its jobs and their retained'
              + ' results — so a job that has just finished has no tree left to walk.',
        detail,
        id: jobId ? {label: 'job id', value: jobId} : null,
        actions: unreachable
            ? [{label: 'Try again', name: 'show-parent-tree', primary: true}]
            : [],
    });
}

async function showParentTree() {
    if (!currentJob) return;
    const jobId = currentJob.id;

    const modal = openModal({
        key: `parent-tree:${jobId}`,
        title: 'Parent tree & results',
        size: 'lg',
        body: treeSkeleton(),
    });
    if (!modal) return;
    // Re-opened via "Try again": the handle is the one already up.
    modal.setBody(treeSkeleton());

    const answer = await getJobParentAndResults(jobId);
    // The operator may have closed it, or opened another job's tree, while we waited.
    if (!document.contains(modal.el)) return;

    if (!answer.ok) {
        modal.setBody(treeFailure('unreachable', answer.error, jobId));
    } else if (!answer.data || !answer.data.job) {
        modal.setBody(treeFailure('gone', '', jobId));
    } else {
        renderParentTree(answer.data, modal.bodyEl, jobId);
    }
    focusInside(modal.el);
}

// --- Tree -------------------------------------------------------------------
function treeStats(node) {
    const stats = { nodes: 0, depth: 0, missing: 0, results: 0, bytes: 0 };
    const walk = (n, depth) => {
        stats.nodes++;
        stats.depth = Math.max(stats.depth, depth);
        if (!n || !n.job) { stats.missing++; return; }
        if (n.result !== null && n.result !== undefined) {
            stats.results++;
            stats.bytes += jsonBytes(n.result);
        }
        Object.values(n.parents || {}).forEach(child => walk(child, depth + 1));
    };
    walk(node, 0);
    return stats;
}

function renderParentTree(root, container, rootId) {
    const stats = treeStats(root);
    const ctx = {
        rootId,
        autoDepth: stats.nodes <= TREE_SMALL ? Infinity : TREE_AUTO_DEPTH,
        sizes: stats.bytes <= JSON_SIZE_LIMIT,
        toggles: [],   // every twisty on the tree, for expand/collapse all
    };

    const facts = [
        `${num(stats.nodes)} job${stats.nodes === 1 ? '' : 's'}`,
        `${num(stats.depth + 1)} level${stats.depth === 0 ? '' : 's'}`,
        stats.results
            ? `${num(stats.results)} result${stats.results === 1 ? '' : 's'} kept (${formatBytes(stats.bytes)})`
            : 'no results retained',
    ];

    container.innerHTML = `
        <div class="ptree-head">
            <p class="ptree-intro">Every nested job is a <strong>parent</strong>: it has to finish, and hand its
                result down, before the job above it can run.</p>
            <div class="ptree-bar">
                <p class="ptree-facts">${facts.map(f => `<span>${esc(f)}</span>`).join('')}</p>
                <div class="ptree-bar-actions">
                    <button class="btn btn-ghost btn-sm" type="button" data-tree="expand">Expand all</button>
                    <button class="btn btn-ghost btn-sm" type="button" data-tree="collapse">Collapse all</button>
                </div>
            </div>
            ${stats.missing ? `<p class="ptree-warn">${esc(
                `${stats.missing} parent${stats.missing === 1 ? '' : 's'} the hub no longer holds — cleared when its pipeline finished, or cut by the cycle guard.`)}</p>` : ''}
        </div>
        <!-- role=list, not role=tree (#17). "tree" commits to the full
             treeview keyboard model -- arrows to move, Home/End, typeahead --
             and none of that is implemented here; a screen reader would put
             the user in a mode whose keys do nothing. As a nested list the
             structure still reads correctly and each node's own .twisty
             button carries the aria-expanded state that does work. -->
        <div class="ptree-scroll"><div class="ptree" role="list"></div></div>
    `;

    container.querySelector('.ptree').appendChild(treeNode(rootId, root, ctx, 0));

    const expandAll = container.querySelector('[data-tree="expand"]');
    if (stats.nodes > TREE_EXPAND_ALL_LIMIT) {
        expandAll.disabled = true;
        expandAll.dataset.tip = `${num(stats.nodes)} jobs is too many to open at once — open the branch you need.`;
    } else {
        expandAll.addEventListener('click', () => setAllTwisties(ctx, true));
    }
    container.querySelector('[data-tree="collapse"]')
        .addEventListener('click', () => setAllTwisties(ctx, false));
}

// `toggles` grows as branches mount, so "expand all" run twice opens the level it
// just revealed. One pass per click is enough to make that predictable.
function setAllTwisties(ctx, open) {
    for (let pass = 0; pass < 40; pass++) {
        const pending = ctx.toggles.filter(t => t.isOpen() !== open);
        if (!pending.length) return;
        pending.forEach(t => t.set(open));
    }
}

function treeNode(id, node, ctx, depth) {
    const el = mk('div', 'ptree-node');
    el.setAttribute('role', 'listitem');
    if (depth >= TREE_DEEP) el.dataset.deep = 'true';

    if (!node || !node.job) {
        el.classList.add('is-missing');
        el.appendChild(missingRow(id));
        return el;
    }

    const job = node.job;
    const parents = Object.entries(node.parents || {});
    const isRoot = depth === 0;
    const hasResult = node.result !== null && node.result !== undefined;

    const row = mk('div', 'ptree-row');
    if (isRoot) row.classList.add('is-current');

    const open = depth < ctx.autoDepth;
    let kids = null;
    let toggle = null;
    if (parents.length) {
        toggle = twisty(`Parents of ${job.name || 'this job'}`, open);
        row.appendChild(toggle);
    } else {
        row.appendChild(mk('span', 'twisty-spacer'));
    }

    const main = mk('div', 'ptree-main');
    const title = mk('div', 'ptree-title');
    const name = mk('span', 'ptree-name', job.name || 'unnamed job');
    name.title = String(job.name || '');
    title.appendChild(name);
    if (isRoot) title.appendChild(mk('span', 'ptree-here', 'you are here'));
    if (job.func && job.func !== job.name) {
        const func = mk('span', 'ptree-func', job.func);
        func.title = String(job.func);
        title.appendChild(func);
    }
    main.appendChild(title);

    const meta = mk('div', 'ptree-meta');
    const idEl = mk('span', 'ptree-id', shortId(job.id));
    idEl.title = String(job.id || '');
    meta.appendChild(idEl);
    meta.appendChild(copyButtonEl(String(job.id || ''), 'job id'));
    const age = timeAgo(job.created);
    if (age) meta.appendChild(mk('span', 'ptree-age', age));
    if (job.type) {
        const type = mk('span', 'ptree-type', job.type);
        type.title = String(job.type);
        meta.appendChild(type);
    }
    main.appendChild(meta);
    row.appendChild(main);

    const side = mk('div', 'ptree-side');
    if (!isRoot) {
        const found = lookupJob(job.id);
        if (found) {
            const openBtn = mk('button', 'btn btn-ghost btn-sm', 'Open');
            openBtn.type = 'button';
            openBtn.dataset.action = 'tree-open-job';
            openBtn.dataset.jobId = job.id;
            openBtn.dataset.tip = 'Leave this dialog and open that job’s page.';
            side.appendChild(openBtn);
        }
    }
    row.appendChild(side);

    el.appendChild(row);

    // The result panel: its own affordance, because "show me what this parent
    // returned" and "show me what fed this parent" are two different questions.
    const resultHost = mk('div', 'ptree-result');
    const resultBtn = mk('button', 'ptree-result-btn');
    resultBtn.type = 'button';
    if (hasResult) {
        const bytes = jsonBytes(node.result);
        resultBtn.innerHTML = CHEVRON;
        resultBtn.appendChild(mk('span', 'ptree-result-label', 'Result'));
        resultBtn.appendChild(mk('span', 'ptree-result-size',
            `${jsonKindLabel(node.result)} · ${formatBytes(bytes)}`));
        resultBtn.setAttribute('aria-expanded', 'false');
        let mounted = false;
        resultBtn.addEventListener('click', () => {
            const nowOpen = resultBtn.getAttribute('aria-expanded') !== 'true';
            resultBtn.setAttribute('aria-expanded', nowOpen ? 'true' : 'false');
            if (nowOpen && !mounted) {
                mounted = true;
                resultHost.appendChild(jsonViewer(node.result, { sizes: ctx.sizes }));
            }
            resultHost.classList.toggle('is-open', nowOpen);
        });
        resultHost.appendChild(resultBtn);
    } else {
        const none = mk('p', 'ptree-noresult', isRoot
            ? 'Nothing retained yet — this job has not produced a result.'
            : 'No result retained.');
        resultHost.appendChild(none);
    }
    el.appendChild(resultHost);

    if (parents.length) {
        kids = mk('div', 'ptree-kids');
        kids.setAttribute('role', 'list');
        el.appendChild(kids);

        let mounted = 0;
        const mount = count => {
            const slice = parents.slice(mounted, mounted + count);
            slice.forEach(([childId, child]) => {
                kids.appendChild(treeNode(childId, child, ctx, depth + 1));
            });
            mounted += slice.length;
            const more = kids.querySelector(':scope > .ptree-more');
            if (more) more.remove();
            if (mounted < parents.length) {
                const left = parents.length - mounted;
                const btn = mk('button', 'ptree-more',
                    `Show ${num(left)} more parent${left === 1 ? '' : 's'}`);
                btn.type = 'button';
                btn.addEventListener('click', () => mount(TREE_FANOUT));
                kids.appendChild(btn);
            }
        };

        const setOpen = nowOpen => {
            toggle.setAttribute('aria-expanded', nowOpen ? 'true' : 'false');
            kids.hidden = !nowOpen;
            if (nowOpen && !mounted) mount(TREE_FANOUT);
        };
        toggle.addEventListener('click', () => {
            setOpen(toggle.getAttribute('aria-expanded') !== 'true');
        });
        ctx.toggles.push({
            isOpen: () => toggle.getAttribute('aria-expanded') === 'true',
            set: setOpen,
        });
        setOpen(open);
    }

    return el;
}

// A parent the hub could not produce. The id is all there is, and it is worth showing:
// it is what you grep the logs for.
function missingRow(id) {
    const row = mk('div', 'ptree-row is-missing-row');
    row.appendChild(mk('span', 'twisty-spacer'));
    const main = mk('div', 'ptree-main');
    main.appendChild(mk('span', 'ptree-name', 'Parent the hub no longer holds'));
    const meta = mk('div', 'ptree-meta');
    const idEl = mk('span', 'ptree-id', shortId(id));
    idEl.title = String(id || '');
    meta.appendChild(idEl);
    if (id) meta.appendChild(copyButtonEl(String(id), 'job id'));
    meta.appendChild(mk('span', 'ptree-age',
        'cleared when its pipeline finished, or cut by the cycle guard'));
    main.appendChild(meta);
    row.appendChild(main);
    return row;
}

// --- JSON viewer ------------------------------------------------------------
// Hand-rolled, no dependency. Collapsed past the first level, lazy children, paged
// levels, type-colored values, a size hint, a copy button and a raw view.

function jsonType(value) {
    if (value === null) return 'null';
    if (value === undefined) return 'undefined';
    if (Array.isArray(value)) return 'array';
    if (typeof value === 'object') return 'object';
    return typeof value;
}

function jsonBytes(value) {
    try {
        const text = JSON.stringify(value);
        return typeof text === 'string' ? text.length : 0;
    } catch (error) {
        return 0;
    }
}

function jsonEntries(value) {
    return jsonType(value) === 'array'
        ? value.map((item, i) => [String(i), item])
        : Object.entries(value);
}

function jsonKindLabel(value) {
    const type = jsonType(value);
    if (type === 'array') return `${num(value.length)} item${value.length === 1 ? '' : 's'}`;
    if (type === 'object') {
        const n = Object.keys(value).length;
        return `${num(n)} key${n === 1 ? '' : 's'}`;
    }
    if (type === 'string') {
        return `${num(value.length)} character${value.length === 1 ? '' : 's'}`;
    }
    return type;
}

function jsonScalarText(value) {
    const type = jsonType(value);
    if (type === 'string') return JSON.stringify(value);
    if (type === 'undefined') return 'undefined';
    return String(value);
}

function jsonPretty(value) {
    const text = JSON.stringify(value, null, 2);
    return typeof text === 'string' ? text : String(value);
}

function jsonViewer(value, options) {
    const opts = options || {};
    const host = mk('div', 'jsonv');
    const bytes = jsonBytes(value);

    const bar = mk('div', 'jsonv-bar');
    const rawBtn = mk('button', 'btn btn-ghost btn-sm', 'Raw');
    rawBtn.type = 'button';
    rawBtn.setAttribute('aria-pressed', 'false');
    bar.appendChild(rawBtn);
    bar.appendChild(lazyCopyButton(() => jsonPretty(value), 'result JSON'));
    host.appendChild(bar);

    const tree = mk('div', 'jsonv-tree');
    const raw = mk('pre', 'jsonv-raw');
    raw.hidden = true;
    let rawPainted = false;
    rawBtn.addEventListener('click', () => {
        const showRaw = raw.hidden;
        if (showRaw && !rawPainted) {
            rawPainted = true;
            const text = jsonPretty(value);
            raw.textContent = text.length > JSON_RAW_CAP
                ? `${text.slice(0, JSON_RAW_CAP)}\n\n… ${num(text.length - JSON_RAW_CAP)} more characters. Copy it to see the rest.`
                : text;
        }
        raw.hidden = !showRaw;
        tree.hidden = showRaw;
        rawBtn.setAttribute('aria-pressed', showRaw ? 'true' : 'false');
    });
    host.appendChild(tree);
    host.appendChild(raw);

    // Enormous payloads are gated: painting 40,000 rows on open would freeze the
    // dialog, and nobody reads a 5 MB result by scrolling anyway.
    if (bytes > JSON_EAGER_LIMIT) {
        const gate = mk('div', 'jsonv-gate');
        gate.appendChild(mk('p', 'jsonv-gate-text',
            `${formatBytes(bytes)} of retained JSON — ${jsonKindLabel(value)}. Rendering it will be slow.`));
        const show = mk('button', 'btn btn-secondary btn-sm', 'Show it anyway');
        show.type = 'button';
        show.addEventListener('click', () => {
            gate.remove();
            jsonMount(tree, value, { ...opts, sizes: false });
        });
        gate.appendChild(show);
        host.appendChild(gate);
        return host;
    }

    jsonMount(tree, value, opts);
    return host;
}

// The root level, always open. A scalar result has no level to open, so it renders as
// one value row.
function jsonMount(tree, value, opts) {
    const type = jsonType(value);
    if (type !== 'object' && type !== 'array') {
        tree.appendChild(jsonLeaf(null, value, opts));
        return;
    }
    jsonLevel(tree, value, opts, 0);
}

function jsonLevel(host, value, opts, depth) {
    const entries = jsonEntries(value);
    if (!entries.length) {
        host.appendChild(mk('p', 'jsonv-empty',
            jsonType(value) === 'array' ? 'empty list' : 'empty object'));
        return;
    }

    let shown = 0;
    const paint = count => {
        const slice = entries.slice(shown, shown + count);
        slice.forEach(([key, child]) => host.appendChild(jsonEntry(key, child, opts, depth)));
        shown += slice.length;
        const more = host.querySelector(':scope > .jsonv-more');
        if (more) more.remove();
        if (shown < entries.length) {
            const left = entries.length - shown;
            const btn = mk('button', 'jsonv-more', `Show all ${num(entries.length)} — ${num(left)} hidden`);
            btn.type = 'button';
            btn.addEventListener('click', () => paint(left));
            host.appendChild(btn);
        }
    };
    paint(JSON_PAGE);
}

function jsonEntry(key, value, opts, depth) {
    const type = jsonType(value);
    if (type !== 'object' && type !== 'array') return jsonLeaf(key, value, opts);

    const node = mk('div', 'jsonv-node');
    const line = mk('div', 'jsonv-line');
    // The first level of a result is listed; anything nested inside it waits to be
    // opened. A retained result is often one key wrapping a 400-row table, and
    // unrolling that on arrival buries the shape of the payload.
    const open = depth < JSON_OPEN_DEPTH;
    const toggle = twisty(`Expand ${key === null ? 'result' : key}`, open);
    line.appendChild(toggle);
    if (key !== null) line.appendChild(jsonKeyEl(key));
    line.appendChild(mk('span', 'jsonv-brace', type === 'array' ? '[ ]' : '{ }'));
    line.appendChild(mk('span', 'jsonv-count', jsonKindLabel(value)));
    if (opts.sizes) {
        const bytes = jsonBytes(value);
        if (bytes > 2048) line.appendChild(mk('span', 'jsonv-size', formatBytes(bytes)));
    }
    node.appendChild(line);

    const kids = mk('div', 'jsonv-kids');
    node.appendChild(kids);
    let mounted = false;
    const setOpen = nowOpen => {
        toggle.setAttribute('aria-expanded', nowOpen ? 'true' : 'false');
        kids.hidden = !nowOpen;
        if (nowOpen && !mounted) {
            mounted = true;
            jsonLevel(kids, value, opts, depth + 1);
        }
    };
    toggle.addEventListener('click', () => setOpen(toggle.getAttribute('aria-expanded') !== 'true'));
    // The whole line is the hit target: a 14px chevron is not one.
    line.addEventListener('click', event => {
        if (event.target.closest('button')) return;
        setOpen(toggle.getAttribute('aria-expanded') !== 'true');
    });
    setOpen(open);
    return node;
}

function jsonKeyEl(key) {
    const numeric = /^\d+$/.test(key);
    const el = mk('span', numeric ? 'jsonv-key is-index' : 'jsonv-key', key);
    el.appendChild(mk('span', 'jsonv-colon', numeric ? '' : ':'));
    return el;
}

function jsonLeaf(key, value, opts) {
    const row = mk('div', 'jsonv-row');
    row.appendChild(mk('span', 'twisty-spacer'));
    if (key !== null) row.appendChild(jsonKeyEl(key));

    const type = jsonType(value);
    const text = jsonScalarText(value);
    const val = mk('span', `jsonv-val is-${type}`);
    if (type === 'string' && text.length > JSON_STRING_CAP) {
        // Long strings (a CSV blob, a base64 payload) are cut, not wrapped: one row
        // per entry is what makes a list of 400 readable.
        val.textContent = `${text.slice(0, JSON_STRING_CAP)}…`;
        val.title = value.length > 4000 ? `${value.slice(0, 4000)}…` : value;
        const grow = mk('button', 'jsonv-grow', `+${num(text.length - JSON_STRING_CAP)} chars`);
        grow.type = 'button';
        grow.addEventListener('click', () => {
            val.classList.add('is-full');
            val.textContent = text;
            grow.remove();
        });
        row.appendChild(val);
        row.appendChild(grow);
    } else {
        val.textContent = text;
        row.appendChild(val);
    }
    return row;
}

// --- Live state: auto-refresh, staleness, connection (#14) ------------------
// One 250ms ticker owns everything time-based on the header: the "updated 12s
// ago" label, the ring counting down to the next poll, and the poll itself.
// There is no second setInterval firing refreshes behind its back.
//
// Honesty rules this block. The label never claims data is fresher than the
// last successful /data; a failed poll does not clear the screen, it dims it
// and says so; and a paused clock says "paused" instead of quietly ageing.

const REFRESH_MS = 30 * 1000;   // normal cadence
const RETRY_MS = 5 * 1000;      // while disconnected: retry harder, visibly
const STALE_MS = 90 * 1000;     // "this is old" threshold for the label
const RING_R = 9;
const RING_CIRCUM = 2 * Math.PI * RING_R;

let autoRefresh = localStorage.getItem('autoRefresh') !== 'false';
let connected = null;           // null until the first attempt resolves
let lastSuccess = null;         // ms epoch of the last good /data
let pollWindow = REFRESH_MS;    // the interval the ring is currently drawing
let nextPollAt = Date.now() + REFRESH_MS;
let refreshInFlight = false;

// "12s" / "4m" / "2h" -- short, because it sits inside a pill.
function shortAge(ms) {
    const s = Math.max(0, Math.round(ms / 1000));
    if (s < 60) return `${s}s`;
    if (s < 3600) return `${Math.round(s / 60)}m`;
    return `${Math.round(s / 3600)}h`;
}

// The same age, spelled out for the screen reader and the banner.
function longAge(ms) {
    const s = Math.max(0, Math.round(ms / 1000));
    if (s < 10) return 'moments ago';
    if (s < 60) return `${s} seconds ago`;
    const m = Math.round(s / 60);
    if (m < 60) return `${m} minute${m === 1 ? '' : 's'} ago`;
    const h = Math.round(m / 60);
    return `${h} hour${h === 1 ? '' : 's'} ago`;
}

function scheduleNextPoll() {
    pollWindow = connected === false ? RETRY_MS : REFRESH_MS;
    nextPollAt = Date.now() + pollWindow;
}

// Called by initApp() with the outcome of every /data attempt.
function markConnected(ok) {
    const was = connected;
    connected = ok;
    if (ok) lastSuccess = Date.now();
    document.body.classList.toggle('is-disconnected', !ok);
    // Only dim what is actually last-known data; a cold start has nothing to dim.
    document.body.classList.toggle('has-data', !!currentData);
    if (was !== ok && was !== null) {
        // No announce() here (#17): #connBanner is a role="status" region and
        // its text is set in the same tick, so a screen reader said this twice.
    }
    // The first failure with nothing on screen still needs something on screen
    // -- and it must not be left looking like it is still loading, which is
    // what the shimmering skeleton would say (#16).
    if (!ok && !currentData) {
        ledgerPrev = null;
        const ledger = document.getElementById('ledger');
        if (ledger) {
            ledger.innerHTML = blankState({
                kind: 'failed',
                title: 'Could not reach the hub',
                body: 'The dashboard is served by the web app; the counts come from the hub'
                    + ' behind it. That request is failing, so there is nothing to show yet'
                    + ' — this is not an empty queue.',
                actions: [{label: 'Try again', name: 'refresh', primary: true}],
                frame: true,
            });
        }
        const grid = document.getElementById('workersGrid');
        // Same reason: three shimmering cards would promise a fleet that is
        // never going to arrive.
        if (grid) grid.innerHTML = blankState({
            kind: 'failed',
            title: 'Worker list unavailable',
            body: 'The hub is unreachable, so the dashboard cannot say which workers'
                + ' are connected. They may well be running.',
            frame: true,
        });
        setSectionMeta('workers', 'unknown');
    }
    paintLiveState();
}

function setRefreshBusy(busy) {
    const btn = document.getElementById('dash-refresh');
    if (!btn) return;
    const label = btn.querySelector('.btn-label');
    const loader = btn.querySelector('.crazy-eyes-loader');
    // Swap two children instead of blowing away innerHTML: the old code reset
    // the button with innerText and permanently lost the loader markup.
    if (label) label.hidden = busy;
    if (loader) loader.hidden = !busy;
    btn.disabled = busy;
    btn.setAttribute('aria-busy', busy ? 'true' : 'false');
}

function paintLiveState() {
    const pill = document.getElementById('liveToggle');
    const label = document.getElementById('liveLabel');
    const ring = document.getElementById('liveRing');
    if (!pill || !label || !ring) return;

    const now = Date.now();
    const age = lastSuccess === null ? null : now - lastSuccess;
    const remaining = Math.max(0, nextPollAt - now);
    const down = connected === false;
    const waiting = connected === null && lastSuccess === null;
    // Time travel stops the poll (#20). The pill has to say so, and pressing
    // it has to be the way back -- an operator who paused the clock by
    // stepping through history should not have to find another button.
    const browsing = inHistory();

    pill.classList.toggle('is-paused', browsing || (!autoRefresh && !down));
    pill.classList.toggle('is-history', browsing);
    pill.classList.toggle('is-down', down && !browsing);
    pill.classList.toggle('is-stale', !browsing && !down && age !== null && age > STALE_MS);
    pill.setAttribute('aria-pressed', browsing || !autoRefresh ? 'true' : 'false');
    pill.title = browsing
        ? 'Return to live'
        : (autoRefresh ? 'Pause auto-refresh' : 'Resume auto-refresh');

    let text;
    if (browsing) text = 'paused · history';
    else if (waiting && refreshInFlight) text = 'connecting…';
    else if (down) text = 'reconnecting…';
    else if (age === null) text = 'no data yet';
    else if (!autoRefresh) text = `paused · ${shortAge(age)} old`;
    else text = `updated ${shortAge(age)} ago`;
    if (label.textContent !== text) label.textContent = text;

    // Ring: the filled arc is the time LEFT before the next poll. Paused or
    // disconnected it stops draining -- a moving ring that is not counting
    // down to anything would be a lie.
    const counting = autoRefresh && !down && !refreshInFlight && !browsing;
    const fraction = counting ? Math.min(1, remaining / pollWindow) : 1;
    ring.style.strokeDasharray = `${RING_CIRCUM}`;
    ring.style.strokeDashoffset = `${RING_CIRCUM * (1 - fraction)}`;

    const cursor = browsing ? historySample(historyIndex) : null;
    const aria = browsing ? [
        'Auto-refresh paused while browsing history',
        `the counts on screen are from ${cursor ? historyClock(cursor.ts) : 'earlier'}`,
        'activate to return to live',
    ].join(', ') : [
        autoRefresh ? 'Auto-refresh on' : 'Auto-refresh paused',
        age === null ? 'no data loaded yet' : `data updated ${longAge(age)}`,
        down ? 'the hub is unreachable, retrying' : (autoRefresh ? `next update in ${Math.ceil(remaining / 1000)} seconds` : ''),
        autoRefresh ? 'activate to pause' : 'activate to resume',
    ].filter(Boolean).join(', ');
    pill.setAttribute('aria-label', aria + '.');

    // Banner: persistent while down, and it says how old the visible data is.
    const banner = document.getElementById('connBanner');
    const bannerText = document.getElementById('connText');
    if (banner && bannerText) {
        banner.hidden = !down;
        if (down) {
            const every = `retrying every ${Math.round(RETRY_MS / 1000)}s`;
            bannerText.textContent = currentData
                ? `Disconnected from the hub — ${every}. Showing the last data received, ${longAge(age)}.`
                : `Cannot reach the hub — ${every}.`;
        }
    }
}

function toggleAutoRefresh() {
    autoRefresh = !autoRefresh;
    localStorage.setItem('autoRefresh', autoRefresh ? 'true' : 'false');
    if (autoRefresh) scheduleNextPoll();
    paintLiveState();
    announce(autoRefresh ? 'Auto-refresh resumed' : 'Auto-refresh paused');
}

function liveTick() {
    // Browsing history freezes the poll outright (#20) -- not the setting, so
    // leaving history resumes whatever the operator had chosen.
    const due = autoRefresh && !refreshInFlight && !inHistory() && Date.now() >= nextPollAt;
    // Auto-refresh is a dashboard behaviour: the worker and job pages hold a
    // snapshot the operator is reading. Off-dashboard the clock keeps running
    // but the poll is skipped, so coming back triggers one immediately.
    if (due) {
        if (document.querySelector('#page1.active')) refreshDashboard();
        else scheduleNextPoll();
    }
    paintLiveState();
    // One clock for everything time-based: the history bar's "2h 10m ago" and
    // the empty table's countdown to the next sample ride along here rather
    // than starting timers of their own.
    paintHistoryClock();
    paintHistoryCountdown();
}

// Initialize app
async function initApp() {
    const data = await getData();
    if (!data) {
        markConnected(false);
        return;
    }
    try {
        currentData = data;
        markConnected(true);
        // Failures are their own section, not a fake worker. Only ask the
        // hub for them when it says there are some.
        errorJobs = [];
        errorsFailure = '';
        const claimed = Number(currentData.counts?.errors) || 0;
        if (claimed > 0) {
            const answer = await getErrorData();
            if (!answer.ok) {
                // The count says there are failures and we cannot show them.
                // Say exactly that, rather than rendering a healthy hub.
                errorsFailure = answer.error;
            } else if (answer.data && answer.data.jobs && answer.data.jobs.length > 0) {
                // `/errors` returns two index-aligned lists.
                const {jobs, errors} = answer.data;
                errorJobs = jobs.map((job, index) => ({
                    ...job,
                    error: errors?.[index]?.error,
                    trace: errors?.[index]?.trace,
                    worker_name: errors?.[index]?.worker_name
                }));
            }
        }
        // One sample per successful poll, then render, then paint the series
        // over it. The order matters: the trend has to include the numbers the
        // ledger is about to show, or the sparkline stops one poll short.
        // renderLedger stays a pure function of `counts` -- the trend is
        // painted into the empty slots it leaves (#4's contract, #18's use).
        recordTrend(currentData.counts);
        // The server's own series (#19), which is what the sparklines read
        // when it is there. Never allowed to break the poll: a hub older than
        // #19, or a broken sampler, just leaves the client buffer in charge.
        try {
            await loadHistory();
        } catch (error) {
            console.error('history load failed:', error);
        }
        // Time travel owns the dashboard while it is on: repainting it with
        // live numbers under a banner that says 14:35 is the one misreading
        // this feature exists to prevent (#20).
        if (inHistory()) renderHistoryView();
        else renderLive();
        // Only when the series actually grew: rebuilding the table under
        // someone who is reading it costs them their place.
        if (historyChanged) {
            historyChanged = false;
            refreshHistoryModal();
        }

        // A deep link that arrived before there was any data to resolve it
        // against (a cold load, or a load while the hub was down) gets its
        // one chance to open now. #15.
        if (pendingRoute) applyRoute(pendingRoute);
    } catch (error) {
        // A throw in here leaves the page half-painted while the header
        // cheerfully reports "updated 0s ago". Never silent again (#16).
        console.error('Error initializing app:', error);
        toast('The dashboard could not finish rendering', {
            tone: 'danger',
            key: 'render-failed',
            detail: String(error && error.message ? error.message : error),
            action: {label: 'Reload', name: 'reload-page'},
        });
    }
}

// Paint the dashboard from the last successful /data. Split out of initApp so
// leaving history mode (#20) has exactly one way back to the live view, rather
// than a second copy of this that can drift.
function renderLive(opts) {
    if (!currentData) return;
    const claimed = Number(currentData.counts && currentData.counts.errors) || 0;
    renderLedger(currentData.counts, opts);
    paintLedgerTrends(document.getElementById('ledger'));
    updateDocTitle();
    if (errorsFailure) renderErrorsFailure(claimed, errorsFailure);
    else renderErrors(errorJobs);
    renderWorkers(currentData.workers, currentData.counts);
}

// Refresh Dashboard
async function refreshDashboard() {
    if (refreshInFlight) return;
    refreshInFlight = true;
    setRefreshBusy(true);
    paintLiveState();
    try {
        // The crazy eyes are the point of pressing the button; hold them long
        // enough to be seen even when the hub answers in 8ms.
        await Promise.all([initApp(), new Promise(r => setTimeout(r, 600))]);
    } finally {
        refreshInFlight = false;
        setRefreshBusy(false);
        scheduleNextPoll();
        paintLiveState();
    }
}

// --- Delegated event handling -----------------------------------------------
// No inline onclick= anywhere: every interactive element declares a
// `data-action` (plus `data-worker-id` / `data-job-id` / `data-page` as needed)
// and one delegated listener per container dispatches it. That keeps untrusted
// ids out of executable attribute values entirely.
const ACTIONS = {
    // Navigation goes through the router (#15): the handler writes the hash,
    // the hashchange handler renders. Never call selectWorker/selectJob here.
    'nav-page': el => navPage(Number(el.dataset.page)),
    'select-worker': el => navigate({ page: 2, workerId: el.dataset.workerId }),
    'select-job': el => navigate({ page: 3, jobId: el.dataset.jobId }),
    'show-parent-tree': () => showParentTree(),
    // A parent in the tree that is still held or parked has a page of its own;
    // going there means leaving the dialog first.
    'tree-open-job': el => {
        if (!lookupJob(el.dataset.jobId)) return;
        while (modalStack.length) closeModal();
        navigate({ page: 3, jobId: el.dataset.jobId });
    },
    'run-job': () => armRunJob(),
    'run-job-confirm': () => runJob(),
    'run-job-cancel': () => cancelRunJob(),
    'stop-job': () => stopRunJob(),
    'scroll-to-console': () => {
        if (runState && runState.scroll) {
            runState.scroll.scrollIntoView({block: 'center', behavior: 'smooth'});
        }
    },
    'open-maze': () => window.open('/static/maze.html', '_blank'),
    'close-modal': () => closeModal(),
    'show-help': el => showHelp(el.dataset.help),
    'show-metric': el => showHelp('status', el.dataset.metric),
    // Pressing Refresh while browsing history means "show me now", so it
    // leaves history first rather than refreshing data you cannot see.
    'refresh': () => { exitHistory(); refreshDashboard(); },
    // --- History (#20) ---
    'history-table': () => showHistoryTable(),
    'history-step': el => stepHistory(Number(el.dataset.step) || 0),
    'history-live': () => exitHistory(),
    'history-goto': el => {
        // The table is the index, the ledger is the reader: jumping closes it.
        gotoHistory(Number(el.dataset.index));
        const open = modalStack.find(m => m.key === 'history');
        if (open) closeModal(open);
    },
    'history-copy-csv': el => copyHistoryCsv(el),
    'history-download-csv': () => downloadHistoryCsv(),
    'history-reload': async () => {
        await loadHistory({full: true});
        refreshHistoryModal();
    },
    'reload-page': () => window.location.reload(),
    'dismiss-toast': el => dismissToast(el.closest('.toast')),
    'toggle-auto': () => { if (inHistory()) exitHistory(); else toggleAutoRefresh(); },
    'toggle-theme': () => toggleTheme(),
    'retry': () => refreshDashboard(),
    // The dangling-deep-link page: poll, then re-resolve the same URL, so a job
    // that has come back opens instead of just re-reporting itself missing.
    // Pressing it used to re-render an identical page whether the job had come
    // back or not, which is indistinguishable from a dead button (#16). Now it
    // shows it is working, and says so when the answer has not changed.
    'recheck-route': async el => {
        const was = currentMissing;
        const label = el.textContent;
        el.disabled = true;
        el.textContent = 'Checking…';
        try {
            await refreshDashboard();
            applyRoute(parseRoute(window.location.hash));
        } finally {
            // A route that resolved replaced this button; only restore a live one.
            if (el.isConnected) { el.disabled = false; el.textContent = label; }
        }
        if (was && currentMissing && currentMissing.id === was.id) {
            toast(`The hub still has no ${was.kind === 'worker' ? 'worker' : 'job'} by that id`, {
                tone: 'info',
                key: 'recheck-route',
                detail: 'Nothing changed since the last check.',
            });
        }
    },
    'select-error-job': el => navigate({ page: 3, jobId: el.dataset.jobId }),
    'sort-jobs': el => sortJobs(el.dataset.key),
    'jobs-page': el => turnJobsPage(el.dataset.page),
    'clear-jobs-filter': () => clearJobsFilter(),
    'copy': el => copyValue(el),
    'toggle-error': el => toggleErrorGroup(el.dataset.errorKey),
    'show-all-errors': () => { errorsShowAll = true; renderErrors(errorJobs); },
    'show-all-workers': () => showAllWorkers(),
    'reset-errors': () => armErrorReset(),
    'reset-errors-cancel': () => cancelErrorReset(),
    'reset-errors-confirm': () => confirmErrorReset(),
};

function activateAction(container, target) {
    const el = target.closest('[data-action]');
    // closest() can walk past the container; only act on our own subtree.
    if (!el || !container.contains(el)) return false;
    const handler = ACTIONS[el.dataset.action];
    if (!handler) return false;
    handler(el);
    return true;
}

function delegateClicks(container) {
    if (!container) return;
    container.addEventListener('click', e => activateAction(container, e.target));
    // Cards and breadcrumbs are divs/spans, so give them the keyboard
    // activation a real <button> would have had.
    container.addEventListener('keydown', e => {
        if (e.key !== 'Enter' && e.key !== ' ') return;
        const el = e.target.closest('[data-action]');
        if (!el || el.tagName === 'BUTTON' || el.tagName === 'A') return;
        if (activateAction(container, e.target)) e.preventDefault();
    });
}

// Modals get their own delegated listener when openModal() creates them.
['header', '#page1', '#page2', '#page3']
    .map(selector => document.querySelector(selector))
    .forEach(delegateClicks);


// Esc closes the topmost modal, and once nothing is stacked it is the way out
// of history mode (#20). Backdrop clicks are bound per modal.
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        hideTip();
        if (modalStack.length) {
            e.preventDefault();
            closeModal();
        } else if (inHistory()) {
            e.preventDefault();
            exitHistory();
        }
    }
});

// ← → Home End walk the recorded series. Every guard is inside
// `historyKeysAllowed`, and the key is only claimed once they all pass.
document.addEventListener('keydown', onHistoryKey);

// Back/forward and any hand-edited URL both arrive here; a hash-only history
// entry fires hashchange, not popstate. This is the only place pages change.
window.addEventListener('hashchange', () => applyRoute(parseRoute(window.location.hash)));

// Initialize on load
document.addEventListener('DOMContentLoaded', () => {
    initTheme();
    mountSectionHeaders();
    // Shapes before data, so the ledger and the worker grid do not jump when
    // the first /data lands (#16). Painted here rather than sitting in
    // index.html so there is exactly one definition of the ledger's frame.
    showDashboardSkeleton();
    paintLiveState();
    // Park whatever URL we were opened with (initApp replays it once data
    // lands) and normalise a bare "/" to "#/" without adding a history entry,
    // so Back from the first click has somewhere honest to return to.
    pendingRoute = parseRoute(window.location.hash);
    if (pendingRoute.page === 1) pendingRoute = null;
    if (!window.location.hash) history.replaceState(null, '', '#/');
    refreshDashboard();
    // One clock for the countdown ring, the age label, and the poll itself.
    setInterval(liveTick, 250);
});
