# d-core-chaos-cockpit — front-end plan and conventions

The SPA that sits in front of the BFF: what it is built with, how it is structured, and the
rules that keep it from turning into the usual React sprawl.

Status: **plan, no code**. Companion to [chaos-cockpit.md](chaos-cockpit.md), which covers the
product and the backend. This document covers only the browser application.

---

## 1. Why this document exists

Two goals, both measurable:

1. **Lean.** A control room is a handful of screens over a small API. Lean means *few concepts,
   few dependencies, small surface* — not the smallest possible byte count. A few engineers on
   desktop, usually on a LAN or a VPN to staging, will not notice 250 kB versus 500 kB.

   Budget: **500 kB gzip for the initial route**, with a **working target of ~250 kB**. The
   ceiling is a tripwire (a dependency that doubles the bundle gets argued about), the target is
   a nudge. Neither is a design constraint: when size and capability conflict, capability wins
   and the delta is written down.
2. **Evolvable.** The cockpit will outlive its first author. Someone must be able to add a screen
   without learning a framework-within-a-framework, and without breaking three others.

Those goals are in tension with the usual React default — component libraries that bring their own
design system, state managers, and folder conventions. This document resolves that tension with a
small number of decisions and a short list of rules, each of which has an enforcement mechanism in
§12.

The rule of this document: **a convention that is not enforced is a wish.** Everything in §5 is
mapped to a lint rule, a CI gate, a type, or a review checklist item.

---

## 2. The shape of the application

| Dimension | Reality |
| :-- | :-- |
| Screens | ~8 (control room, composer, active faults, drills, drill run, scenarios, timeline, history) |
| What dominates | forms (fault composer), tables (runs, history, faults), live status (grid, drill run) |
| Interactivity | request/response + polling; one long-lived view (drill run) |
| Users | a few engineers, desktop, keyboard-heavy, often at night |
| Lifetime | years; maintained by whoever owns staging |
| Team | one engineer at a time, reviewed by another |

Nothing here needs a client-side router with loaders, a global store, offline support, optimistic
mutations, or a design system. Everything here needs **fast tables, honest status, and forms that
cannot inject a fault by accident**.

The UI is dark-first: it is a control room, often used for long sessions, and status colour must
read at a glance.

---

## 3. Decision record — component library

**Decision: shadcn/ui** (Radix primitives + Tailwind CSS + `class-variance-authority`), with
TanStack Table for data grids.

### Why

| Criterion | shadcn/ui | Mantine | MUI (the POC's stack) |
| :-- | :-- | :-- | :-- |
| What we own | the component source (copied into the repo) | a dependency | a dependency |
| Lean by construction | yes — only components we add exist | mostly — big shared runtime | no — theme engine + emotion |
| Design control | total (it is our code) | theme tokens, some overrides | override-heavy |
| Upgrade cost | manual re-copy, only when we choose | major-version migrations | major-version migrations |
| Speed to first screen | medium | **fastest** (forms, dates, notifications built in) | fast |
| Accessibility | Radix (excellent) | good | good |
| Lock-in risk | low | medium | high |
| Fits "strict conventions" | yes — primitives are generated, features compose them | medium — library conventions compete with ours | low |

The deciding factor is not aesthetics. It is that **the components become our code**, which is
exactly what the "reuse proper abstractions" rule (§6) needs: we can shape `<ConfirmDestructive>`,
`<Countdown>` and `<StatusBadge>` as real primitives instead of wrapping a third-party theme and
hoping it composes.

Mantine wins on day one — it has the modal, notification and date components we would otherwise
assemble. It loses on the constraints this document is written for: a large runtime we do not
control, and a second set of conventions competing with ours.

MUI is rejected for a different reason: the POC's stack is not a precedent. Its idiom (sx props,
theme callbacks) is the opposite of "styling lives in tokens and variants", and its theme engine is
a second styling system competing with ours. The bundle size is a symptom, not the argument — this
decision does not reopen when the budget changes (§11.1).

### Consequences we accept

- We assemble some widgets ourselves: data table (TanStack Table), toasts (`sonner`), dates
  (`react-day-picker` if needed — the cockpit mostly formats, not picks).
- Upgrading a component is a manual act, done deliberately.
- `components/ui` is generated code and is **not hand-edited** (rule R7); customisation lives in
  feature components and variants.

### What would change this decision

- If first delivery has to be dramatically faster than the 12-day MVP, Mantine is the fallback;
  §4–§12 do not change, only §3 and the component imports.
- If the team grows to several front-end engineers with a designer, a real design system would
  replace shadcn rather than extend it.

---

## 4. Architecture

```
┌──────────────────────────────────────────────────────────────┐
│ App shell                                                     │
│  app/providers.tsx   QueryClient, theme, error boundary       │
│  app/router.tsx      routes, lazy per screen                  │
│  app/layout.tsx      app bar (engine, kill switch, presence)  │
├──────────────────────────────────────────────────────────────┤
│ features/<feature>/                                           │
│   <Feature>Page.tsx  screen: composes components + hooks      │
│   components/        feature-specific, presentational         │
│   hooks/             useX: data, mutations, derived state     │
│   api.ts             query/mutation functions (typed)         │
│   schema.ts          zod schemas for this feature's payloads  │
├──────────────────────────────────────────────────────────────┤
│ components/ui/       shadcn primitives (generated, untouched) │
│ components/          shared app primitives (§6)               │
│ lib/                 api client, query keys, formatters, types│
└──────────────────────────────────────────────────────────────┘
```

### Data layer

- **TanStack Query** is the only way to read or mutate server state. No `useEffect` fetching, no
  `fetch` in components.
- One typed client in `lib/api/`: `apiGet/apiPost/apiDelete` wrapping `fetch`, attaching the CSRF
  header, parsing `{error: {code, message}}`, throwing `ApiError` with a stable `code`.
- Every response is validated with **zod** at the boundary (`schema.parse`). A BFF change that the
  SPA did not expect fails loudly in development instead of rendering `undefined`.
- **Query keys** live in one factory (`lib/queryKeys.ts`): `keys.overview()`, `keys.faults()`,
  `keys.drillRun(id)`. No ad-hoc arrays in components.
- Mutations invalidate precisely: `onSuccess` invalidates the keys the mutation can affect.
  Optimistic updates are **not** used for fault or drill lists (the engine is the truth; §7 of the
  cockpit plan).

### Polling discipline

| View | Interval | Notes |
| :-- | :-- | :-- |
| Control Room overview | 2 s | single request; the BFF caches |
| Active faults | 2 s | |
| Drill run (live) | 1 s | only while a drill is running |
| Runs list / history | 10 s | |

- Polling pauses when `document.hidden` and resumes on focus (one shared `useVisibility` hook).
- Only **mounted** panels poll; leaving a screen cancels its queries.
- A panel shows *last updated* and an explicit **stale** state when a poll fails; it never keeps
  showing old data as if it were current.

### Forms

- **react-hook-form + zod resolver**. The schema is the same one used to validate the API response
  shape, where applicable.
- Validation is client-side *and* server-side (the BFF enforces TTL bounds; the UI merely fails
  fast).
- Every destructive form uses `<ConfirmDestructive>` (§6): type the target name, not a checkbox.

### Error model

- Route-level error boundary; a failed screen must not white-screen the app bar (the kill switch
  must remain reachable).
- `ApiError.code` drives the message: `forbidden`, `drill-preflight-failed`, `engine-unreachable`.
- Mutations surface errors as a toast **and** an inline message on the form; never a silent failure.
- Unknown codes render the raw code plus "report this" — visible, not swallowed.

### Routing

- `react-router` with lazy routes; the drill run route is deep-linkable (`/drills/:id`).
- **URL is the source of truth for filters and selection** (fault filters, experiment selection,
  drill run id) so a control room state can be shared as a link. No filter state in a store.
- A running drill's URL is stable and shareable; the guard against two operators is the server's
  (one drill at a time), not the router's.

---

## 5. Conventions (the strict part)

MUST rules are enforced in CI or review; SHOULD rules are guidance with a comment required when
deviating.

### Code shape

| # | Rule | Level |
| :-- | :-- | :-- |
| R1 | One component per file; named export; file name = component name (kebab-case file, PascalCase component). | MUST |
| R2 | A component file stays under **150 lines**. Past that, extract a child component or a hook. | MUST |
| R3 | Components are presentational: no `fetch`, no business rules, no `Date.now()` arithmetic. Logic lives in `hooks/`. | MUST |
| R4 | A feature never imports from another feature. Shared code moves to `components/` or `lib/`. | MUST |
| R5 | No default exports in `features/`, `components/`, `lib/` (router route modules may use them). | MUST |
| R6 | No `any`. `unknown` at the boundary, narrowed by zod. `strict` and `noUncheckedIndexedAccess` on. | MUST |
| R7 | `components/ui/**` is generated: never edit it by hand; compose it in `components/` or features. | MUST |
| R8 | One hook per concern in `features/<f>/hooks/`; hooks return data + state, never JSX. | SHOULD |
| R9 | No barrel files (`index.ts` re-exports) outside `components/ui`. They hide cycles and defeat code splitting. | MUST |
| R10 | Dead code is deleted, not commented out. | MUST |

### Data and state

| # | Rule | Level |
| :-- | :-- | :-- |
| R11 | Server state lives in TanStack Query only. No `useState` copies of server data. | MUST |
| R12 | No global store. If one is ever needed, it is Zustand, one file, with a comment stating why. | MUST |
| R13 | Every API call goes through `lib/api`; no `fetch` elsewhere. | MUST (lint) |
| R14 | Query keys come from the factory; no inline arrays. | MUST (review) |
| R15 | Every screen renders the five states: loading, empty, error, stale, forbidden. | MUST |
| R16 | Times and durations are formatted by `lib/format` (`8.2 s`, `in 4:31`, `2 min ago`) — never inline. | MUST |

### Styling

| # | Rule | Level |
| :-- | :-- | :-- |
| R17 | Design tokens only: Tailwind theme + CSS variables. No arbitrary values (`w-[437px]`) without a comment. | MUST |
| R18 | No inline `style` except for computed values (e.g. a chart coordinate). | MUST |
| R19 | Status is never colour-only: icon + text + colour. It must survive colour-blindness and greyscale. | MUST |
| R20 | Dark theme is the default and the only theme in v1; tokens are defined once. | — |

### Accessibility

| # | Rule | Level |
| :-- | :-- | :-- |
| R21 | Every interactive element is reachable and operable by keyboard; the kill switch is reachable in ≤ 3 Tab presses from any screen. | MUST |
| R22 | The RTO clock is `aria-live="polite"` only when a claim crosses its threshold — not every second. | MUST |
| R23 | Dialogs trap focus and restore it on close (Radix does this; do not reimplement). | MUST |
| R24 | Every icon-only button has an `aria-label`. | MUST |

### Testing

| # | Rule | Level |
| :-- | :-- | :-- |
| R25 | Pure logic (schemas, formatters, claim rendering, countdown math) has unit tests. | MUST |
| R26 | Hooks with non-trivial state have tests with `msw` or a fake client. | SHOULD |
| R27 | Three Playwright flows are protected: login → inject → revert, drill run → evidence, panic. | MUST |
| R28 | No snapshot tests of whole screens. | MUST |

---

## 6. Shared abstractions — use these, not your own

The list below is the answer to "reuse proper abstractions". If a feature needs one of these
behaviours, it imports the shared component; it does not implement a variant.

| Need | Use | Never |
| :-- | :-- | :-- |
| Loading / empty / error / stale panel | `<AsyncPanel>` | a bespoke `if (loading) …` in every screen |
| Status colour + icon + label | `<StatusBadge status>` (one mapping) | inline colour logic |
| Countdown to a server deadline | `<Countdown expiresAt>` (server time, monotonic) | `Date.now()` math in a component |
| "Type the name to confirm" | `<ConfirmDestructive>` | `window.confirm` |
| Table with sorting, row actions, empty state | `<DataTable>` (TanStack Table wrapper) | a new `<table>` per screen |
| Polling with pause-on-hidden | `usePolling(queryKey, fn, ms)` | `setInterval` in a component |
| Relative/absolute time, durations | `lib/format` | `toLocaleString()` inline |
| Toasts | `sonner` via `notify.ok/error` | a custom toast system |
| Copy strings | `lib/copy` (one module) | scattered literals (labels, errors) |
| API errors | `ApiError` + `errorMessage(code)` | `error.message` shown raw |

Each shared abstraction is introduced **with** its first real use, not speculatively. The table is a
ceiling on invention, not a mandate to build all of it up front.

### 6.1 The inline sparkline contract

A sparkline in a table cell does not justify a charting library — but it does justify a spec,
because hand-rolled SVG fails in the same four places every time.

`<Sparkline points mode="line|bars" domain? height=24>` must:

| Case | Behaviour |
| :-- | :-- |
| 0 points | a dashed hairline baseline plus `aria-label="no data"` — never an empty `<svg>` |
| 1 point | a single centred dot (a line of length zero renders nothing) |
| all values equal (flatline) | horizontal line at mid-height; the domain is padded to avoid a divide-by-zero in scaling |
| `NaN` / missing | the path **breaks** into segments; no interpolation across a gap, which would invent data |
| irregular sampling | when timestamps are supplied, x is proportional to time, not to index — otherwise a 3 s gap looks like a 100 ms one |
| > 200 points | path only, no per-point DOM |
| resize | `viewBox` + `width:100%`; `preserveAspectRatio="none"` only together with `vector-effect="non-scaling-stroke"` |

No animation, no tooltip, no axes: if the reader needs to interrogate a value, they are looking at
the wrong component (use the drill chart, §11.1). The component is ~60 lines and has unit tests for
the four edge cases above.

---

## 7. Folder structure

```
web/
  src/
    app/            providers, router, layout, error boundary
    features/
      overview/     Control Room
      faults/       list, composer, active faults
      drills/       list, run, history, evidence
      scenarios/    list, run, progress
      timeline/     journal viewer
      history/      audit + replay
      session/      login, logout
    components/
      ui/           shadcn primitives (generated; do not edit)
      async-panel.tsx, status-badge.tsx, countdown.tsx,
      confirm-destructive.tsx, data-table.tsx, app-bar.tsx
    lib/
      api/          client, endpoints, ApiError
      query-keys.ts, format.ts, copy.ts, use-polling.ts
    styles/         tokens.css, index.css
  e2e/              Playwright
  CONVENTIONS.md    this document's §5–§7, short, with examples
```

**Import direction is one-way:** `app → features → components → lib`.
Enforced with `eslint-plugin-boundaries` (or `import/no-restricted-paths`); a violation fails CI.

---

## 8. Styling

- Tailwind with tokens defined as CSS variables: `--status-up`, `--status-degraded`,
  `--status-down`, `--status-unknown`, surface/ink/line scales.
- Status palette is chosen for contrast on dark and verified in greyscale (R19).
- Density: one comfortable density; the tables are dense enough for an operator without a
  "compact mode" toggle (which would be a second stylesheet to maintain).
- No animations beyond 150 ms transitions; a control room should feel instant, and motion on
  status changes is a lie about how fast things happened.

---

## 9. Accessibility

Target: WCAG 2.1 AA for the three critical flows, and keyboard-first for everything.

- The kill switch is in the app bar, first Tab stop, and labelled with its target.
- Focus rings are never removed, only restyled.
- Live regions are used sparingly (R22): the drill's claim list, not the clock.
- Tables have proper headers and scopes; a status column carries text.
- Colour contrast checked in CI with `axe-playwright` on the three flows.

---

## 10. Testing

| Layer | Tool | What |
| :-- | :-- | :-- |
| Unit | Vitest | schemas, formatters, query keys, countdown math, claim rendering, chart series transforms (empty / single / flat / gapped) |
| Hooks | Vitest + Testing Library + msw | polling pause/resume, mutation invalidations, error mapping |
| Components | Testing Library | composer validation, `<ConfirmDestructive>`, `<StatusBadge>` mapping |
| E2E | Playwright | login → inject → revert; drill run → evidence download; panic |
| A11y | axe-playwright | the three flows |

E2E runs against the compose stack, tagged like the backend tiers (`COCKPIT_STACK=1`), and is the
only suite allowed to be slow.

---

## 11. Dependency policy

**Budget**: a new runtime dependency needs a reason, a size, and an owner in the PR description.
The default answer is no.

| Allowed now | Why |
| :-- | :-- |
| react, react-dom, react-router | the app |
| @tanstack/react-query | server state, polling |
| @tanstack/react-table | the tables |
| react-hook-form, zod | forms + boundary validation |
| shadcn/ui deps (radix primitives, cva, clsx, tailwind-merge) | the component layer |
| sonner | toasts |
| recharts (**lazy-loaded**, drill + history routes only) | product charts, §11.1 |

`date-fns` is **not** allowed: `Intl.DateTimeFormat` and `Intl.RelativeTimeFormat` cover absolute
and relative time, and duration/countdown formatting is arithmetic on milliseconds that we already
own in `lib/format` (~80 lines). One less dependency for the same output.

| Rejected | Why |
| :-- | :-- |
| MUI, Chakra, Ant | competing design systems with their own styling runtime |
| Redux, MobX, Jotai | no global state (R12) |
| Axios | `fetch` + a 40-line client is enough |
| ECharts, Chart.js, Highcharts | large, canvas-based (harder to make accessible), and more capability than we need |
| full `d3` | a toolkit, not a chart component; we would rebuild the axes and tooltips ourselves |
| i18n framework (v1) | `lib/copy` is enough until a second language exists |

### 11.1 Charts

There are two different jobs, and they get two different answers:

| Job | Where | Approach |
| :-- | :-- | :-- |
| **Product charts** — probe series with axes, the RTO threshold as a reference line, the "act" moment as an annotation, tooltips, multi-series compare in drill history | Drill Run, Drill History | **Recharts**, in a lazy route chunk |
| **Inline sparkline** — one number per cell, no axes, no interaction | tables, cards | hand-rolled `<Sparkline>`, §6.1 |

Why a library for the first: axes with time zones, tooltip hit-testing, reference lines, brush/zoom
and responsive sizing are exactly where a hand-rolled chart starts costing weeks and ends up worse.
Recharts renders **SVG** (better for accessibility, greyscale and colour-blind checks than canvas),
is declarative React, and is well understood — the maintenance argument that made us own the
component layer does not apply to chart maths.

Why it is affordable: Recharts is the one dependency allowed to be large (~100 kB gzip), and it is
**only** in the drill chunk. The Control Room — the screen an operator opens first and keeps open —
never imports it.

Rules: charts are lazily imported per route; a chart's data transform is a pure function in
`lib/charts` with unit tests (empty, single point, flat, gapped series); no chart is rendered for a
series with zero points — `<AsyncPanel>` shows "no data" instead; every chart has a text summary
next to it (the same numbers, for screen readers and greyscale).

A raised bundle budget changes what a dependency may *weigh*; it does not change what it may *be*.
The component-library decision (§3) stays as it is — Recharts is allowed because it does one job we
do not want to own, not because there is room.

If a future requirement needs more than Recharts offers (very high-frequency series in the
thousands of points, GPU-scale rendering), **uPlot** is the fallback — but that is a later
decision with a measurement behind it, not a guess made now.

---

## 12. Tooling and CI gates

| Gate | Tool | Fails when |
| :-- | :-- | :-- |
| Types | `tsc --noEmit` strict | any `any`, any error |
| Lint | eslint + typescript-eslint + boundaries | R4, R5, R9, R13, R17, R24 |
| Format | prettier | — |
| Unit tests | vitest | any failure |
| Build | vite build | any warning-as-error |
| Bundle | `vite build` + `size-limit` | initial route > 500 kB gzip, or any lazy route chunk > 200 kB without a note in the PR |
| A11y | axe-playwright | a violation in the three flows |
| E2E | playwright | a failing critical flow |

The 150-line rule (R2) is enforced by review, not lint — it is a judgement call, and the PR
template asks about it.

---

## 13. Definition of done (per feature)

Copy this into the PR body:

- [ ] Screen renders all five states (loading, empty, error, stale, forbidden)
- [ ] Data via TanStack Query; keys from the factory; no `fetch` outside `lib/api`
- [ ] Destructive actions use `<ConfirmDestructive>`
- [ ] Times/durations via `lib/format`; strings via `lib/copy`
- [ ] No component over 150 lines; no feature importing another feature
- [ ] Keyboard path works; kill switch still reachable
- [ ] Unit tests for pure logic; the three E2E flows still green
- [ ] Bundle budget unchanged or justified

---

## 14. Relationship to the POC

`js-fe-client-poc/my-app` is a proof of concept with MUI. We copy **nothing** from it: not the
stack (§3), not the folder layout, not the components. It remains useful as evidence that the
build/test/container pattern works, and its Dockerfile/Makefile shape informs ours.

---

## 15. Open questions

| # | Question | Proposed default |
| :-- | :-- | :-- |
| 1 | **Component library: shadcn/ui, or Mantine for speed?** | shadcn/ui (§3) |
| 2 | Dark only, or dark + light? | dark only in v1; tokens make light cheap later |
| 3 | Is `lib/copy` worth it for one language? | yes — it is the cheapest way to keep labels consistent |
| 4 | Playwright in CI from A0, or from A3? | from A0 with one flow (login), expanded at A3 |
| 5 | Who reviews the React code? | one named owner; the BFF contract is the shared review surface |
| 6 | Does the front end live in `web/` of the cockpit repo? | yes — monorepo decided in the cockpit plan |
| 7 | **Charting: Recharts (lazy) or visx?** | Recharts, on the drill routes only; confirm React 19 support at A0 and fall back to visx if it lags |

---

## 16. Cost

The conventions cost about **one day at A0** (scaffold, boundaries, gates, the first shared
abstractions) and save more than that by A2, when the second and third screens would otherwise
each invent their own loading state. No milestone in the cockpit plan moves because of this
document; it is the difference between 37 days of a codebase and 37 days of a codebase someone can
still change.
