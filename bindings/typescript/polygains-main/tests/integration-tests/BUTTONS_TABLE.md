# PolyGains Frontend - Interactive Elements Reference

## Overview

This document catalogs all interactive elements (buttons, checkboxes, inputs, links) across both frontend pages for testing purposes.

**Pages:**
- `TerminalPage` (`/`) - Main terminal interface with advanced analytics
- `MainV2Page` (`/mainv2`) - Modern glassmorphism UI for mobile-first experience

---

## TerminalPage (`/`) - Button Inventory

### 1. Filter Controls

| # | Element | Type | Selector | Label/Icon | Action | State |
|---|---------|------|----------|------------|--------|-------|
| 1 | ALL | Button | `.join button:has-text("ALL")` | ALL | Filter alerts by all categories | Toggle (pressed/unpressed) |
| 2 | CRYPTO | Button | `.join button:has-text("CRYPTO")` | CRYPTO | Filter alerts by crypto category | Toggle (pressed/unpressed) |
| 3 | SPORTS | Button | `.join button:has-text("SPORTS")` | SPORTS | Filter alerts by sports category | Toggle (pressed/unpressed) |
| 4 | POLITICS | Button | `.join button:has-text("POLITICS")` | POLITICS | Filter alerts by politics category | Toggle (pressed/unpressed) |
| 5 | BOTH | Button | `.join button:has-text("BOTH")` | BOTH | Show both winners and losers | Toggle (pressed/unpressed) |
| 6 | WINNERS | Button | `.join button:has-text("WINNERS")` | WINNERS | Show only winning trades | Toggle (pressed/unpressed) |
| 7 | LOSERS | Button | `.join button:has-text("LOSERS")` | LOSERS | Show only losing trades | Toggle (pressed/unpressed) |

### 2. Pagination Controls

| # | Element | Type | Selector | Label | Action | State |
|---|---------|------|----------|-------|--------|-------|
| 8 | Alerts PREV | Button | `button[aria-label="Previous page"]:first` | ← PREV | Go to previous alerts page | Disabled on first page |
| 9 | Alerts NEXT | Button | `button[aria-label="Next page"]:first` | NEXT → | Go to next alerts page | Disabled on last page |
| 10 | Markets PREV | Button | `button[aria-label="Previous page"]:nth(1)` | ← PREV | Go to previous markets page | Disabled on first page |
| 11 | Markets NEXT | Button | `button[aria-label="Next page"]:nth(1)` | NEXT → | Go to next markets page | Disabled on last page |

### 3. Alert Row Actions

| # | Element | Type | Selector | Label | Action | State |
|---|---------|------|----------|-------|--------|-------|
| 12 | Lookup Trader | Link | `a[aria-label^="Lookup trader"]` | 🔍 | Open trader profile on Polymarket | Always enabled |

*Note: Multiple instances (one per alert row)*

### 4. Live Tracker Controls

| # | Element | Type | Selector | Label | Action | State |
|---|---------|------|----------|-------|--------|-------|
| 13 | Min Price | Input | `input[placeholder="Min P"]` | Min P | Set minimum price filter | Value: 0.00-1.00 |
| 14 | Max Price | Input | `input[placeholder="Max P"]` | Max P | Set maximum price filter | Value: 0.00-1.00 |
| 15 | 1 BET/MKT | Checkbox | `label:has-text("1 BET/MKT") input` | 1 BET/MKT | Toggle one bet per market | Checked/Unchecked |
| 16 | FIXED $10 | Checkbox | `label:has-text("FIXED $10") input` | FIXED $10 | Toggle fixed stake sizing | Checked/Unchecked |
| 17 | Sound Toggle | Button | `button[aria-label^="Sound"]` | 🔊/🔇 | Toggle sound notifications | Active/Inactive |
| 18 | FOLLOW | Checkbox | `label:has-text("FOLLOW") input` | FOLLOW | Enable follow insider strategy | Checked/Unchecked |
| 19 | REVERSE | Checkbox | `label:has-text("REVERSE") input` | REVERSE | Enable reverse insider strategy | Checked/Unchecked |
| 20 | YES | Checkbox | `label:has-text("YES") input` | YES | Include YES side bets | Checked/Unchecked |
| 21 | NO | Checkbox | `label:has-text("NO") input` | NO | Include NO side bets | Checked/Unchecked |

### 5. Backtest Controls

| # | Element | Type | Selector | Label | Action | State |
|---|---------|------|----------|-------|--------|-------|
| 22 | Run Backtest | Button | `button:has-text("Run Backtest")` | Run Backtest | Start backtest simulation | Changes to "Processing..." |
| 23 | Continue Backtest | Button | `button:has-text("Continue Backtest")` | Continue Backtest | Continue paused backtest | Enabled when can continue |

---

## MainV2Page (`/mainv2`) - Button Inventory

### 1. Category Filter Pills

| # | Element | Type | Selector | Label | Action | State |
|---|---------|------|----------|-------|--------|-------|
| 1 | ALL Pill | Button | `button[aria-label="Filter by category ALL"]` | ALL | Show all categories | Toggle (pressed/unpressed) |
| 2 | CRYPTO Pill | Button | `button[aria-label="Filter by category CRYPTO"]` | CRYPTO | Filter by crypto | Toggle (pressed/unpressed) |
| 3 | SPORTS Pill | Button | `button[aria-label="Filter by category SPORTS"]` | SPORTS | Filter by sports | Toggle (pressed/unpressed) |
| 4 | POLITICS Pill | Button | `button[aria-label="Filter by category POLITICS"]` | POLITICS | Filter by politics | Toggle (pressed/unpressed) |
| N | Other Categories | Button | `button[aria-label^="Filter by category"]` | {Category} | Filter by specific category | Dynamic based on API |

### 2. Alert Row Actions (Desktop Table)

| # | Element | Type | Selector | Label | Action | State |
|---|---------|------|----------|-------|--------|-------|
| 5 | Lookup Trader | Button | `button[aria-label^="Lookup trader"]` | 🔍 | Open trader profile | Always enabled |

### 3. Alert Card Actions (Mobile View)

| # | Element | Type | Selector | Label | Action | State |
|---|---------|------|----------|-------|--------|-------|
| N/A | Entire Card | Clickable | `li` (glass card) | - | View alert details | Clickable |

---

## Summary Statistics

| Page | Buttons | Checkboxes | Inputs | Links | Total Interactive |
|------|---------|------------|--------|-------|-------------------|
| TerminalPage | 14 | 6 | 2 | N | 22+ per alert row |
| MainV2Page | 4+ | 0 | 0 | 0 | 4+ per alert row |

---

## ARIA Labels Reference

### TerminalPage
```
aria-label="Filter alerts by {CATEGORY}"
aria-label="Show {both|winners|losers}"
aria-label="Previous page"
aria-label="Next page"
aria-label="Lookup trader {wallet_address}"
aria-label="Sound Enabled"
aria-label="Sound Muted"
aria-label="{Run Backtest|Continue Backtest|Processing...}"
```

### MainV2Page
```
aria-label="Filter by category {CATEGORY}"
aria-label="Lookup trader {wallet_address}"
```

---

## Test Priority Matrix

| Priority | Elements | Test Coverage |
|----------|----------|---------------|
| P0 (Critical) | Category filters, Pagination, Lookup buttons | Must work on all viewports |
| P1 (High) | Checkboxes (FOLLOW, REVERSE, YES, NO), Price inputs | Core functionality |
| P2 (Medium) | Sound toggle, Backtest button, Fixed stake | Nice to have |
| P3 (Low) | 1 BET/MKT | Edge case testing |

---

## Responsive Behavior

### Desktop (>1024px)
- All buttons visible
- Full table layout in MainV2Page
- Side-by-side controls

### Tablet (768px-1024px)
- Category buttons may wrap
- Stats stack vertically
- Tables scroll horizontally if needed

### Mobile (<768px)
- **TerminalPage**: Horizontal scroll risk on tables
- **MainV2Page**: Cards instead of table
- Category pills scroll horizontally
- Min touch target: 44px (configured)

---

*Generated for integration testing purposes*
