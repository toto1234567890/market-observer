// static/dashboard.js
/**
 * @fileoverview Main frontend controller for the Market Observer Dashboard.
 * Handles WebSocket connections, real-time data merging, and UI updates.
 */

/**
 * Main application class for controlling the Dashboard UI.
 * Manages state, WebSocket communication, and DOM manipulation.
 */
class MarketObserverDashboard {
    /**
     * Initializes the dashboard state, configuration, and starts the boot sequence.
     */
    constructor() {
        this.ws = null;
        /** @type {Object} The local state cache containing {raw_data, aggregations, timestamp} */
        this.data = {};

        /** @type {Array} Log of connection events {time, text, className} */
        this.connectionLogs = [];

        // TimeFrame configuration 
        this.timeframes = ['5m'];
        this.currentTimeframe = '5m';

        // State
        /** @type {boolean} Flag to prevent multiple render loops */
        this.isUpdatePending = false;

        // Start Async Init
        this.initializeApp();
    }


    //----------------------------------------------------------------------------------
    /**
     * Bootstraps the application: fetches config, sets up initial UI, and connects.
     */
    async initializeApp() {
        await this.fetchConfig();
        this.setupTimeframeSelector();
        this.startWebSocket();
        this.setupTimeframeEventListener();
    }

    //----------------------------------------------------------------------------------
    /**
     * Fetches dynamic configuration (like available timeframes) from the backend API.
     * Falls back to defaults on error.
     */
    async fetchConfig() {
        try {
            const response = await fetch('/api/config');
            const config = await response.json();
            if (config.timeframes && Array.isArray(config.timeframes)) {
                this.timeframes = config.timeframes;
                // Ensure default is valid if possible
                if (!this.timeframes.includes(this.currentTimeframe)) {
                    this.currentTimeframe = this.timeframes[0];
                }
            }
        } catch (e) {
            console.error("Failed to load config, using defaults:", e);
        }
    }

    //----------------------------------------------------------------------------------
    /**
     * Sets up the initial DOM elements, such as the timeframe selector.
     * Cleans up any potential debris from previous sessions/hot-reloads.
     */
    setupTimeframeSelector() {
        const selector = document.getElementById('timeframeSelector');

        if (selector) {
            selector.innerHTML = this.timeframes.map(tf =>
                `<option value="${tf}" ${tf === this.currentTimeframe ? 'selected' : ''}>${tf}</option>`
            ).join('');
        } else {
            console.warn('Timeframe selector not found in DOM.');
        }
    }

    //----------------------------------------------------------------------------------

    startWebSocket() {
        this.updateConnectionStatus('Connecting...', 'bg-warning');
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/ws`;

        this.ws = new WebSocket(wsUrl);

        /**
         * Handler for WS Connection Open.
         * Updates status and sends initial subscription payload.
         */
        this.ws.onopen = () => {
            this.updateConnectionStatus('Connection open');
            this.sendSubscription();
        };

        /**
         * Handler for Incoming Messages.
         * Routes messages based on 'type':
         * - INITIAL: Full state replacement (new subscription/reconnect)
         * - UPDATE: Partial state merge (real-time delta)
         */
        this.ws.onmessage = (event) => {
            try {
                const incoming = JSON.parse(event.data);

                if (incoming.type === 'INITIAL' || incoming.type === 'UPDATE') {
                    // Full snapshot replacement
                    this.data = incoming;
                    console.log('Dashboard data loaded');
                    this.scheduleUpdate();
                } else {
                    // Fallback for legacy or unknown messages
                    if (!incoming.type) {
                        this.data = incoming;
                        this.scheduleUpdate();
                    }
                }

            } catch (e) {
                console.error("[Dashboard] Error processing WS message:", e);
            }
        };

        /**
         * Handler for Connection Close.
         * Attempts automatic reconnection after 3 seconds.
         */
        this.ws.onclose = () => {
            this.updateConnectionStatus('Connection closed');
            setTimeout(() => this.startWebSocket(), 3000);
        };

        this.ws.onerror = (err) => {
            this.updateConnectionStatus('Error');
        };
    }

    /**
     * Sends the 'subscribe' command to the server.
     * This tells the server which timeframe we start with.
     */
    sendSubscription() {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify({
                command: 'subscribe',
                clientType: 'dashboard',
                symbols: [],
                timeframe: this.currentTimeframe
            }));
        }
    }

    //----------------------------------------------------------------------------------

    //----------------------------------------------------------------------------------

    /**
     * Merges a 'delta' update into the local state.
     * Crucial for functionality: Server sends lightweight updates (deltas) that often
     * omit heavy fields like 'time_series'. We must preserve those if they exist locally.
     * 
     * @param {Object} incoming - The partial update object from the server.
     */
    mergeData(incoming) {
        // Initialize if empty
        if (!this.data.raw_data) this.data.raw_data = {};
        if (!this.data.aggregations) this.data.aggregations = {};

        // 1. Merge Raw Data
        if (incoming.raw_data) {
            Object.keys(incoming.raw_data).forEach(sym => {
                // If we already have data for this symbol, careful not to lose time_series 
                // if the update doesn't include it (common in lightweight updates).
                const existing = this.data.raw_data[sym] || {};
                const update = incoming.raw_data[sym];

                // Preserve existing time_series if update lacks it
                if (existing.time_series && !update.time_series) {
                    update.time_series = existing.time_series;
                }

                this.data.raw_data[sym] = update;
            });
        }

        // 2. Merge Aggregations
        if (incoming.aggregations) {
            Object.keys(incoming.aggregations).forEach(sym => {
                if (!this.data.aggregations[sym]) this.data.aggregations[sym] = {};

                // Nesting: {Symbol: {Window: Data}}
                // We overwrite specific window entries
                Object.keys(incoming.aggregations[sym]).forEach(window => {
                    this.data.aggregations[sym][window] = incoming.aggregations[sym][window];
                });
            });
        }

        // 3. Update Metrics/Timestamp
        this.data.timestamp = incoming.timestamp || Date.now() / 1000;
        if (incoming.processing_metrics) {
            this.data.processing_metrics = incoming.processing_metrics;
        }
    }

    //----------------------------------------------------------------------------------

    //----------------------------------------------------------------------------------
    /**
     * Sets up event listeners for dashboard controls (e.g., Timeframe selector).
     */
    setupTimeframeEventListener() {
        const tfSelector = document.getElementById('timeframeSelector');
        if (tfSelector) {
            tfSelector.addEventListener('change', (e) => {
                const newValue = e.target.value;
                if (this.currentTimeframe !== newValue) {
                    this.currentTimeframe = newValue;
                    console.log('Timeframe changed to:', newValue);

                    // We re-subscribe, which triggers a fresh INITIAL payload from server
                    this.sendSubscription();
                }
            });
        }
    }

    //----------------------------------------------------------------------------------

    //----------------------------------------------------------------------------------
    /**
     * Updates the connection status icon in the header.
     */
    updateConnectionStatus(status = "Connecting...") {
        const button = document.getElementById('connectionIcon');
        const icon = button ? button.querySelector('i') : null;
        // Text element might not exist in new layout, but kept for Fallback
        const connectionStatusElement = document.getElementById("connectionStatus");

        if (button && icon) {
            switch (status) {
                case 'Connection open':
                    button.style.color = 'green';
                    icon.className = 'fa fa-check-circle fa-fw';
                    break;
                case 'Connecting...':
                    button.style.color = 'blue';
                    icon.className = 'fa fa-spinner fa-spin fa-fw';
                    break;
                case 'Connection closed':
                    button.style.color = 'red';
                    icon.className = 'fa fa-times-circle fa-fw';
                    break;
                default:
                    if (connectionStatusElement) {
                        connectionStatusElement.textContent = status;
                    } else {
                        // Fallback icon for error/unknown if text element missing
                        button.style.color = 'red';
                        icon.className = 'fa fa-exclamation-triangle fa-fw';
                    }
            }
            button.title = status;
        }

        // Log the event
        this.connectionLogs.unshift({
            time: new Date().toLocaleTimeString(),
            text: status
        });
    }

    /**
     * Populates and opens the connection log modal.
     */
    openConnectionLog() {
        const tbody = document.getElementById('modalContent');
        if (!tbody) return;

        if (this.connectionLogs.length === 0) {
            tbody.innerHTML = '<tr><td colspan="2" class="text-center text-muted">No logs yet</td></tr>';
            return;
        }

        tbody.innerHTML = this.connectionLogs.map(log => {
            return `
                <tr>
                    <td class="small text-muted">${log.time}</td>
                    <td class="${log.className}">${log.text}</td>
                </tr>
            `;
        }).join('');
    }

    //----------------------------------------------------------------------------------

    //----------------------------------------------------------------------------------

    /**
     * Retrieves and prepares the data for display based on the selected timeframe.
     * 
     * Strategy:
     * 1. Start with the Raw Data (latest snapshot).
     * 2. If Aggregated Data exists for the *current timeframe*, overlay it.
     *    This ensures we show timeframe-specific metrics (e.g., 1h % change)
     *    instead of just raw snapshot metrics.
     * 
     * @returns {Object} A map of {Symbol: DisplayObject}
     */
    getDisplayData() {
        const rawData = this.data.raw_data || {};
        const aggregations = this.data.aggregations || {};

        // Derive symbols from both raw and aggregated data to handle empty raw_data case
        const rawSymbols = Object.keys(rawData);
        const aggSymbols = Object.keys(aggregations);
        const symbols = [...new Set([...rawSymbols, ...aggSymbols])];

        const result = {};

        symbols.forEach(sym => {
            // Default to raw data or empty object with symbol
            let displayItem = rawData[sym] || { symbol: sym };

            // If we have aggregated data for this timeframe, prefer it
            if (aggregations[sym] && aggregations[sym][this.currentTimeframe]) {
                // Merge aggregated metrics (price_open, price_close, etc) onto the base object
                // effectively overriding the "raw" snapshot metrics with the timeframe metrics.
                // We keep the symbol name, etc.
                const agg = aggregations[sym][this.currentTimeframe];

                // Construct a display object. 
                // Note: The structure of agg items usually matches what we need (price_percent_change, volume, etc.)
                // We assume agg contains: { price_percent_change, volume, volume_percent_change, ... }
                displayItem = {
                    ...displayItem,
                    ...agg
                };
            }

            result[sym] = displayItem;
        });

        return result;
    }

    //----------------------------------------------------------------------------------

    /**
     * Schedules a UI update for the next animation frame.
     * Prevents UI thrashing by debouncing updates.
     */
    scheduleUpdate() {
        if (!this.isUpdatePending) {
            this.isUpdatePending = true;
            requestAnimationFrame(() => this.updateUI());
        }
    }

    /**
     * The main Render Loop.
     * Called by scheduleUpdate() to refresh all UI components.
     */
    updateUI() {
        this.isUpdatePending = false;
        try {
            const currentData = this.getDisplayData();
            this.updateMetrics(currentData);
            this.updateAnalysisBlocks(currentData);
            this.updateSymbolsList(currentData);
        } catch (e) {
            console.error("[Dashboard] Error in updateUI:", e);
        }
    }

    //----------------------------------------------------------------------------------

    //----------------------------------------------------------------------------------

    /**
     * Updates the top-level metrics (Active Symbols, Last Update, Backend Health).
     * @param {Object} data - The map of display data items.
     */
    updateMetrics(data) {
        const timestamp = this.data.timestamp;
        const count = Object.keys(data).length;

        const activeSymbolsEl = document.getElementById('activeSymbols');
        if (activeSymbolsEl) {
            activeSymbolsEl.textContent = count;
            activeSymbolsEl.classList.toggle('text-success', count > 0);
        }

        const lastUpdateEl = document.getElementById('lastUpdate');
        if (lastUpdateEl && timestamp) {
            lastUpdateEl.textContent = new Date(timestamp * 1000).toLocaleTimeString();
        }

        // Performance Metrics
        const metrics = this.data.processing_metrics || {};
        const aggLatencyEl = document.getElementById('aggLatency');
        const processedItemsEl = document.getElementById('processedItems');

        if (aggLatencyEl && metrics.aggregation_time_seconds !== undefined) {
            const lat = metrics.aggregation_time_seconds;
            aggLatencyEl.textContent = lat.toFixed(3) + 's';

            aggLatencyEl.className = 'metric-value'; // Reset
            if (lat < 1.0) aggLatencyEl.classList.add('text-success');
            else if (lat < 2.0) aggLatencyEl.classList.add('text-warning');
            else aggLatencyEl.classList.add('text-danger');
        }

        if (processedItemsEl) {
            const sym = metrics.valid_symbols || 0;
            const win = metrics.windows_processed || 0;
            processedItemsEl.textContent = `${sym} / ${win}`;
        }
    }

    //----------------------------------------------------------------------------------

    //----------------------------------------------------------------------------------

    /**
     * Updates the "Analysis" cards (Top Gainers/Losers, Anomalies, Activity).
     * Sorts and slices the data to show top 5 items for each category.
     * @param {Object} data - The map of display data items.
     */
    updateAnalysisBlocks(data) {
        const symbols = Object.keys(data);
        if (symbols.length === 0) return;

        const dataArray = symbols.map(s => data[s]);

        // 1. Gainers / Losers
        // Sort by price_percent_change
        const sortedByPrice = [...dataArray].sort((a, b) => {
            const va = a.price_percent_change || 0;
            const vb = b.price_percent_change || 0;
            return vb - va;
        });

        const midPoint = Math.ceil(sortedByPrice.length / 2);
        // If list is small, gainers/losers might overlap or just show Top N
        const gainers = sortedByPrice.slice(0, 5); // Show top 5
        const losers = sortedByPrice.slice(-5).reverse(); // Show bottom 5

        // Helper for formatting
        const formatPct = (val) => {
            const num = val || 0;
            const pct = (num * 100).toFixed(2);
            const isPos = num >= 0;
            const sign = isPos ? '+' : '';
            const color = isPos ? 'text-success' : 'text-danger';
            return { pct, sign, color };
        };

        const gainerLoserRenderer = (item) => {
            const { pct, sign, color } = formatPct(item.price_percent_change);
            return `<div class="list-group-item list-group-item-action d-flex justify-content-between align-items-center px-3 py-2 border-0 bg-transparent">
                <span class="item-symbol fw-bold text-truncate" title="${item.symbol}" style="max-width: 60%;">${item.symbol}</span>
                <span class="item-value ${color} text-nowrap ml-2">${sign}${pct}%</span>
            </div>`;
        };

        this.renderList('topGainersList', gainers, gainerLoserRenderer);
        this.renderList('topLosersList', losers, gainerLoserRenderer);

        // 2. Abnormal Volume
        const abnormal = dataArray.filter(d => (d.volume_anomaly_ratio || 0) > 1.0)
            .sort((a, b) => (b.volume || 0) - (a.volume || 0))
            .slice(0, 5);

        this.renderList('abnormalVolList', abnormal, (item) => {
            return `<div class="list-group-item list-group-item-action d-flex justify-content-between align-items-center px-3 py-2 border-0 bg-transparent">
                <span class="item-symbol fw-bold text-truncate" title="${item.symbol}" style="max-width: 60%;">${item.symbol}</span>
                <span class="item-value text-nowrap ml-2">${(item.volume || 0).toLocaleString()}</span>
            </div>`;
        });

        // 3. Activity (Volume Change?)
        // Assuming volume_percent_change is what we want here
        const sortedByAct = [...dataArray].sort((a, b) => (b.volume_percent_change || 0) - (a.volume_percent_change || 0));

        const highActivity = sortedByAct.slice(0, 5);
        const lowActivity = sortedByAct.slice(-5).reverse();

        const activityRenderer = (item) => {
            const val = item.volume_percent_change;
            const { pct, sign, color } = formatPct(val);
            return `<div class="list-group-item list-group-item-action d-flex justify-content-between align-items-center px-3 py-2 border-0 bg-transparent">
                <span class="item-symbol fw-bold text-truncate" title="${item.symbol}" style="max-width: 60%;">${item.symbol}</span>
                <span class="item-value ${color} text-nowrap ml-2">${sign}${pct}%</span>
            </div>`;
        };

        this.renderList('highActivityList', highActivity, activityRenderer);
        this.renderList('lowActivityList', lowActivity, activityRenderer);
    }

    //----------------------------------------------------------------------------------

    //----------------------------------------------------------------------------------

    /**
     * Generic helper to render a list of items into a DOM container.
     * @param {string} elementId - DOM ID of the container.
     * @param {Array} items - Array of data objects to render.
     * @param {Function} rowRenderer - Function returning HTML string for a single item.
     */
    renderList(elementId, items, rowRenderer) {
        const el = document.getElementById(elementId);
        if (!el) return;

        if (items.length === 0) {
            el.innerHTML = '<div class="text-muted text-center p-2 small">No data</div>';
            return;
        }

        el.innerHTML = items.map(item => `
            <div class="list-item d-flex justify-content-between align-items-center p-2 border-bottom clickable-row" 
                 onclick="dashboard.openSymbolWindow('${item.symbol}')" style="cursor: pointer;">
                ${rowRenderer(item)}
            </div>
        `).join('');
    }

    //----------------------------------------------------------------------------------

    //----------------------------------------------------------------------------------

    /**
     * Renders the main grid of Symbol Cards (Ticker, Price, Change).
     * @param {Object} data - The map of display data items.
     */
    updateSymbolsList(data) {
        const grid = document.getElementById('symbolsGrid');
        if (!grid) return;

        const symbols = Object.keys(data).sort();

        if (symbols.length === 0) {
            grid.innerHTML = '<div class="col-12"><div class="alert alert-info">No symbols active</div></div>';
            return;
        }

        let html = '';
        symbols.forEach(sym => {
            const item = data[sym];
            const price = item.price !== undefined ? item.price.toFixed(2) : '0.00';
            const pctVal = item.price_percent_change !== undefined ? item.price_percent_change : 0;
            const pct = (pctVal * 100).toFixed(2);

            const trendClass = pctVal >= 0 ? 'text-success' : 'text-danger';
            const trendIcon = pctVal >= 0 ? '▲' : '▼';
            const cardBorder = pctVal >= 0 ? 'border-success' : 'border-danger';

            html += `
                <div class="col-xl-3 col-lg-4 col-md-6 col-sm-12 mb-3">
                    <div class="card symbol-card ${cardBorder} shadow rounded-lg overflow-hidden h-100" onclick="dashboard.openSymbolWindow('${sym}')" style="cursor: pointer;">
                        <div class="card-body text-center p-3">
                            <h5 class="card-title fw-bold mb-1">${sym}</h5>
                            <div class="card-text h5 mb-2">$${price}</div>
                            <div class="small ${trendClass} fw-bold">
                                ${trendIcon} ${pct}%
                            </div>
                        </div>
                    </div>
                </div>
            `;
        });
        grid.innerHTML = html;
    }

    //----------------------------------------------------------------------------------

    //----------------------------------------------------------------------------------

    /**
     * Opens a detailed popup window for a specific symbol.
     * Injects a standalone HTML/JS page into the popup to render an interactive chart.
     * 
     * @param {string} symbol - The symbol to open (e.g., "AAPL").
     */
    openSymbolWindow(symbol) {
        // Open symbol view in a new browser tab
        //const win = window.open(`/symbol_view.html?symbol=${symbol}`, `Symbol_view_${symbol}`, 'width=900,height=750,resizable=yes,scrollbars=yes');
        //if (!win) {
        //    alert("Popup blocked!");
        //} else {
        //    win.focus();
        //}
        const win = window.open(`https://finance.yahoo.com/quote/${symbol}/`);
        if (win) {
            win.focus();
        }
    }
}

document.addEventListener('DOMContentLoaded', () => { dashboard = new MarketObserverDashboard(); });
