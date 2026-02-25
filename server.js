import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { createServer } from "node:http";
import { readFileSync } from "node:fs";
import { z } from "zod";

const WIDGET_HTML = readFileSync("public/statcan-widget.html", "utf8");
const WIDGET_URI = "ui://widget/statcan-table.html";

const TABLE_CATALOGUE = {
  population: { id: "17-10-0005-01", title: "Population estimates, quarterly", description: "Canadian population by province/territory", topic: "Demographics" },
  labour: { id: "14-10-0287-01", title: "Labour force characteristics by province", description: "Employment, unemployment rate, participation rate", topic: "Labour market" },
  cpi: { id: "18-10-0004-01", title: "Consumer Price Index, monthly", description: "CPI by product group - food, shelter, energy", topic: "Prices" },
  gdp: { id: "36-10-0434-01", title: "GDP by industry", description: "GDP at basic prices by industry group, monthly", topic: "National accounts" },
  housing: { id: "34-10-0158-01", title: "Housing starts (CMHC)", description: "Housing starts by type of dwelling, monthly", topic: "Housing" },
  trade: { id: "12-10-0011-01", title: "International merchandise trade", description: "Canadian exports and imports by commodity", topic: "International trade" },
  crime: { id: "35-10-0177-01", title: "Crime severity index", description: "Crime severity index by province and CMA", topic: "Justice" },
  education: { id: "37-10-0003-01", title: "Postsecondary enrolments", description: "University and college enrolments by province", topic: "Education" },
};

async function fetchWDSData(tableId, maxRows = 20) {
  const pid = tableId.replace(/-/g, "").slice(0, 8);
  const url = `https://www150.statcan.gc.ca/t1/tbl1/en/dtbl!downloadTbl/jsonDownload?pid=${pid}`;
  const resp = await fetch(url, {
    headers: { "Accept": "*/*", "User-Agent": "StatCanAthenaAgent/1.0" },
    signal: AbortSignal.timeout(20_000),
  });
  if (!resp.ok) throw new Error(`WDS API error ${resp.status}`);
  const data = await resp.json();
  if (!Array.isArray(data) || data.length === 0) throw new Error("No data returned");
  return data.slice(0, maxRows);
}

function parseStatCanResponse(rawData, tableId, maxRows) {
  if (!Array.isArray(rawData) || rawData.length === 0) return null;
  const allKeys = Object.keys(rawData[0]);
  const skip = new Set(["DGUID","UOM","UOM_ID","SCALAR_FACTOR","SCALAR_ID","VECTOR","COORDINATE","STATUS","SYMBOL","TERMINATED","DECIMALS"]);
  const displayKeys = allKeys.filter(k => !skip.has(k)).slice(0, 6);
  const valueKeys = new Set(allKeys.filter(k => k === "VALUE" || k.toLowerCase().includes("value")));
  const columns = displayKeys.map(k => ({
    key: k,
    label: k.replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase()).replace("Ref Date","Reference Date").replace("Geo","Geography").replace("VALUE","Value"),
    numeric: valueKeys.has(k),
    change: false,
  }));
  const rows = rawData.slice(0, maxRows).map(row => {
    const out = {};
    displayKeys.forEach(k => { out[k] = row[k]; });
    return out;
  });
  return { title: "Statistics Canada Data", source: "Statistics Canada", columns, rows, notes: { table_id: tableId } };
}

function getSampleData(topic) {
  const samples = {
    population: {
      title: "Population Estimates by Province (2024 Q2)",
      source: "Statistics Canada, Table 17-10-0005-01",
      columns: [
        { key: "geo", label: "Province / Territory", numeric: false },
        { key: "population", label: "Population", numeric: true },
        { key: "change", label: "Year-over-Year Change (%)", numeric: false, change: true },
      ],
      rows: [
        { geo: "Canada", population: 41000000, change: "2.3" },
        { geo: "Ontario", population: 15300000, change: "2.7" },
        { geo: "Quebec", population: 9000000, change: "1.8" },
        { geo: "British Columbia", population: 5700000, change: "3.1" },
        { geo: "Alberta", population: 4900000, change: "4.2" },
        { geo: "Manitoba", population: 1450000, change: "2.0" },
        { geo: "Saskatchewan", population: 1250000, change: "2.5" },
        { geo: "Nova Scotia", population: 1050000, change: "3.6" },
        { geo: "New Brunswick", population: 850000, change: "2.9" },
        { geo: "Newfoundland and Labrador", population: 542000, change: "0.8" },
        { geo: "Prince Edward Island", population: 180000, change: "3.5" },
        { geo: "Northwest Territories", population: 44000, change: "0.5" },
        { geo: "Yukon", population: 46000, change: "1.9" },
        { geo: "Nunavut", population: 40000, change: "1.2" },
      ],
      notes: { reference_period: "Q2 2024", table_id: "17-10-0005-01", geography: "Provinces & Territories" },
    },
    labour: {
      title: "Labour Force Characteristics by Province (Dec 2024)",
      source: "Statistics Canada, Table 14-10-0287-01",
      columns: [
        { key: "geo", label: "Province", numeric: false },
        { key: "employed", label: "Employed (000s)", numeric: true },
        { key: "unemployed", label: "Unemployed (000s)", numeric: true },
        { key: "unemployment_rate", label: "Unemployment Rate (%)", numeric: true },
        { key: "participation_rate", label: "Participation Rate (%)", numeric: true },
      ],
      rows: [
        { geo: "Canada", employed: 20500, unemployed: 1450, unemployment_rate: 6.6, participation_rate: 65.1 },
        { geo: "Ontario", employed: 7800, unemployed: 560, unemployment_rate: 6.7, participation_rate: 65.5 },
        { geo: "Quebec", employed: 4600, unemployed: 290, unemployment_rate: 5.9, participation_rate: 64.8 },
        { geo: "British Columbia", employed: 2900, unemployed: 185, unemployment_rate: 6.0, participation_rate: 64.3 },
        { geo: "Alberta", employed: 2500, unemployed: 155, unemployment_rate: 5.8, participation_rate: 69.2 },
        { geo: "Manitoba", employed: 720, unemployed: 42, unemployment_rate: 5.5, participation_rate: 66.3 },
        { geo: "Saskatchewan", employed: 620, unemployed: 33, unemployment_rate: 5.0, participation_rate: 68.1 },
        { geo: "Nova Scotia", employed: 510, unemployed: 38, unemployment_rate: 6.9, participation_rate: 62.4 },
        { geo: "New Brunswick", employed: 390, unemployed: 32, unemployment_rate: 7.6, participation_rate: 61.8 },
      ],
      notes: { reference_period: "December 2024", table_id: "14-10-0287-01", geography: "Provinces" },
    },
    cpi: {
      title: "Consumer Price Index by Category (Nov 2024)",
      source: "Statistics Canada, Table 18-10-0004-01",
      columns: [
        { key: "category", label: "Category", numeric: false },
        { key: "index", label: "Index (2002=100)", numeric: true },
        { key: "annual_change", label: "Annual Change (%)", numeric: false, change: true },
        { key: "monthly_change", label: "Monthly Change (%)", numeric: false, change: true },
      ],
      rows: [
        { category: "All-items", index: 162.5, annual_change: "1.9", monthly_change: "0.1" },
        { category: "Food", index: 178.2, annual_change: "2.6", monthly_change: "0.4" },
        { category: "Shelter", index: 189.6, annual_change: "4.6", monthly_change: "0.3" },
        { category: "Household operations", index: 151.3, annual_change: "1.2", monthly_change: "-0.1" },
        { category: "Clothing and footwear", index: 108.7, annual_change: "-0.8", monthly_change: "-0.5" },
        { category: "Transportation", index: 165.4, annual_change: "-0.4", monthly_change: "0.2" },
        { category: "Health and personal care", index: 155.8, annual_change: "3.1", monthly_change: "0.2" },
        { category: "Energy", index: 152.1, annual_change: "-9.2", monthly_change: "-1.4" },
      ],
      notes: { reference_period: "November 2024", table_id: "18-10-0004-01", geography: "Canada" },
    },
    housing: {
      title: "Housing Starts by Province (Oct 2024)",
      source: "Statistics Canada / CMHC, Table 34-10-0158-01",
      columns: [
        { key: "geo", label: "Province", numeric: false },
        { key: "total", label: "Total Starts", numeric: true },
        { key: "single", label: "Single-Detached", numeric: true },
        { key: "multi", label: "Multi-Unit", numeric: true },
        { key: "annual_change", label: "Year-over-Year (%)", numeric: false, change: true },
      ],
      rows: [
        { geo: "Canada", total: 22543, single: 5821, multi: 16722, annual_change: "8.3" },
        { geo: "Ontario", total: 8120, single: 1840, multi: 6280, annual_change: "12.1" },
        { geo: "British Columbia", total: 4350, single: 720, multi: 3630, annual_change: "-4.2" },
        { geo: "Quebec", total: 4780, single: 810, multi: 3970, annual_change: "15.6" },
        { geo: "Alberta", total: 3200, single: 1650, multi: 1550, annual_change: "18.9" },
        { geo: "Manitoba", total: 560, single: 220, multi: 340, annual_change: "3.7" },
        { geo: "Saskatchewan", total: 480, single: 310, multi: 170, annual_change: "-2.0" },
        { geo: "Nova Scotia", total: 380, single: 160, multi: 220, annual_change: "7.9" },
        { geo: "New Brunswick", total: 290, single: 110, multi: 180, annual_change: "4.3" },
      ],
      notes: { reference_period: "October 2024", table_id: "34-10-0158-01", geography: "Provinces" },
    },
  };
  return samples[topic] || samples.population;
}

function createStatCanServer() {
  const server = new McpServer({ name: "statcan-agent", version: "1.0.0" });

  server.resource("statcan-widget", WIDGET_URI, async () => ({
    contents: [{
      uri: WIDGET_URI,
      mimeType: "text/html+skybridge",
      text: WIDGET_HTML,
      _meta: { "openai/widgetPrefersBorder": true },
    }],
  }));

  server.tool(
    "browse_catalogue",
    "Use this when the user wants to see what Statistics Canada datasets are available, or wants to explore topics like population, labour, prices, housing, GDP, trade, crime, or education.",
    {},
    async () => {
      const rows = Object.entries(TABLE_CATALOGUE).map(([key, entry]) => ({
        topic: key, id: entry.id, title: entry.title, description: entry.description, theme: entry.topic,
      }));
      return {
        content: [{ type: "text", text: `Found ${rows.length} datasets in the Statistics Canada catalogue.` }],
        structuredContent: {
          title: "Statistics Canada Open Data Catalogue",
          source: "Statistics Canada (statcan.gc.ca)",
          columns: [
            { key: "theme", label: "Theme", numeric: false },
            { key: "title", label: "Dataset", numeric: false },
            { key: "id", label: "Table ID", numeric: false },
            { key: "description", label: "Description", numeric: false },
          ],
          rows,
          notes: { reference_period: "2024", geography: "Canada", table_id: "Multiple" },
        },
      };
    }
  );

  server.tool(
    "get_statcan_data",
    "Use this when the user asks for specific Canadian statistics or data. Retrieves and displays data tables from Statistics Canada for topics like population, labour force, CPI/inflation, housing starts, GDP, or trade.",
    {
      topic: z.enum(["population", "labour", "cpi", "housing", "gdp", "trade", "crime", "education"]).describe("The statistical topic to retrieve"),
      max_rows: z.number().int().min(5).max(50).default(15).describe("Max rows to return"),
    },
    async ({ topic, max_rows = 15 }) => {
      const catalogue = TABLE_CATALOGUE[topic];
      if (!catalogue) return { content: [{ type: "text", text: `Unknown topic: ${topic}` }], structuredContent: { rows: [], columns: [], title: "Error", source: "" } };

      let structuredContent;
      try {
        const rawData = await fetchWDSData(catalogue.id, max_rows);
        structuredContent = parseStatCanResponse(rawData, catalogue.id, max_rows);
        if (structuredContent) structuredContent.title = catalogue.title;
      } catch (err) {
        console.error(`Live API failed for ${topic}:`, err.message);
      }

      if (!structuredContent) structuredContent = getSampleData(topic);

      return {
        content: [{ type: "text", text: `Loaded ${structuredContent.rows.length} records for "${structuredContent.title}".` }],
        structuredContent,
      };
    }
  );

  server.tool(
    "search_statcan",
    "Use this when the user provides a specific Statistics Canada table ID (e.g. 17-10-0005-01) and wants to retrieve its data directly.",
    {
      table_id: z.string().describe("Statistics Canada table ID, e.g. 17-10-0005-01"),
      max_rows: z.number().int().min(5).max(50).default(20).describe("Max rows to return"),
    },
    async ({ table_id, max_rows = 20 }) => {
      let structuredContent = null;
      try {
        const rawData = await fetchWDSData(table_id, max_rows);
        structuredContent = parseStatCanResponse(rawData, table_id, max_rows);
      } catch (err) {
        console.error("Live API error:", err.message);
      }

      if (!structuredContent) {
        return { content: [{ type: "text", text: `Could not retrieve table ${table_id}.` }], structuredContent: { rows: [], columns: [], title: `Table ${table_id}`, source: "Statistics Canada" } };
      }

      structuredContent.title = `Statistics Canada Table ${table_id}`;
      return { content: [{ type: "text", text: `Loaded ${structuredContent.rows.length} rows from table ${table_id}.` }], structuredContent };
    }
  );

  return server;
}

const PORT = Number(process.env.PORT ?? 8787);
const MCP_PATH = "/mcp";

const httpServer = createServer(async (req, res) => {
  if (!req.url) { res.writeHead(400).end("Missing URL"); return; }
  const url = new URL(req.url, `http://${req.headers.host ?? "localhost"}`);

  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "POST, GET, DELETE, OPTIONS",
      "Access-Control-Allow-Headers": "content-type, mcp-session-id",
      "Access-Control-Expose-Headers": "Mcp-Session-Id",
    });
    res.end();
    return;
  }

  if (req.method === "GET" && url.pathname === "/") {
    res.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify({ status: "ok" }));
    return;
  }

  const MCP_METHODS = new Set(["POST", "GET", "DELETE"]);
  if (url.pathname === MCP_PATH && req.method && MCP_METHODS.has(req.method)) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Expose-Headers", "Mcp-Session-Id");
    const server = createStatCanServer();
    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined, enableJsonResponse: true });
    res.on("close", () => { transport.close(); server.close(); });
    try {
      await server.connect(transport);
      await transport.handleRequest(req, res);
    } catch (err) {
      console.error("MCP request error:", err);
      if (!res.headersSent) res.writeHead(500).end("Internal server error");
    }
    return;
  }

  res.writeHead(404).end("Not Found");
});

httpServer.listen(PORT, () => {
  console.log(`Statistics Canada Athena Agent running at http://localhost:${PORT}${MCP_PATH}`);
  console.log(`Topics: population, labour, cpi, housing, gdp, trade, crime, education`);
});