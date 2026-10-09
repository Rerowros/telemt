// Renders the bench matrix cells into a markdown summary table.
// usage: node bench-report.mjs <prefix> <artifactsDir>
import fs from "node:fs";
import path from "node:path";

const [prefix, dir = "artifacts"] = process.argv.slice(2);
if (!prefix) {
  console.error("usage: node bench-report.mjs <prefix> <artifactsDir>");
  process.exit(2);
}

const rows = fs
  .readdirSync(dir)
  .filter(
    (name) => name.startsWith(`bench-${prefix}-`) && name.endsWith(".json"),
  )
  .sort()
  .map((name) => {
    const cell = JSON.parse(fs.readFileSync(path.join(dir, name), "utf8"));
    const port = new URL(cell.edge).port || "443";
    const edge = port === "8443" || port === "10443" ? "h2" : "h1.1";
    return {
      cell: cell.label,
      ok: cell.ok,
      up: cell.uplink ? cell.uplink.mbps : "fail",
      down: cell.downlink ? cell.downlink.mbps : "fail",
      p50i: cell.pingIdle ? cell.pingIdle.p50.toFixed(0) : "fail",
      p95i: cell.pingIdle ? cell.pingIdle.p95.toFixed(0) : "fail",
      p50l: cell.pingLoad ? cell.pingLoad.p50.toFixed(0) : "fail",
      p95l: cell.pingLoad ? cell.pingLoad.p95.toFixed(0) : "fail",
      edge,
    };
  });

const header =
  "| cell | edge | up MiB/s | down MiB/s | ping idle p50/p95 | ping load p50/p95 | ok |\n" +
  "|---|---|---|---|---|---|---|\n";
const body = rows
  .map(
    (r) =>
      `| ${r.cell} | ${r.edge} | ${r.up} | ${r.down} | ${r.p50i}/${r.p95i} | ${r.p50l}/${r.p95l} | ${r.ok} |\n`,
  )
  .join("");
const md = `# bench ${prefix}\n\n${header}${body}`;
fs.writeFileSync(path.join(dir, `bench-${prefix}.md`), md);
console.log(md);
