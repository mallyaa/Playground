# Example manifests

Use these with the CLI or dashboard for realistic DAG, cost, and lint insights.

| File | Models | Use case |
|------|--------|----------|
| **sample_manifest.json** | 5 | Minimal demo (original). |
| **ecommerce_manifest.json** | 14 | E‑commerce pipeline: staging → intermediate → marts. Varied execution times (45s–420s), SELECT \*, no WHERE, one CROSS JOIN, nested subqueries. Good for cost variance and downstream impact. |
| **analytics_manifest.json** | 10 | Analytics/ERP: invoices, line items, customers → fct/dim/reports. Heavy fact table, CROSS JOIN in a report, excessive nested subqueries. |

Each manifest uses the optional **`execution_time_seconds`** field per model so cost and “top expensive” / “downstream impact” are meaningful.

**CLI:**

```bash
flakeguard --manifest examples/ecommerce_manifest.json --warehouse M .
flakeguard --manifest examples/analytics_manifest.json --warehouse S .
```

**Dashboard:** In the sidebar, set **Manifest path** to the full path to one of these files (e.g. `.../flakeguard/examples/ecommerce_manifest.json`).
