# API Payloads

## PATCH /api/v1/canvas/drafts/{draft_id} — Cartesian

`query_id` and `original_query` are optional — server derives them from the chart's source row if omitted.

```json
{
  "slots": [
    {
      "x": 0, "y": 0, "w": 6, "h": 4,
      "chart": {
        "chart_id": "0c7e9b4a-c3d6-4d2c-9c8a-7b1d5d8f9e10",
        "chart": { "type": "column" },
        "title": { "text": "Weekly GC Run Rate by Region" },
        "subtitle": { "text": "NTM Projects, Last 12 Weeks" },
        "description": "Central region leads at 0.25 sites/week.",
        "insight": "Central leads with 0.25 sites/week, 3x the rate of Northeast and South.",
        "script": "sql = 'SELECT rgn_region, COUNT(*) FROM ...'\nresult = run_sql(sql)",
        "sql_index": 1,
        "colors": ["#2E86AB", "#F18F01", "#A23B72", "#3B8EA5", "#C73E1D", "#6A994E"],
        "xAxis": { "categories": ["CENTRAL", "NORTHEAST", "SOUTH"], "title": { "text": "Region" } },
        "yAxis": { "title": { "text": "Sites / week" } },
        "series": [{ "name": "Run rate", "data": [0.25, 0.08, 0.08] }],
        "legend": { "enabled": true },
        "tooltip": { "valueSuffix": " sites/week" },
        "plotOptions": { "column": { "dataLabels": { "enabled": true } } }
      }
    }
  ]
}
```

## PATCH /api/v1/canvas/drafts/{draft_id} — Pie

```json
{
  "slots": [
    {
      "x": 6, "y": 0, "w": 6, "h": 4,
      "chart": {
        "chart_id": "1f4f9a2b-1a2c-4d5e-8f6a-9b0c1d2e3f40",
        "chart": { "type": "pie" },
        "title": { "text": "Pending Sites by Region" },
        "subtitle": { "text": "NTM, snapshot 2026-04-24" },
        "description": "Central holds 46% of pending sites.",
        "insight": "Central accounts for 46% of pending sites, followed by South (29%) and Northeast (25%).",
        "script": "sql = 'SELECT rgn_region, COUNT(*) FROM ...'\nresult = run_sql(sql)",
        "sql_index": 1,
        "colors": ["#2E86AB", "#F18F01", "#A23B72"],
        "series": [
          {
            "name": "Pending sites",
            "data": [
              { "name": "CENTRAL", "y": 46 },
              { "name": "SOUTH", "y": 29 },
              { "name": "NORTHEAST", "y": 25 }
            ]
          }
        ],
        "legend": { "enabled": true },
        "tooltip": { "pointFormat": "{point.name}: <b>{point.y}</b>" }
      }
    }
  ]
}
```

## POST /api/v1/templates

```json
{
  "user_id": "12",
  "draft_id": "a15b14c3-3264-4780-bdd1-94597528d2f3",
  "title": "Q2 NTM Run-Rate Report",
  "project_type": "NTM"
}
```

`title` and `project_type` are optional — server defaults to the draft's `name` and `project_type`.
Server pulls the canvas's slots and saves them as the template's selections by chart_id reference. Upsert by `draft_id` — same draft never produces a duplicate template.
