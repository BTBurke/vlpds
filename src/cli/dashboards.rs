//! `vlpds dashboards`: the Grafana dashboards, so an operator with only the
//! image or the binary can import them. Embedded from the import-ready copy
//! that bench/obs/grafana/gen_dashboard.py writes (its `--check` keeps the
//! copy current); docs/operations/monitoring.md "Import into your own
//! Grafana".

use anyhow::{Context, Result};
use serde_json::{json, Value as J};

/// (--name, file name, import-ready JSON).
pub const DASHBOARDS: [(&str, &str, &str); 2] = [
    ("vlpds", "vlpds.json", include_str!("../../bench/obs/grafana/dashboards/vlpds.json")),
    ("internals", "vlpds-internals.json", include_str!("../../bench/obs/grafana/dashboards/vlpds-internals.json")),
];

/// The dashboard as embedded, or with `datasource_uid` (a Prometheus
/// datasource uid, or `default`) pre-selected in its Prometheus picker and
/// `__inputs` dropped: file provisioning never fills `__inputs`.
pub fn render(embedded: &str, datasource_uid: Option<&str>) -> Result<String> {
    let Some(uid) = datasource_uid else {
        return Ok(embedded.to_string());
    };
    let mut d: J = serde_json::from_str(embedded).context("embedded dashboard")?;
    let obj = d.as_object_mut().context("dashboard is not an object")?;
    obj.remove("__inputs");
    let vars = obj
        .get_mut("templating")
        .and_then(|t| t.get_mut("list"))
        .and_then(J::as_array_mut)
        .context("dashboard has no templating list")?;
    let picker =
        vars.iter_mut().find(|v| v["name"] == "ds_prometheus").context("dashboard has no ds_prometheus variable")?;
    picker["current"] = json!({"selected": false, "text": uid, "value": uid});
    Ok(serde_json::to_string_pretty(&d)? + "\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_are_import_ready() {
        for (name, _, body) in DASHBOARDS {
            let d: J = serde_json::from_str(body).unwrap();
            assert_eq!(d["__inputs"][0]["name"], "DS_PROMETHEUS", "{name}");
            assert!(body.contains("${ds_prometheus}"), "{name}");
        }
    }

    #[test]
    fn datasource_uid_preselects_and_drops_inputs() {
        for (name, _, body) in DASHBOARDS {
            let out = render(body, Some("my-prom")).unwrap();
            let d: J = serde_json::from_str(&out).unwrap();
            assert!(d.get("__inputs").is_none(), "{name}");
            assert!(!out.contains("${DS_PROMETHEUS}"), "{name}");
            let picker =
                d["templating"]["list"].as_array().unwrap().iter().find(|v| v["name"] == "ds_prometheus").unwrap();
            assert_eq!(picker["current"]["value"], "my-prom", "{name}");
        }
    }
}
