# caddy

Caddy in a Docker container (`caddy_container_name`), configured from
`/etc/caddy/Caddyfile` (global options + `import sites/*.Caddyfile`) and one
file per entry in `caddy_sites`, each rendered from
`templates/<template>.Caddyfile.j2`. Variables and their types:
[`defaults/main.yml`](defaults/main.yml), [`meta/argument_specs.yml`](meta/argument_specs.yml).

The vlpds playbook (`playbooks/vlpds.yml`) runs it after the vlpds role and
derives every Caddy setting from the `vlpds_*` variables: the `vlpds-site`
template, the vlpds docker network, the global options (ACME email,
on-demand TLS `ask`, trusted proxies) and, for a wildcard certificate, the
Cloudflare DNS build with its token. Nothing here needs setting by hand for
a vlpds node.

- **`vlpds-site`**: the PDS hostname and `*.<handle domain>` proxied to
  vlpds, with node-to-node, metrics and admin paths answered 404, request
  body caps, and either on-demand TLS (each handle certificate checked
  against vlpds' `/tls-check`) or one wildcard certificate by DNS-01, with
  `/tls-check` asked per request instead.
- **Cloudflare DNS-01** (`caddy_cloudflare_dns: true`): the role builds
  `local/caddy-cloudflare:<caddy>-<module>` on the host from
  `templates/cloudflare.Dockerfile.j2` (only when the image is missing or the
  Dockerfile changed) and passes `caddy_env` (`CF_API_TOKEN`, a token with
  Zone:Zone:Read + Zone:DNS:Edit) as the container's environment, never
  written to disk; the task is `no_log` then.
- Changes to a site or the Caddyfile restart the container at the end of the
  play (handler `Reload caddy`; Caddy's admin API is off).
- The container joins `bridge` plus `caddy_docker_networks` and publishes
  `caddy_ports`. With `caddy_mount_tailscale_socket` it also mounts the
  tailscaled socket, for `*.ts.net` certificates.
