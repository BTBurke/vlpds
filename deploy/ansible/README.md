# Deploying vlpds with Ansible

An example deployment kit: one or more vlpds nodes on plain Ubuntu VPSes, each
running vlpds, Caddy (TLS, handle certificates) and Grafana Alloy (metrics and
logs) under Docker Compose, with Tailscale as the private network for the
operator console, metrics and the cluster's peer port.
[docs/operations/deploy.md](../../docs/operations/deploy.md) explains the
choices; this file is the checklist.

| Path | What |
|---|---|
| `playbooks/bootstrap.yml` | First run on a fresh VPS with only the provider's password login: creates the operator account, then hardens SSH, the firewall, fail2ban and unattended upgrades. |
| `playbooks/vlpds.yml` | Every later run: roles `common`, `vlpds`, `caddy`, `alloy`, one node at a time. |
| `roles/vlpds` | The node itself: config checks, host prep, secrets as files, compose, verify. Its [README](roles/vlpds/README.md) and `defaults/main.yml` document every variable. |
| `roles/common`, `roles/caddy`, `roles/alloy` | Base hardening and Docker, the TLS proxy, metrics and logs. |
| `inventories/example` | A single node serving `pds.example.com`. Copy it. |

## Steps

1. **Tools**: Ansible (ansible-core 2.21 or newer), [sops](https://github.com/getsops/sops)
   with an [age](https://github.com/FiloSottile/age) key, and `sshpass` for the
   bootstrap's one password login. Then, from this directory:

       ansible-galaxy collection install -r requirements.yml

2. **Inventory**: `cp -r inventories/example inventories/<name>` and edit
   `hosts.yml` (address), `group_vars/vlpds.yml` (hostname, bucket, image) and
   `group_vars/all/main.yml` (your SSH key, monitoring endpoint).
3. **Secrets**: `cp .sops.yaml.example .sops.yaml` with your age recipient,
   then `cp inventories/<name>/group_vars/vlpds.sops.yaml.example
   inventories/<name>/group_vars/vlpds.sops.yaml`, fill it in and
   `sops encrypt -i` it. Back up the KEK and the PLC rotation key offline:
   they are not in the bucket.
4. **Bootstrap** (once per fresh host; the provider's password in
   `.bootstrap-pw`, which is gitignored):

       ansible-playbook -i inventories/<name>/hosts.yml playbooks/bootstrap.yml --limit <node>

5. **DNS**: `A` records for the hostname and `*.<handle domain>`.
6. **Deploy**, the first time with the bucket probe:

       ansible-playbook -i inventories/<name>/hosts.yml playbooks/vlpds.yml --check --diff
       ansible-playbook -i inventories/<name>/hosts.yml playbooks/vlpds.yml -e vlpds_preflight_probe=true

7. **First account**: `docker exec vlpds vlpds admin create-invite-code` on the
   node, sign up or [migrate](../../docs/migration.md) an account, then set
   `vlpds_crawlers: ["bsky.network"]` and run the playbook again.

Upgrades: set `vlpds_image` to the new pinned tag and run the playbook (with
`--tags vlpds-deploy,vlpds-verify` for only the node). More nodes:
[Scaling and clustering](../../docs/operations/scaling-and-clustering.md).
