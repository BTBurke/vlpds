# common

The base a vlpds node gets first, on a fresh Ubuntu LTS VPS: a minimal
package set and the locale, the hostname, the operator account
(`operator_user`, its SSH keys, NOPASSWD sudo), sshd settings and
hardening, ufw, fail2ban, unattended security upgrades, Docker CE (pinned
versions, daemon-wide log rotation), syslog rotation and, optionally,
Tailscale. Variables and their types:
[`defaults/main.yml`](defaults/main.yml), [`meta/argument_specs.yml`](meta/argument_specs.yml).

- **Operator account**: `operator_user` (default `operator`) with
  `operator_ssh_keys`. Ansible logs in as this user after bootstrap.
  `operator_ssh_keys_absent` removes keys (dropping a key from
  `operator_ssh_keys` alone does not).
- **Hardening**, each part behind its own switch (all on except the password
  lock): `common_ssh_hardening_enabled` (key-only sshd drop-in for
  `common_ssh_allow_users`, checked with `sshd -t` before any reload and
  with a fresh key login after it; the controller-side check reads
  `common_ssh_known_hosts_file`), `common_lock_password_users` (e.g.
  `[ubuntu, root]`), `common_firewall_enabled` (ufw: SSH, 80, 443 tcp/udp
  and the trusted interfaces), `common_fail2ban_enabled`,
  `common_unattended_upgrades_enabled` (no automatic reboots).
  `playbooks/bootstrap.yml` runs the same task files (`users.yml`,
  `ssh_hardening.yml`, `lock_passwords.yml`, `firewall.yml`, `fail2ban.yml`,
  `unattended_upgrades.yml`) on a fresh VPS.
- **Pins**: `common_docker_version`, `common_containerd_version`,
  `common_docker_compose_version`, `common_docker_buildx_version`,
  `common_tailscale_version`. Raising one upgrades the package on the next
  run; a host newer than its pin fails the install (apt refuses the
  downgrade), so raise the pin instead. Docker/containerd upgrades restart
  every container.
- **Tailscale (optional)**: off by default. vlpds serves its operator
  console (`vlpds_tailnet_console_port`) and, in a cluster, its peer port
  over the tailnet with `tailscale serve`, so turn on `tailscale_enabled`
  for either. `tailscale up` runs only with a `tailscale_auth_key` (keep it
  in an encrypted vars file); otherwise run
  `sudo tailscale up --advertise-tags=tag:vlpds` on the host yourself.
  `common_tailscale_ssh` is kept in line on running hosts
  (`tailscale set --ssh`); note that Tailscale SSH bypasses sshd and its
  hardening, so the tailnet ACLs decide who may log in.
  `common_firewall_trusted_interfaces` lets everything in on `tailscale0`.
- Docker-published ports bypass ufw (Docker's own iptables chains): publish
  internal services on `127.0.0.1` only.
