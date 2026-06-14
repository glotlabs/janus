# FreeBSD Deployment

This guide deploys `strait-server` in a FreeBSD jail after you have already built
the production binary.

## Build

From the repository root on the build host:

```sh
cargo build --release --locked -p strait-server
```

The production binary is:

```sh
target/release/strait-server
```

Copy that binary into the target jail.

## Install Dependencies

Inside the jail:

```sh
pkg update
pkg install -y git ca_root_nss openssl
```

`git` is required because `strait-server` creates bare repositories and archives
source trees from them.

## Create User And Directories

```sh
pw groupadd strait
pw groupadd git
pw useradd strait -g strait -d /var/db/strait-server -s /usr/sbin/nologin
pw useradd git -g git -d /home/git -s /bin/sh

install -d -o strait -g git -m 0750 /var/db/strait-server
install -d -o strait -g git -m 2770 /var/db/strait-server/repos
install -d -o strait -g strait -m 0750 /var/db/strait-server/db
install -d -o git -g git -m 0750 /home/git
install -d -o root -g wheel -m 0755 /usr/local/bin
install -d -o root -g wheel -m 0755 /usr/local/libexec
install -d -o root -g wheel -m 0755 /usr/local/etc/strait-server
```

Install the binary:

```sh
install -m 0755 /path/to/strait-server /usr/local/bin/strait-server
```

## Create Server Config

Create `/usr/local/etc/strait-server/server.toml`:

```sh
SESSION_SECRET="$(openssl rand -base64 48)"

cat > /usr/local/etc/strait-server/server.toml <<EOF
data_dir = "/var/db/strait-server"
repos_dir = "/var/db/strait-server/repos"

[database]
path = "/var/db/strait-server/db/server.sqlite3"

[server]
listen = "127.0.0.1:8090"
public_base_url = "your-ci-hostname.example.com"

[control]
socket_path = "/var/run/strait_server/control.sock"
socket_mode = 0o660

[auth]
session_secret = "$SESSION_SECRET"
session_ttl_days = 7
session_cookie_secure = true
login_rate_limit_per_minute = 10

[scheduler]
poll_interval_ms = 2000
cancel_stuck_timeout_seconds = 30
max_cancel_retries = 3
max_infra_retries = 2

[runners]
healthcheck_interval_seconds = 30
connect_timeout_seconds = 5
request_timeout_seconds = 120

[runner_url_policy]
require_https = false
allow_credentials = true
allow_query = true
allow_fragment = true
allow_path = true
allow_localhost = true
allow_private_ips = true
allow_link_local_ips = true
allow_documentation_ips = true
allow_multicast_ips = true

[limits]
request_body_bytes = 1048576
runner_json_bytes = 4194304
runner_logs_bytes = 8388608
runner_artifact_bytes = 268435456
runner_error_bytes = 16384
server_artifact_bytes = 268435456
EOF

chown root:strait /usr/local/etc/strait-server/server.toml
chmod 0640 /usr/local/etc/strait-server/server.toml
```

`admin runner-key init` adds the required `[runner_auth]` section to this file.

```sh
/usr/local/bin/strait-server admin runner-key init --config /usr/local/etc/strait-server/server.toml
chown strait:git /var/db/strait-server
chown -R strait:strait /var/db/strait-server/db
chown -R strait:git /var/db/strait-server/repos
chmod 0750 /var/db/strait-server
chmod 0750 /var/db/strait-server/db
chmod 2770 /var/db/strait-server/repos
```

## Bootstrap Admin User

Run this before any users exist:

```sh
/usr/local/bin/strait-server admin bootstrap-admin \
  --username admin \
  --config /usr/local/etc/strait-server/server.toml
```

For non-interactive setup:

```sh
printf '%s\n' "$ADMIN_PASSWORD" | /usr/local/bin/strait-server admin bootstrap-admin \
  --username admin \
  --password-stdin \
  --config /usr/local/etc/strait-server/server.toml
```

## Create rc Service

Create `/usr/local/etc/rc.d/strait_server`:

```sh
cat > /usr/local/etc/rc.d/strait_server <<'EOF'
#!/bin/sh

# PROVIDE: strait_server
# REQUIRE: NETWORKING
# KEYWORD: shutdown

. /etc/rc.subr

name="strait_server"
rcvar="strait_server_enable"

load_rc_config "$name"

: ${strait_server_enable:="NO"}
: ${strait_server_user:="strait"}
: ${strait_server_config:="/usr/local/etc/strait-server/server.toml"}

pid_dir="/var/run/strait_server"
pidfile="${pid_dir}/strait_server.pid"
logfile="/var/log/strait_server.log"

command="/usr/sbin/daemon"
command_args="-p ${pidfile} -o ${logfile} env PATH=/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin /usr/local/bin/strait-server serve --config ${strait_server_config}"

start_precmd="strait_server_precmd"

strait_server_precmd()
{
    install -d -o "${strait_server_user}" -g git -m 2770 "${pid_dir}"
    touch "${logfile}"
    chown "${strait_server_user}:${strait_server_user}" "${logfile}"
}

run_rc_command "$1"
EOF

chmod 0755 /usr/local/etc/rc.d/strait_server
```

Enable and start:

```sh
sysrc strait_server_enable=YES
service strait_server start
service strait_server status
```

Check logs:

```sh
cat /var/log/strait_server.log
```

## Configure Git SSH

Install the forced-command wrapper:

```sh
cat > /usr/local/libexec/strait-git-ssh <<'EOF'
#!/bin/sh
set -eu

SOCKET="/var/run/strait_server/control.sock"

case "${SSH_ORIGINAL_COMMAND:-}" in
  "git-upload-pack "*)
    git_cmd="git-upload-pack"
    repo=${SSH_ORIGINAL_COMMAND#git-upload-pack }
    ;;
  "git-receive-pack "*)
    git_cmd="git-receive-pack"
    repo=${SSH_ORIGINAL_COMMAND#git-receive-pack }
    ;;
  *)
    echo "unsupported git command" >&2
    exit 1
    ;;
esac

repo=${repo#\'}
repo=${repo%\'}
repo=${repo#/}
repo=${repo%.git}

bare_path=$(/usr/local/bin/strait-server git resolve-repo \
  --socket-path "$SOCKET" \
  --repo "$repo")

export GIT_CONFIG_COUNT=1
export GIT_CONFIG_KEY_0=safe.directory
export GIT_CONFIG_VALUE_0="$bare_path"

exec "$git_cmd" "$bare_path"
EOF

chmod 0755 /usr/local/libexec/strait-git-ssh
```

Configure sshd:

```sshconfig
Match User git
    PubkeyAuthentication yes
    PasswordAuthentication no
    X11Forwarding no
    AllowTcpForwarding no
    PermitTTY no
    ForceCommand /usr/local/libexec/strait-git-ssh
```

Then restart sshd:

```sh
service sshd restart
```

Install SSH public keys for the `git` user in `/home/git/.ssh/authorized_keys`.
Keep the SSH home and key files owned by `git` so sshd strict mode accepts them:

```sh
install -d -o git -g git -m 0700 /home/git/.ssh
touch /home/git/.ssh/authorized_keys
chown -R git:git /home/git/.ssh
chmod 0600 /home/git/.ssh/authorized_keys
```

Verify the socket, repo resolution, and repository access:

```sh
ls -ld /var/run/strait_server
ls -l /var/run/strait_server/control.sock
su -m git -c '/usr/local/bin/strait-server git resolve-repo --repo your-repo-name'
su -m git -c 'repo=$(/usr/local/bin/strait-server git resolve-repo --repo your-repo-name) && GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=safe.directory GIT_CONFIG_VALUE_0="$repo" git --git-dir="$repo" rev-parse --git-dir'
```

The runtime directory should be owned by `strait:git` with mode `2770`, and the
control socket should be owned by `strait:git` with mode `0660`.

The `git` user does not need read access to the SQLite database or server
config. It resolves repository names through the local control socket, then runs
`git-upload-pack` or `git-receive-pack` against the resolved bare repository.

If any repositories existed before installing this wrapper, regenerate their
hooks:

```sh
/usr/local/bin/strait-server admin reconcile-hooks --config /usr/local/etc/strait-server/server.toml
```

## Verify As Service User

If the service does not start, first verify that the configured service user can
run the server:

```sh
su -m strait -c '/usr/local/bin/strait-server serve --config /usr/local/etc/strait-server/server.toml'
```

Stop it with `Ctrl-C` after the test.

If SQLite reports `attempt to write a readonly database`, fix ownership of the
database file and its parent directory. SQLite must be able to write sidecar
files such as `server.sqlite3-wal` or `server.sqlite3-journal`.

```sh
service strait_server stop
chown strait:git /var/db/strait-server
chmod 0750 /var/db/strait-server
chown -R strait:strait /var/db/strait-server/db
chown -R strait:git /var/db/strait-server/repos
chmod 2770 /var/db/strait-server/repos
find /var/db/strait-server/repos -type d -exec chmod g+rws {} +
find /var/db/strait-server/repos -type f -exec chmod g+rw {} +
chmod 0750 /var/db/strait-server/db
chmod 0640 /var/db/strait-server/db/server.sqlite3 2>/dev/null || true
```

## Reverse Proxy

The example config binds the app to:

```toml
listen = "127.0.0.1:8090"
```

Put a reverse proxy in front of that address, or change `listen` to an address
reachable from outside the jail.

Keep `session_cookie_secure = true` when serving over HTTPS. If you test the app
directly over plain HTTP, set it to `false` temporarily or login cookies will not
work in browsers.

`public_base_url` is used for clone URLs shown in the UI:

```toml
public_base_url = "your-ci-hostname.example.com"
```

Repository clone URLs are rendered as:

```text
ssh://git@your-ci-hostname.example.com/repo-name
```

## Moving The Binary Or Config

Repository `post-receive` hooks embed the absolute path to the `strait-server`
binary and the control socket path. Install the binary and choose the final
socket path before creating repositories.

If you move either path later, run:

```sh
/usr/local/bin/strait-server admin reconcile-hooks --config /usr/local/etc/strait-server/server.toml
```

## Runner Trust Key

Show the server public key snippet to add to runner configs:

```sh
/usr/local/bin/strait-server admin runner-key show --format toml --config /usr/local/etc/strait-server/server.toml
```
