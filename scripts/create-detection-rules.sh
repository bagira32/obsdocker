#!/usr/bin/env bash
# Create (or update) Kibana Security detection rules for auditd events.
# Idempotent: uses rule_id as a stable key — re-running updates existing rules in place.
set -euo pipefail

KIBANA_URL="${KIBANA_URL:-http://localhost:5601}"
KIBANA_CREDS="${KIBANA_CREDS:-elastic:${ELASTIC_PASSWORD:-changeme}}"

echo "Waiting for Kibana at $KIBANA_URL ..."
for i in {1..60}; do
  if curl -sf -u "$KIBANA_CREDS" "$KIBANA_URL/api/status" >/dev/null 2>&1; then break; fi
  sleep 2
done

# Create or update a rule: POST if new, PUT if already exists (keyed by rule_id)
upsert_rule() {
  local name="$1"
  local payload="$2"
  local rule_id
  rule_id=$(echo "$payload" | python3 -c "import sys,json; print(json.load(sys.stdin)['rule_id'])")

  # Check if rule exists
  local exists_code
  exists_code=$(curl -s -o /dev/null -w "%{http_code}" -u "$KIBANA_CREDS" \
    "$KIBANA_URL/api/detection_engine/rules?rule_id=$rule_id")

  local method="POST"
  [[ "$exists_code" == "200" ]] && method="PUT"

  local http_code
  http_code=$(curl -s -o /dev/null -w "%{http_code}" -u "$KIBANA_CREDS" \
    -X "$method" "$KIBANA_URL/api/detection_engine/rules" \
    -H 'kbn-xsrf: true' \
    -H 'Content-Type: application/json' \
    -d "$payload")

  if [[ "$http_code" =~ ^2 ]]; then
    echo "  [ok] $name"
  else
    echo "  [fail:$http_code] $name" >&2
  fi
}

echo ""
echo "Registering auditd detection rules..."
echo ""

# ---------------------------------------------------------------------------
# 1. Failed authentication
# Fires when auditd records a USER_AUTH event with res=failed.
# Repeated hits indicate brute-force or credential stuffing.
# ---------------------------------------------------------------------------
upsert_rule "[auditd] Failed Authentication" '{
  "rule_id": "auditd-failed-authentication",
  "type": "query",
  "language": "kuery",
  "query": "audit_type: \"USER_AUTH\" and message: *res=failed*",
  "index": ["logs-fluentbit-*"],
  "name": "[auditd] Failed Authentication",
  "description": "Detects failed authentication attempts recorded by auditd (USER_AUTH res=failed). Repeated alerts from the same source may indicate brute-force activity.",
  "severity": "medium",
  "risk_score": 47,
  "enabled": true,
  "interval": "1m",
  "from": "now-65s",
  "max_signals": 100,
  "threat": [{
    "framework": "MITRE ATT&CK",
    "tactic": {"id":"TA0006","name":"Credential Access","reference":"https://attack.mitre.org/tactics/TA0006/"},
    "technique": [{"id":"T1110","name":"Brute Force","reference":"https://attack.mitre.org/techniques/T1110/"}]
  }]
}'

# ---------------------------------------------------------------------------
# 2. Direct root login
# Fires on USER_LOGIN events where uid=0 (root logged in interactively).
# Root should never log in directly in a hardened environment.
# ---------------------------------------------------------------------------
upsert_rule "[auditd] Direct Root Login" '{
  "rule_id": "auditd-direct-root-login",
  "type": "query",
  "language": "kuery",
  "query": "audit_type: \"USER_LOGIN\" and message: *uid=0*",
  "index": ["logs-fluentbit-*"],
  "name": "[auditd] Direct Root Login",
  "description": "Detects a successful interactive login as root (uid=0). In hardened environments root login should be disabled; any hit here warrants investigation.",
  "severity": "high",
  "risk_score": 73,
  "enabled": true,
  "interval": "1m",
  "from": "now-65s",
  "max_signals": 100,
  "threat": [{
    "framework": "MITRE ATT&CK",
    "tactic": {"id":"TA0001","name":"Initial Access","reference":"https://attack.mitre.org/tactics/TA0001/"},
    "technique": [{"id":"T1078","name":"Valid Accounts","reference":"https://attack.mitre.org/techniques/T1078/"}]
  }]
}'

# ---------------------------------------------------------------------------
# 3. Sudo / privileged command execution
# Every sudo invocation generates a USER_CMD record with the full command.
# Low severity on its own but feeds into escalation chains.
# ---------------------------------------------------------------------------
upsert_rule "[auditd] Sudo Command Executed" '{
  "rule_id": "auditd-sudo-command",
  "type": "query",
  "language": "kuery",
  "query": "audit_type: \"USER_CMD\"",
  "index": ["logs-fluentbit-*"],
  "name": "[auditd] Sudo Command Executed",
  "description": "Fires on every sudo invocation. The message field contains the full command string. Low severity individually; correlate with USER_AUTH failures for privilege escalation signals.",
  "severity": "low",
  "risk_score": 21,
  "enabled": true,
  "interval": "1m",
  "from": "now-65s",
  "max_signals": 200,
  "threat": [{
    "framework": "MITRE ATT&CK",
    "tactic": {"id":"TA0004","name":"Privilege Escalation","reference":"https://attack.mitre.org/tactics/TA0004/"},
    "technique": [{"id":"T1548.003","name":"Sudo and Sudo Caching","reference":"https://attack.mitre.org/techniques/T1548/003/"}]
  }]
}'

# ---------------------------------------------------------------------------
# 4. New local user account created
# ADD_USER fires when useradd/adduser creates a new account.
# Unexpected account creation is a common persistence technique.
# ---------------------------------------------------------------------------
upsert_rule "[auditd] New User Account Created" '{
  "rule_id": "auditd-new-user-created",
  "type": "query",
  "language": "kuery",
  "query": "audit_type: \"ADD_USER\"",
  "index": ["logs-fluentbit-*"],
  "name": "[auditd] New User Account Created",
  "description": "Fires when a new local user account is created (useradd/adduser). Unexpected account creation is a common attacker persistence technique (MITRE T1136.001).",
  "severity": "medium",
  "risk_score": 47,
  "enabled": true,
  "interval": "1m",
  "from": "now-65s",
  "max_signals": 100,
  "threat": [{
    "framework": "MITRE ATT&CK",
    "tactic": {"id":"TA0003","name":"Persistence","reference":"https://attack.mitre.org/tactics/TA0003/"},
    "technique": [{"id":"T1136.001","name":"Create Account: Local Account","reference":"https://attack.mitre.org/techniques/T1136/001/"}]
  }]
}'

# ---------------------------------------------------------------------------
# 5. User account deleted
# DEL_USER fires when userdel removes an account.
# Attackers sometimes delete accounts to cover tracks or disrupt access.
# ---------------------------------------------------------------------------
upsert_rule "[auditd] User Account Deleted" '{
  "rule_id": "auditd-user-deleted",
  "type": "query",
  "language": "kuery",
  "query": "audit_type: \"DEL_USER\"",
  "index": ["logs-fluentbit-*"],
  "name": "[auditd] User Account Deleted",
  "description": "Fires when a local user account is deleted (userdel). May indicate an attacker removing evidence or disrupting legitimate access.",
  "severity": "high",
  "risk_score": 73,
  "enabled": true,
  "interval": "1m",
  "from": "now-65s",
  "max_signals": 100,
  "threat": [{
    "framework": "MITRE ATT&CK",
    "tactic": {"id":"TA0040","name":"Impact","reference":"https://attack.mitre.org/tactics/TA0040/"},
    "technique": [{"id":"T1531","name":"Account Access Removal","reference":"https://attack.mitre.org/techniques/T1531/"}]
  }]
}'

# ---------------------------------------------------------------------------
# 6. Audit configuration tampered
# Matches events tagged key=auditconfig or key=audittools — the watch rules
# in /etc/audit/rules.d fire these when audit config files or binaries are
# accessed/modified.
# ---------------------------------------------------------------------------
upsert_rule "[auditd] Audit Configuration Tampered" '{
  "rule_id": "auditd-config-tampered",
  "type": "query",
  "language": "kuery",
  "query": "message: *key=auditconfig* or message: *key=audispconfig*",
  "index": ["logs-fluentbit-*"],
  "name": "[auditd] Audit Configuration Tampered",
  "description": "Fires when audit configuration files (/etc/audit, /etc/audisp) are written or their attributes changed. Modifying audit config is a classic defense evasion move.",
  "severity": "high",
  "risk_score": 73,
  "enabled": true,
  "interval": "1m",
  "from": "now-65s",
  "max_signals": 100,
  "threat": [{
    "framework": "MITRE ATT&CK",
    "tactic": {"id":"TA0005","name":"Defense Evasion","reference":"https://attack.mitre.org/tactics/TA0005/"},
    "technique": [{"id":"T1562.001","name":"Disable or Modify Tools","reference":"https://attack.mitre.org/techniques/T1562/001/"}]
  }]
}'

# ---------------------------------------------------------------------------
# 7. Audit tools accessed (auditctl, ausearch, aureport)
# The watch rules tag reads of audit binaries with key=audittools.
# An attacker reading audit state before acting is a reconnaissance signal.
# ---------------------------------------------------------------------------
upsert_rule "[auditd] Audit Tools Accessed" '{
  "rule_id": "auditd-tools-accessed",
  "type": "query",
  "language": "kuery",
  "query": "message: *key=audittools*",
  "index": ["logs-fluentbit-*"],
  "name": "[auditd] Audit Tools Accessed",
  "description": "Fires when audit binaries (auditctl, ausearch, aureport) are executed or the audit log directory is read. An attacker probing the audit subsystem before tampering generates this signal.",
  "severity": "medium",
  "risk_score": 47,
  "enabled": true,
  "interval": "1m",
  "from": "now-65s",
  "max_signals": 100,
  "threat": [{
    "framework": "MITRE ATT&CK",
    "tactic": {"id":"TA0007","name":"Discovery","reference":"https://attack.mitre.org/tactics/TA0007/"},
    "technique": [{"id":"T1082","name":"System Information Discovery","reference":"https://attack.mitre.org/techniques/T1082/"}]
  }]
}'

# ---------------------------------------------------------------------------
# 8. Auditd daemon stopped
# DAEMON_END fires when the auditd process exits. Stopping the audit daemon
# is a common attacker move to blind the system before noisy operations.
# ---------------------------------------------------------------------------
upsert_rule "[auditd] Audit Daemon Stopped" '{
  "rule_id": "auditd-daemon-stopped",
  "type": "query",
  "language": "kuery",
  "query": "audit_type: \"DAEMON_END\"",
  "index": ["logs-fluentbit-*"],
  "name": "[auditd] Audit Daemon Stopped",
  "description": "Fires when the auditd daemon exits (DAEMON_END). Stopping audit logging before performing malicious actions is a textbook defense evasion technique.",
  "severity": "critical",
  "risk_score": 99,
  "enabled": true,
  "interval": "1m",
  "from": "now-65s",
  "max_signals": 100,
  "threat": [{
    "framework": "MITRE ATT&CK",
    "tactic": {"id":"TA0005","name":"Defense Evasion","reference":"https://attack.mitre.org/tactics/TA0005/"},
    "technique": [{"id":"T1562.001","name":"Disable or Modify Tools","reference":"https://attack.mitre.org/techniques/T1562/001/"}]
  }]
}'

echo ""
echo "Done. View rules at $KIBANA_URL/app/security/rules"
