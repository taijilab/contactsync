#!/usr/bin/env bash
# CloudSyncContacts 集成测试
# 通过 Docker Compose 验证 PostgreSQL + Redis + API 端到端功能
#
# 用法:
#   bash scripts/integration_test.sh
#
# 前置条件:
#   - docker compose 可用
#   - curl, jq 已安装

set -euo pipefail

BASE_URL="http://localhost:8000"
PASS=0
FAIL=0

green()  { printf "\033[32m%s\033[0m\n" "$*"; }
red()    { printf "\033[31m%s\033[0m\n" "$*"; }
yellow() { printf "\033[33m%s\033[0m\n" "$*"; }

check() {
    local desc="$1" ok="$2"
    if [ "$ok" = "true" ]; then
        green "  PASS: $desc"
        PASS=$((PASS + 1))
    else
        red "  FAIL: $desc"
        FAIL=$((FAIL + 1))
    fi
}

# ---------- 1. 启动 Docker Compose ----------
echo ""
yellow "=== Step 1: 启动 Docker Compose ==="
docker compose up -d --build 2>&1 | tail -3

# ---------- 2. 等待 API 就绪 ----------
yellow "=== Step 2: 等待 API 就绪 (最多 90s) ==="
READY=false
for i in $(seq 1 90); do
    if curl -sf "$BASE_URL/health" > /dev/null 2>&1; then
        READY=true
        break
    fi
    sleep 1
done
check "API 健康检查" "$READY"
if [ "$READY" != "true" ]; then
    red "API 未就绪，终止测试"
    docker compose logs api | tail -30
    docker compose down -v 2>/dev/null
    exit 1
fi

# ---------- 3. 注册测试用户 ----------
yellow "=== Step 3: 注册测试用户 ==="
REGISTER_RESP=$(curl -sf -X POST "$BASE_URL/api/v1/auth/register" \
    -H "Content-Type: application/json" \
    -d "{\"email\": \"inttest-$(date +%s)@example.com\", \"password\": \"Password123\"}")

TOKEN=$(echo "$REGISTER_RESP" | jq -r '.access_token')
check "注册返回 access_token" "$([ -n "$TOKEN" ] && [ "$TOKEN" != "null" ] && echo true || echo false)"

AUTH="Authorization: Bearer $TOKEN"

# ---------- 4. 批量上传联系人 (含 2 条重复电话) ----------
yellow "=== Step 4: 批量上传 10 条联系人 ==="
BATCH_RESP=$(curl -sf -X POST "$BASE_URL/api/v1/contacts/batch" \
    -H "Content-Type: application/json" \
    -H "$AUTH" \
    -d '{
    "contacts": [
        {
            "local_id": "c1",
            "display_name": "Alice Zhang",
            "phone_numbers": [{"type": "mobile", "value": "+86 138-0013-8000"}],
            "email_addresses": [{"type": "work", "value": "alice@example.com"}],
            "postal_addresses": [], "organization": "CloudSync", "job_title": "Engineer", "notes": ""
        },
        {
            "local_id": "c2",
            "display_name": "Alice Z.",
            "phone_numbers": [{"type": "mobile", "value": "0138 0013 8000"}],
            "email_addresses": [{"type": "personal", "value": "alice.z@gmail.com"}],
            "postal_addresses": [], "organization": "CloudSync", "job_title": "", "notes": ""
        },
        {
            "local_id": "c3",
            "display_name": "Bob Li",
            "phone_numbers": [{"type": "mobile", "value": "+1 202 555 0100"}],
            "email_addresses": [{"type": "work", "value": "bob@example.com"}],
            "postal_addresses": [], "organization": "CloudSync", "job_title": "PM", "notes": ""
        },
        {
            "local_id": "c4",
            "display_name": "Carol Wang",
            "phone_numbers": [{"type": "mobile", "value": "+86 139-0000-1111"}],
            "email_addresses": [{"type": "work", "value": "carol@example.com"}],
            "postal_addresses": [], "organization": "TestCorp", "job_title": "Designer", "notes": ""
        },
        {
            "local_id": "c5",
            "display_name": "David Chen",
            "phone_numbers": [{"type": "mobile", "value": "+86 150-1234-5678"}],
            "email_addresses": [{"type": "work", "value": "david@example.com"}],
            "postal_addresses": [], "organization": "", "job_title": "", "notes": "test contact"
        },
        {
            "local_id": "c6",
            "display_name": "Eva Lin",
            "phone_numbers": [{"type": "mobile", "value": "+1 415 555 0200"}],
            "email_addresses": [{"type": "work", "value": "eva@example.com"}],
            "postal_addresses": [], "organization": "CloudSync", "job_title": "CTO", "notes": ""
        },
        {
            "local_id": "c7",
            "display_name": "Frank Wu",
            "phone_numbers": [{"type": "mobile", "value": "+86 188-8888-9999"}],
            "email_addresses": [{"type": "work", "value": "frank@example.com"}],
            "postal_addresses": [], "organization": "", "job_title": "", "notes": ""
        },
        {
            "local_id": "c8",
            "display_name": "Grace Liu",
            "phone_numbers": [{"type": "mobile", "value": "+86 177-0000-2222"}],
            "email_addresses": [{"type": "work", "value": "grace@example.com"}],
            "postal_addresses": [], "organization": "TestCorp", "job_title": "QA", "notes": ""
        },
        {
            "local_id": "c9",
            "display_name": "Henry Zhao",
            "phone_numbers": [{"type": "mobile", "value": "+1 650 555 0300"}],
            "email_addresses": [{"type": "work", "value": "henry@example.com"}],
            "postal_addresses": [], "organization": "", "job_title": "", "notes": ""
        },
        {
            "local_id": "c10",
            "display_name": "Ivy Xu",
            "phone_numbers": [{"type": "mobile", "value": "+86 166-1111-3333"}],
            "email_addresses": [{"type": "work", "value": "ivy@example.com"}],
            "postal_addresses": [], "organization": "CloudSync", "job_title": "Dev", "notes": ""
        }
    ]
}')

CREATED=$(echo "$BATCH_RESP" | jq -r '.created')
check "批量上传创建 10 条联系人" "$([ "$CREATED" = "10" ] && echo true || echo false)"

# ---------- 5. 查询联系人列表 ----------
yellow "=== Step 5: 查询联系人列表 (PostgreSQL 读取) ==="
LIST_RESP=$(curl -sf "$BASE_URL/api/v1/contacts?page=1&page_size=20" -H "$AUTH")
TOTAL=$(echo "$LIST_RESP" | jq -r '.total')
ITEMS=$(echo "$LIST_RESP" | jq -r '.items | length')
check "联系人总数 = 10" "$([ "$TOTAL" = "10" ] && echo true || echo false)"
check "返回条目数 = 10" "$([ "$ITEMS" = "10" ] && echo true || echo false)"

# ---------- 6. 再次查询 (Redis 缓存路径) ----------
yellow "=== Step 6: 再次查询联系人列表 (Redis 缓存) ==="
LIST_RESP2=$(curl -sf "$BASE_URL/api/v1/contacts?page=1&page_size=5" -H "$AUTH")
TOTAL2=$(echo "$LIST_RESP2" | jq -r '.total')
ITEMS2=$(echo "$LIST_RESP2" | jq -r '.items | length')
check "缓存后总数仍 = 10" "$([ "$TOTAL2" = "10" ] && echo true || echo false)"
check "分页返回 5 条" "$([ "$ITEMS2" = "5" ] && echo true || echo false)"

# ---------- 7. 去重候选检测 ----------
yellow "=== Step 7: 去重候选检测 (倒排索引算法) ==="
DEDUPE_RESP=$(curl -sf "$BASE_URL/api/v1/dedupe/candidates?min_score=75" -H "$AUTH")
DEDUPE_COUNT=$(echo "$DEDUPE_RESP" | jq -r '.count')
check "检测到重复候选 >= 1 (c1 与 c2 共享电话)" "$([ "$DEDUPE_COUNT" -ge 1 ] 2>/dev/null && echo true || echo false)"

# 检查候选对包含 c1|c2
PAIR_KEY=$(echo "$DEDUPE_RESP" | jq -r '.items[0].pair_key // ""')
HAS_C1_C2=$(echo "$PAIR_KEY" | grep -c "c1" || true)
check "候选对包含 c1" "$([ "$HAS_C1_C2" -ge 1 ] && echo true || echo false)"

# ---------- 8. 增量同步 ----------
yellow "=== Step 8: 增量同步 ==="
SYNC_RESP=$(curl -sf -X POST "$BASE_URL/api/v1/sync" \
    -H "Content-Type: application/json" \
    -H "$AUTH" \
    -d "{
        \"last_sync_time\": \"2020-01-01T00:00:00+00:00\",
        \"device_id\": \"test-device-1\",
        \"local_changes\": [{
            \"op\": \"upsert\",
            \"local_id\": \"sync-1\",
            \"display_name\": \"Sync Contact\",
            \"phone_numbers\": [{\"type\": \"mobile\", \"value\": \"+86 199-0000-0001\"}],
            \"email_addresses\": [{\"type\": \"work\", \"value\": \"sync1@example.com\"}],
            \"postal_addresses\": [],
            \"organization\": \"\", \"job_title\": \"\", \"notes\": \"\"
        }]
    }")

LOCAL_APPLIED=$(echo "$SYNC_RESP" | jq -r '.local_applied')
check "同步新增 1 条联系人" "$([ "$LOCAL_APPLIED" = "1" ] && echo true || echo false)"

# ---------- 9. METRICS 验证 ----------
yellow "=== Step 9: METRICS 键验证 (路由模式 vs 实际路径) ==="
METRICS_RESP=$(curl -sf "$BASE_URL/api/v1/metrics" -H "$AUTH")
TOTAL_REQ=$(echo "$METRICS_RESP" | jq -r '.total_requests')
check "总请求数 >= 5" "$([ "$TOTAL_REQ" -ge 5 ] 2>/dev/null && echo true || echo false)"

# 检查 endpoints 键使用路由模式 (不含具体 ID)
ENDPOINT_KEYS=$(echo "$METRICS_RESP" | jq -r '.endpoints | keys[]' 2>/dev/null || echo "")
HAS_PATTERN=$(echo "$ENDPOINT_KEYS" | grep -c "contacts" || true)
check "METRICS 包含 contacts 端点" "$([ "$HAS_PATTERN" -ge 1 ] && echo true || echo false)"

# ---------- 10. 性能基线 (可选，仅 500 联系人快速跑) ----------
yellow "=== Step 10: 性能基线 (500 联系人) ==="
PERF_OUTPUT=$(python3 scripts/perf_test.py \
    --base-url "$BASE_URL" \
    --contacts 500 \
    --batch-size 100 \
    --sync-changes 100 \
    --workers 4 2>&1) || true
echo "$PERF_OUTPUT" | tail -20

UPLOAD_THROUGHPUT=$(echo "$PERF_OUTPUT" | python3 -c "
import sys, json
for line in sys.stdin:
    line = line.strip()
    if not line or line[0] != '{': continue
    try:
        d = json.loads(line)
        if d.get('name') == 'full_upload':
            print(d.get('throughput_contacts_per_sec', 0))
            break
    except: pass
" 2>/dev/null || echo "0")

check "上传吞吐量 > 0" "$(python3 -c "print('true' if float('${UPLOAD_THROUGHPUT:-0}') > 0 else 'false')" 2>/dev/null || echo false)"

# ---------- 结果汇总 ----------
echo ""
yellow "=============================="
yellow "  测试结果汇总"
yellow "=============================="
green "  通过: $PASS"
if [ "$FAIL" -gt 0 ]; then
    red "  失败: $FAIL"
else
    echo "  失败: 0"
fi
echo ""

# ---------- 11. 清理 ----------
yellow "=== Step 11: 清理 Docker Compose ==="
docker compose down -v 2>&1 | tail -3

if [ "$FAIL" -gt 0 ]; then
    red "存在失败用例，请检查日志"
    exit 1
fi

green "全部通过!"
