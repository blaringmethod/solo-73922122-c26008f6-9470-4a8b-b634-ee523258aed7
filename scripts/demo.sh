#!/usr/bin/env bash
# 端到端演示：整件发布 -> 幂等重试 -> 冲突拒绝 -> 取件 -> 差分发布（四层链/第五层拒绝）
#          -> 重建取件 -> 两类证明 -> 离线核验 -> 篡改拒绝。
# 用法：BASE=http://127.0.0.1:8001 bash scripts/demo.sh
set -euo pipefail

BASE="${BASE:-http://127.0.0.1:8001}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

cat > "$WORK/day1.csv" <<'CSV'
station,time,ph,do
ST01,2026-11-16T08:00,7.2,8.1
CSV
cat > "$WORK/day2.csv" <<'CSV'
station,time,ph,do
ST02,2026-11-16T08:00,7.0,8.4
CSV

echo "== 1. 首次发布 =="
curl -s -X POST "$BASE/releases" \
  -F station_id=ST01 -F sample_date=2026-11-16 -F order_no=ORD-001 \
  -F file=@"$WORK/day1.csv" | python3 -m json.tool

echo "== 2. 同单同输入重试（期望 reused=true）=="
curl -s -X POST "$BASE/releases" \
  -F station_id=ST01 -F sample_date=2026-11-16 -F order_no=ORD-001 \
  -F file=@"$WORK/day1.csv" | python3 -m json.tool

echo "== 3. 同单异输入（期望 HTTP 409）=="
curl -s -w "\nHTTP %{http_code}\n" -X POST "$BASE/releases" \
  -F station_id=ST01 -F sample_date=2026-11-16 -F order_no=ORD-001 \
  -F file=@"$WORK/day2.csv"

echo "== 4. 第二件发布 =="
curl -s -X POST "$BASE/releases" \
  -F station_id=ST02 -F sample_date=2026-11-16 -F order_no=ORD-002 \
  -F file=@"$WORK/day2.csv" | python3 -m json.tool

echo "== 5. 下载并比对原件 =="
curl -s "$BASE/releases/ST01/2026-11-16/ORD-001/download" -o "$WORK/back.csv"
cmp "$WORK/day1.csv" "$WORK/back.csv" && echo "字节一致"

# ---------------------------------------------------------------------------
# 差分发布：对 ORD-001 的基稿做字节区间替换（pH 7.2 -> 7.6，溶解氧 8.1 -> 8.3）
# ---------------------------------------------------------------------------
BASE_ID="$(python3 - "$BASE" <<'PY'
import json, sys, urllib.request
base = sys.argv[1]
with urllib.request.urlopen(
    f"{base}/releases/ST01/2026-11-16/ORD-001"
) as r:
    print(json.loads(r.read())["id"])
PY
)"
echo "== 6. 差分发布（基稿序号 $BASE_ID，两段非空区间替换）=="
# 区间偏移从实际基稿字节动态计算（左闭右开），避免手工数错位。
SEGMENTS="$(python3 - "$WORK/back.csv" <<'PY'
import base64, json, sys
base = open(sys.argv[1], "rb").read()
def repl(old: bytes, new: bytes) -> dict:
    start = base.index(old)
    return {"start": start, "end": start + len(old),
            "b64": base64.b64encode(new).decode()}
print(json.dumps([repl(b"7.2", b"7.6"), repl(b"8.1", b"8.3")]))
PY
)"
curl -s -X POST "$BASE/releases/delta" \
  -F station_id=ST01 -F sample_date=2026-11-16 -F order_no=ORD-001-R1 \
  -F base_release_id="$BASE_ID" -F segments="$SEGMENTS" | python3 -m json.tool

echo "== 7. 差分稿取件（服务端按链重建，比对预期全文）=="
cat > "$WORK/expected.csv" <<'CSV'
station,time,ph,do
ST01,2026-11-16T08:00,7.6,8.3
CSV
curl -s "$BASE/releases/ST01/2026-11-16/ORD-001-R1/download" \
  -o "$WORK/rebuilt.csv"
cmp "$WORK/expected.csv" "$WORK/rebuilt.csv" && echo "重建字节与预期一致"
# 基稿仍可原样取回，两份稿并存。
curl -s "$BASE/releases/ST01/2026-11-16/ORD-001/download" \
  -o "$WORK/base-again.csv"
cmp "$WORK/day1.csv" "$WORK/base-again.csv" && echo "基稿字节未受差分影响"

echo "== 8. 非法差分（区间重叠，期望 HTTP 422）=="
curl -s -w "\nHTTP %{http_code}\n" -X POST "$BASE/releases/delta" \
  -F station_id=ST01 -F sample_date=2026-11-16 -F order_no=ORD-BAD \
  -F base_release_id="$BASE_ID" \
  -F segments='[{"start":0,"end":5,"b64":"AAAA"},{"start":4,"end":6,"b64":"Qg=="}]'

echo "== 9. 纳入证明与离线核验（差分稿凭重建全文核验）=="
curl -s "$BASE/releases/ST01/2026-11-16/ORD-001-R1/inclusion-proof" \
  > "$WORK/inclusion-delta.json"
python3 -m app.verify inclusion \
  --proof "$WORK/inclusion-delta.json" --file "$WORK/rebuilt.csv"

echo "== 10. 追加证明（1 号 -> 最新）与离线核验 =="
curl -s "$BASE/checkpoints/1" > "$WORK/cp1.json"
curl -s "$BASE/checkpoints/latest" > "$WORK/cp-latest.json"
curl -s "$BASE/checkpoints/1/append-proof" > "$WORK/append.json"
python3 -m app.verify append \
  --proof "$WORK/append.json" \
  --old-checkpoint "$WORK/cp1.json" \
  --new-checkpoint "$WORK/cp-latest.json"

echo "== 11. 篡改原件（期望离线拒绝，退出码 1）=="
cp "$WORK/day1.csv" "$WORK/tampered.csv"
printf 'X' >> "$WORK/tampered.csv"
python3 -m app.verify inclusion \
  --proof "$WORK/inclusion-delta.json" --file "$WORK/tampered.csv" || true

