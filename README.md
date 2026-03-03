# CloudSyncContacts v0.1

基于规格书 `v0.1 + v0.2 + v0.3` 的可运行后端实现。

## 已实现能力
- `POST /api/v1/auth/register` 用户注册（邮箱/手机号 + 密码）
- `POST /api/v1/auth/login` 用户登录并获取 Bearer Token
- `POST /api/v1/contacts/batch` 批量上传联系人（最多 500 条）
- `GET /api/v1/contacts` 分页查询联系人
- `POST /api/v1/contacts/{id}/photo` 上传头像（请求体为图片二进制，压缩后 <=512KB）
- `POST /api/v1/sync` 双向同步（提交本地变更 + 返回服务端变更 + 冲突列表）
- `GET /api/v1/sync/changes?since={ts}&device_id={id}` 拉取增量变更
- `POST /api/v1/sync/ack` 确认客户端已应用变更
- `GET /api/v1/conflicts` 查询冲突列表（open/resolved/all）
- `POST /api/v1/conflicts/{conflict_id}/resolve` 冲突解决（保留本地/保留云端/手动合并）
- `GET /api/v1/contacts/{id}/history` 查询联系人历史快照
- `POST /api/v1/contacts/{id}/rollback` 回滚到指定历史版本
- `GET /health` 健康检查

## 本地运行
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## 快速验证
```bash
# 1) 注册
curl -sS -X POST http://127.0.0.1:8000/api/v1/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"email":"demo@example.com","password":"Password123"}'

# 2) 登录拿 token
TOKEN=$(curl -sS -X POST http://127.0.0.1:8000/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"demo@example.com","password":"Password123"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')

# 3) 批量上传
curl -sS -X POST http://127.0.0.1:8000/api/v1/contacts/batch \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"contacts":[{"local_id":"ios-1","display_name":"Alice","phone_numbers":[{"type":"mobile","value":"+8613800138000"}],"email_addresses":[{"type":"work","value":"alice@example.com"}],"postal_addresses":[],"organization":"ACME","job_title":"PM","notes":"seed","source_device_id":"iphone-15"}]}'

# 4) 查询列表
curl -sS "http://127.0.0.1:8000/api/v1/contacts?page=1&page_size=20" \
  -H "Authorization: Bearer $TOKEN"
```

## 注意
- 当前为 v0.3 后端可验证版本，未包含 iOS/Android 客户端实现。
- 数据库使用 SQLite（`contactsync.db`）以便快速启动；后续可迁移到 PostgreSQL。
- Token 使用 HS256 JWT 自实现，生产环境建议改为成熟库并加入 refresh token/密钥轮换。
