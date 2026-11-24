# 跨系统数据交换与批量传输平台

面向异构系统间批量数据交换的字段契约、增量窗口抽取、暂存落地、校验和对账、断点续传与到达核对平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/views/DeliveryView.vue       传输交付数据包页面
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/routers/delivery.py  传输交付数据包接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/transfer_delivery.py  交付包打包/下载/核验
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 源端登记 | `source_register` | 源端系统 | 源端编号、系统名称、数据域 |
| 目标端登记 | `target_register` | 目标端点 | 目标编号、系统名称、接收格式 |
| 字段契约 | `field_contract` | 字段映射 | 契约编号、源字段、目标字段 |
| 增量窗口 | `extract_window` | 抽取窗口 | 窗口编号、源端编号、增量字段 |
| 传输批次 | `transfer_batch` | 传输批次 | 批次编号、窗口编号、目标编号 |
| 暂存落地 | `staging_area` | 暂存分区 | 暂存编号、批次编号、落地路径 |
| 校验核对 | `checksum_verify` | 校验任务 | 校验编号、批次编号、源端校验值 |
| 到达对账 | `arrival_reconcile` | 对账单 | 对账编号、批次编号、应到条数 |
| 字段冲突 | `conflict_resolve` | 冲突记录 | 冲突编号、记录主键、冲突字段 |
| 断点续传 | `resume_point` | 续传断点 | 断点编号、批次编号、已完成分片 |
| 速率整形 | `speed_control` | 限速策略 | 策略编号、作用范围、每秒记录数 |
| 拒收回执 | `reject_record` | 拒收记录 | 拒收编号、批次编号、记录主键 |
| 格式适配 | `format_adapt` | 适配版本 | 适配编号、记录格式、旧版结构 |
| 传输血缘 | `transfer_lineage` | 血缘链路 | 链路编号、源端编号、抽取窗口 |
| 批次归档 | `history_archive` | 归档卷 | 归档编号、批次范围、归档路径 |
| 历史重放 | `exchange_replay` | 重放任务 | 重放编号、归档编号、重放范围 |
| 时效监控 | `sla_monitor` | 时效指标 | 指标编号、源端编号、约定到达时限 |
| 通道配置 | `channel_config` | 传输通道 | 通道编号、传输协议、并发连接数 |
| 传输交付数据包 | `transfer_delivery` | 交付包 | 交付编号、批次编号、采用回执、交付校验值 |

## 传输交付数据包

通用导出只输出模块登记清单（csv）。`transfer_delivery` 提供的是**实际下载包**：
按批号生成 zip，内含该批次的实际传输分片、`delivery_manifest.json` 清单和采用的
暂存确认回执，可用下载件逐片复算校验。

### 数据来源与一致性

- 传输批次执行时按分片大小把数据写成 `data/batches/<批次号>/chunk_*.jsonl`，并写下
  `chunks.json` 分片清单（序号、条数、字节数、逐片 SHA256）。
- 暂存落地按同样的分片边界写入 `data/staging/<暂存编号>/`，确认落地时读取**实际可读取
  的分片字节**生成暂存回执 `data/receipts/<批次号>/<暂存编号>.json`。同一批次可以先后
  多次暂存确认，第二次可以改用不同落地路径，先前回执不会被覆盖。
- 打包开始即冻结采用回执及其分片字节；打包期间即使同批次又完成了一次新路径暂存确认，
  也不会混入本包的数据或清单，只会作为“快照后观察记录”写进清单提示。

### 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/delivery/receipts?batch=` | 列出批次全部暂存回执（含先前回执） |
| POST | `/api/delivery/package` | 按批号打包：`{batch, receipt_code?, landing_path?, confirm_landing_path?}` |
| GET | `/api/delivery/deliveries?batch=` | 列出正式交付记录（不含中断尝试） |
| GET | `/api/delivery/{交付号}/download` | 按交付号下载已正式交付包（中断重下后仍可下载） |
| POST | `/api/delivery/{交付号}/verify` | 按交付号核验 |
| POST | `/api/delivery/{交付号}/landing-verify` | 目标端落地校验，给出另一轮待校验范围 |
| POST | `/api/delivery/verify-upload[?batch=]` | 上传下载件 zip 原始字节复算 |

### 关键语义

- 只输入批号即可打包，默认采用最新暂存回执；`confirm_landing_path=true` 时要求采用回执的
  落地路径必须等于 `landing_path`（成包前确认新路径）。
- 打包中断（含请求带 `interrupt_after_chunks`）不产生正式交付记录，只留审计记录；
  重做后按交付号可看到多次正式交付。
- 核验结果包含：清单自校验、逐片复算、采用回执与包内分片的关联、后续待校验范围
  （比采用回执更晚确认的回执，含包内快照后观察到的回执与当前回执账）。
- 若采用了较早回执，`another_round_required=true`，并给出另一轮校验的回执、落地路径与分片范围。


## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
