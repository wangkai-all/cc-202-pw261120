# 表结构迁移工作台：单一地址字段 → 结构化地址

一个**零依赖、本地运行**的 Web 应用，演示如何把旧表的单一 `address_raw` 字段安全迁移为
结构化地址投影，同时满足：

- 迁移期间旧版（v1）与新版（v2）消费者都可查询；
- 新增/修订数据在**同一事务**内按同一确定规则写入两套投影，不使用数据库触发器，
  不存在“触发顺序”造成的差异；
- 历史回填带**进度检查点**，进程中断（含 `SIGKILL`）后可从检查点续跑；
- 增量更新与回填撞到同一记录时，**以业务版本号仲裁**，旧回填不得覆盖新修改；
- 无法可靠拆分的地址保留为 `pending/partial`，只保存原文中真实存在的片段，**不编造字段**；
- 发布前核对主键、外键引用、投影覆盖率、版本一致、规则确定性与关键查询，
  存在硬性失败时禁止切换；支持一键切换或撤回。

## 技术栈

Python 3.11 标准库（`http.server` + `sqlite3`）+ 单文件原生 HTML/JS，无任何第三方依赖、
无构建步骤、无外部网络请求。

## 目录结构

```
app/
  parser.py     确定性地址拆分器（纯函数；同一输入永远同一输出；逐字提取不编造）
  db.py         SQLite 双投影表结构、检查点表、审计日志、崩溃启动恢复
  service.py    同事务双写、乐观版本、回填工作线程、冲突仲裁、人工处理、查询
  preflight.py  发布前核对（7 项检查）
  server.py     HTTP JSON API 与静态页面
web/index.html  单页工作台（5 个标签页）
run_server.py   入口脚本（支持 python -I 隔离模式）
tests/
  test_workbench.py      17 个单元/HTTP 测试
  test_process_crash.py  真实进程 SIGKILL 中断续跑端到端测试
```

## 运行

```bash
python3 -I run_server.py
# 打开 http://127.0.0.1:8080/
# 可选环境变量：PORT / HOST / MIGRATION_DB
```

## 推荐操作流程（对应页面五个标签页）

1. **总览与回填**
   - “写入 15 条历史样例”：模拟迁移开始前只有旧投影的旧库（含格式不完整地址）。
   - “启动回填”或“慢速回填”：观察检查点 `last_id / processed` 推进；
     可随时“中断（保留检查点）”，再点“启动回填”续跑。
2. **在线写入 / 并发修订**
   - 在线新增走同事务双写；“模拟并发修订”让两笔基于同一版本的修订竞争，只有一笔成功，
     另一笔收到 409；“提交过期回填”演示回填快照过期被拒。
   - 慢速回填运行期间在线修订一条尚未回填的记录，可在时间线看到 `backfill_skip`，
     检查点 `skipped_newer` 增加。
3. **双投影数据**：逐行对照 v1 原文、v2 结构化字段、解析状态与投影版本。
4. **待处理地址**：对 `pending/partial` 记录做人工确认；只保存显式填写的字段，留空不补全，
   状态置为 `manual`。
5. **比较与发布**：单记录两版输出比较 + 规则现算一致性核对；运行发布前核对；
   在 `legacy → dual → structured` 之间切换，任何阶段可撤回 `legacy`。

### 进程中断演示（独立脚本）

```bash
python3 -I tests/test_process_crash.py
```

脚本会启动真实服务、灌入数据、在回填中途 `kill -9` 进程，再启动新进程验证检查点续跑、
投影完整与发布核对通过。

## 测试

```bash
python3 -I -m unittest discover -s tests -v
python3 -I tests/test_process_crash.py
```

## 核心设计说明

### 双投影与确定性双写

- `customers`（旧投影）保留 `address_raw` 原始值与单调递增的 `version`；
- `customer_addresses`（新投影）保存 8 个结构化字段、`parse_status` 与 `source_version`；
- 所有写入都在 `service.write_customer` 的**一个 `BEGIN IMMEDIATE` 事务**内完成，
  地址只调用一次 `parse_address`，两投影同源同序；库中无任何 trigger（测试强制断言）。

### 业务版本仲裁

- 在线修订使用乐观并发：提交方带 `expected_version`，不匹配即 409；
- 回填逐行比较 `customer_addresses.source_version` 与 `customers.version`，
  新投影版本不落后才写入，否则计入 `skipped_newer` 并跳过；
- 显式的过期快照回填接口同样以 `ConflictError` 拒绝。

### 检查点与崩溃恢复

- 检查点单行表记录 `last_id / status / 各状态计数 / skipped_newer`；
- 回填逐行提交，检查点与该行在同一事务内更新，崩溃最多损失当前行；
- 服务启动时把残留的 `running/paused` 检查点恢复为 `interrupted`，再次启动即续跑。

### 解析三态与不编造

- `parsed`：行政层级 + 街道/门牌号等细节足够；
- `partial`：拆出部分字段但缺关键层级；
- `pending`：无法可靠拆分；人工确认后为 `manual`。
- 除直辖市同城复制这一显式规则外，所有字段值都是输入字符串的逐字片段，测试会校验
  “字段值必须是原文子串”。
