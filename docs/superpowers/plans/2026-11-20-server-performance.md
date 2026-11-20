# Server Performance Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Измерить и снизить серверные CPU/allocations и очереди olcRTC для Mamba/DataChannel и VK/VP8 без изменения wire-протокола и клиентов.

**Architecture:** Сначала появляются loopback pprof, общий reader метрик процесса, server metrics в gate и read-only diagnostics. Затем два независимых кандидата: однопроходный marshal Jitsi EndpointMessage и быстрый путь уже упорядоченных RTP-пакетов. Каждый кандидат сравнивается с базой отдельно и откатывается при отсутствии живого эффекта.

**Tech Stack:** Go 1.26.3, стандартная библиотека, существующие Pion/smux/KCP, Mage, golangci-lint v2.

**Spec:** `docs/superpowers/specs/2026-11-20-server-performance-design.md`

## Global Constraints

- Не менять JSON/base64 на проводе, RTP/KCP framing, порядок пакетов, FPS, batch, KCP windows, pacing и таймауты.
- Не добавлять внешние зависимости. Код остаётся pure Go.
- Все новые AI-generated функции и блоки помечать `// ai-generated` по правилам `AGENTS.md`.
- Тесты запускать с `-race`; сборка и проверки через `mage check`, `mage build`, `mage lint`.
- pprof доступен только на loopback. Никаких новых публичных портов и секретов в `/stats`, отчётах или логах.
- Не перезапускать одновременно все комнаты. Canary: сначала `m1`, затем `v1` на Германии, остальные линии остаются активными.
- Оптимизации Mamba и VK не смешивать в одном коммите или одном A/B-замере.

## Review Focus

- Не-loopback `stats.listen`: `/stats` работает как раньше, а pprof возвращает 404. Проверка в Task 1.
- Потерянный `/proc`, нулевой PID и завершившийся сервер: ячейка gate не падает и не получает фиктивные нулевые метрики. Проверка в Task 2.
- Старый Jitsi peer с top-level `raw`, специальные символы endpoint ID и порядок JSON-полей: wire остаётся совместимым. Проверка в Task 6.
- Первый, дублированный, переупорядоченный и потерянный RTP-пакет: быстрый путь не нарушает последовательность и не оставляет старый gap timer. Проверка в Task 7.
- Reconnect или peer teardown во время чтения diagnostics: нет panic, race, блокировки сетевого вызова или утечки peer/session ID. Проверки в Tasks 3-5.

---

### Task 1: Loopback pprof на существующем stats listener

**Files:**
- Create: `internal/server/http.go`, `internal/server/http_test.go`
- Modify: `internal/server/server.go:266-285`
- Modify: `docs/configuration.md`, `docs/configuration.ru.md`

**Interfaces:**
- Consumes: существующий `(*meter).statsHandler(link func() LinkState) http.Handler`.
- Produces: `statsHTTPHandler(stats http.Handler, listen string) http.Handler`, используемый только `serveStats`.

- [ ] **Step 1: Write the failing HTTP tests**

`TestStatsPprofLoopbackOnly` проверяет таблицу:

```go
tests := []struct{ listen string; pprof int }{
    {"127.0.0.1:9464", http.StatusOK},
    {"[::1]:9464", http.StatusOK},
    {"0.0.0.0:9464", http.StatusNotFound},
    {"[::]:9464", http.StatusNotFound},
    {"bad", http.StatusNotFound},
}
```

Для каждого случая `/stats` должен сохранять ответ исходного handler, а `/debug/pprof/` иметь указанный статус. Добавить запросы `/debug/pprof/heap` и `/debug/pprof/goroutine?debug=1` для loopback.

- [ ] **Step 2: Run tests to verify failure**

Run: `go test -race ./internal/server -run 'TestStatsPprof' -count=1`
Expected: FAIL, `statsHTTPHandler` не определён.

- [ ] **Step 3: Implement the wrapper**

В `http.go` создать `statsHTTPHandler`. Он монтирует переданный `/stats`, а handlers `net/http/pprof` регистрирует только когда host из `net.SplitHostPort(listen)` парсится `netip.ParseAddr` и `Addr.IsLoopback()`. Не использовать default `http.DefaultServeMux`. В `serveStats` заменить Handler на `statsHTTPHandler(s.meter.statsHandler(s.LinkState), addr)`.

Обновить обе конфигурационные документации: pprof существует только на loopback `stats.listen`.

- [ ] **Step 4: Run server tests**

Run: `go test -race ./internal/server`
Expected: PASS, включая старый `/stats` contract.

- [ ] **Step 5: Commit**

```bash
git add internal/server/http.go internal/server/http_test.go internal/server/server.go docs/configuration.md docs/configuration.ru.md
git commit -m "feat: expose pprof on loopback stats listener"
```

### Task 2: CPU/RSS процесса и server metrics в gate

**Files:**
- Create: `internal/procstats/procstats.go`, `internal/procstats/procstats_linux.go`, `internal/procstats/procstats_other.go`, `internal/procstats/procstats_test.go`
- Create: `internal/gate/server_metrics.go`, `internal/gate/server_metrics_test.go`
- Modify: `internal/gate/gate.go`, `internal/gate/target_local.go`, `internal/gate/run.go`, `internal/gate/scenarios.go`

**Interfaces:**
- Produces: `procstats.Sample{CPU time.Duration, RSSBytes uint64}` и `procstats.Read(pid int) (Sample, bool)`.
- Produces: `Endpoint.ServerPID int`, равный нулю для link target.
- Produces: metric keys `payload_bytes`, `server_cpu_seconds`, `server_rss_peak_bytes`, `server_cpu_per_mbit`.

- [ ] **Step 1: Write procstats parser tests**

`TestParseLinuxProcStat` проверяет PID с пробелами и `)` в comm, корректные utime/stime, обрезанные и нечисловые записи. `TestParseLinuxProcStatm` проверяет resident pages и malformed input. `TestReadMissingProcess` требует `ok=false`, а не нулевой Sample.

- [ ] **Step 2: Run tests to verify failure**

Run: `go test -race ./internal/procstats`
Expected: FAIL, package отсутствует.

- [ ] **Step 3: Implement procstats**

`Read(pid<=0)` возвращает `false`. Linux-реализация читает `/proc/<pid>/stat` и `/proc/<pid>/statm`; CPU равен `(utime+stime)/100` секунд по Linux proc ABI, RSS равен `resident*os.Getpagesize()`. Парсинг stat начинается после последнего `") "`. Другие ОС возвращают `false`. Пакет не запускает внешние команды.

- [ ] **Step 4: Write gate metric tests**

`TestServerMetricsDeltaAndPeak` использует fake reader: CPU берётся как разность начала/конца, RSS как максимум периодических Sample. `TestServerCPUPerMbit` при 2 CPU seconds и 16 Mbit payload ожидает `0.125`. `TestServerMetricsUnavailable` с PID 0 и failed read оставляет все три server keys отсутствующими. `TestServerMetricsDoNotChangeVerdict` вызывает существующий `Evaluate` и проверяет отсутствие новых failures.

- [ ] **Step 5: Run gate tests to verify failure**

Run: `go test -race ./internal/gate -run 'TestServer(Metrics|CPU)' -count=1`
Expected: FAIL, sampler и metric keys не реализованы.

- [ ] **Step 6: Implement gate collection**

В `server` сохранить `pid int` после `cmd.Start`, затем выставить `ep.ServerPID`. Добавить server sampler с интервалом 1 секунда и injectable reader для тестов. `runCell` запускает его только для S2/S3 и останавливает сразу после сценария. S2/S3 записывают `payload_bytes=out.Bytes`.

После сценария добавить CPU delta, RSS peak и `cpuSeconds/(payloadBytes*8/1e6)`. Метрики добавляются только при валидных начальном и конечном Sample и `payloadBytes>0`. Не менять `Thresholds`, `Evaluate` или report schema.

- [ ] **Step 7: Run focused tests**

Run: `go test -race ./internal/procstats ./internal/gate`
Expected: PASS; link target и отсутствующий `/proc` не создают server metrics.

- [ ] **Step 8: Commit**

```bash
git add internal/procstats internal/gate/gate.go internal/gate/target_local.go internal/gate/run.go internal/gate/scenarios.go internal/gate/server_metrics.go internal/gate/server_metrics_test.go
git commit -m "test: measure server cpu and rss in gate"
```

### Task 3: Read-only diagnostics в `/stats`

**Files:**
- Create: `internal/diagnostics/diagnostics.go`
- Create: `internal/server/diagnostics.go`, `internal/server/diagnostics_test.go`
- Modify: `internal/server/server.go`, `internal/server/meter.go`, `internal/server/meter_test.go`

**Interfaces:**
- Produces: `diagnostics.Snapshot` и `diagnostics.Provider`:

```go
type Snapshot struct {
    BacklogBytes         int     `json:"backlog_bytes,omitempty"`
    RelayInFlightBytes   uint64  `json:"relay_in_flight_bytes,omitempty"`
    OutboundQueuePackets int     `json:"outbound_queue_packets,omitempty"`
    PacingBytesPerSecond uint64  `json:"pacing_bytes_per_second,omitempty"`
    DeliveredRatio       *float64 `json:"delivered_ratio,omitempty"`
}
type Provider interface { Diagnostics() Snapshot }
```

- Produces:

```go
type ServerDiagnostics struct {
    Peers      int                    `json:"peers"`
    Goroutines int                    `json:"goroutines"`
    RSSBytes   uint64                 `json:"rss_bytes,omitempty"`
    Transport  *diagnostics.Snapshot  `json:"transport,omitempty"`
}
```

- [ ] **Step 1: Write the stats contract tests**

`TestStatsIncludesDiagnosticsWithoutIdentifiers` заполняет peer/session/key маркерными строками и проверяет, что JSON содержит counts и transport values, но не эти строки. `TestStatsWithoutDiagnosticsProvider` требует валидный JSON с `transport:null` или omitted transport и прежние `link/keys/total`. Обновить старый contract test: теперь четыре верхних поля, включая `diagnostics`.

`TestDiagnosticsDuringReconnect` использует Server с `ln=nil` и закрытым peer map: ответ должен быть нулевым snapshot без panic.

- [ ] **Step 2: Run tests to verify failure**

Run: `go test -race ./internal/server -run 'TestStats|TestDiagnostics' -count=1`
Expected: FAIL, diagnostics types и поля отсутствуют.

- [ ] **Step 3: Implement server diagnostics**

`(*Server).diagnostics()` читает peer count, `runtime.NumGoroutine()` и `procstats.Read(os.Getpid())`. Если `s.ln` реализует `diagnostics.Provider`, snapshot вызывается один раз без сети и без ожидания lock, принадлежащего send loop.

Изменить `meter.statsHandler` на `statsHandler(link func() LinkState, diagnostics func() ServerDiagnostics)`. `StatsBody` получает `Diagnostics ServerDiagnostics `json:"diagnostics"``. `serveStats` передаёт `s.diagnostics`. Nil-функция в старых unit tests означает zero ServerDiagnostics.

- [ ] **Step 4: Run tests**

Run: `go test -race ./internal/diagnostics ./internal/server`
Expected: PASS; существующие key totals и link не изменились.

- [ ] **Step 5: Commit**

```bash
git add internal/diagnostics internal/server/diagnostics.go internal/server/diagnostics_test.go internal/server/server.go internal/server/meter.go internal/server/meter_test.go
git commit -m "feat: report server transport diagnostics"
```

### Task 4: Mamba diagnostics и baseline benchmarks

**Files:**
- Create: `internal/engine/jitsi/diagnostics.go`, `internal/engine/jitsi/diagnostics_test.go`, `internal/engine/jitsi/bridge_bench_test.go`
- Modify: `internal/transport/datachannel/transport.go`, `internal/transport/datachannel/transport_test.go`

**Interfaces:**
- Consumes: `diagnostics.Snapshot` из Task 3.
- Produces: `(*jitsi.Session).Diagnostics() diagnostics.Snapshot` и forwarding `(*datachannel.streamTransport).Diagnostics() diagnostics.Snapshot`.

- [ ] **Step 1: Write diagnostics tests**

`TestJitsiDiagnosticsSumsQueuesWithoutPeerIDs` создаёт два relay state и peer queues: backlog равен `bridgeBacklog()`, relay in flight равен сумме `sent-echoed`, peer IDs отсутствуют в формируемом JSON. `TestJitsiDiagnosticsDuringReconnect` с nil `jSess` и пустыми maps не паникует. `TestDataChannelForwardsDiagnostics` проверяет forwarding и zero snapshot для engine без Provider.

- [ ] **Step 2: Run tests to verify failure**

Run: `go test -race ./internal/engine/jitsi ./internal/transport/datachannel -run 'Test.*Diagnostics' -count=1`
Expected: FAIL, methods отсутствуют.

- [ ] **Step 3: Implement snapshots**

Jitsi snapshot читает bridge gauge и под `relayMu` суммирует только активные `sent-echoed`. Не вызывать `waitJSession`, `Connect` или Send. DataChannel проверяет `p.session.(diagnostics.Provider)` и возвращает zero Snapshot при отсутствии интерфейса.

- [ ] **Step 4: Add baseline benchmarks**

`BenchmarkEndpointMessageMarshal12KiB` выполняет `json.Marshal(newEndpointMessage(...))`; `BenchmarkDecodeRaw12KiB` и `BenchmarkDecodeRawLegacy12KiB` проходят текущий `decodeRaw`. Все вызывают `ReportAllocs` и `SetBytes(12*1024)`.

Run и сохранить базу:

```bash
go test ./internal/engine/jitsi -run '^$' -bench 'EndpointMessage|DecodeRaw' -benchmem -count=10 > /tmp/olcrtc-jitsi-baseline.txt
```

- [ ] **Step 5: Run tests and commit**

Run: `go test -race ./internal/engine/jitsi ./internal/transport/datachannel`
Expected: PASS.

```bash
git add internal/engine/jitsi/diagnostics.go internal/engine/jitsi/diagnostics_test.go internal/engine/jitsi/bridge_bench_test.go internal/transport/datachannel/transport.go internal/transport/datachannel/transport_test.go
git commit -m "test: add jitsi diagnostics and bridge benchmarks"
```

### Task 5: VP8 diagnostics и ordered-RTP baseline

**Files:**
- Create: `internal/transport/vp8channel/diagnostics.go`, `internal/transport/vp8channel/diagnostics_test.go`
- Modify: `internal/transport/vp8channel/transport.go`, `internal/transport/vp8channel/peers.go`, `internal/transport/vp8channel/writer.go`, `internal/transport/vp8channel/lane.go`, `internal/transport/vp8channel/frames.go`, `internal/transport/vp8channel/benchmark_test.go`

**Interfaces:**
- Consumes: `diagnostics.Snapshot`.
- Produces: `(*vp8channel.streamTransport).Diagnostics() diagnostics.Snapshot`.
- Internal: `laneDiagnostics{Queued, Pacing atomic.Uint64}` и `dataLane.diag *laneDiagnostics`; writer только сохраняет values, reader только делает `Load`. Новых allocations на flush нет.

- [ ] **Step 1: Write diagnostics tests**

`TestVP8DiagnosticsReadsLiveAndReleasedPeers` проверяет queue length, pacing и delivered ratio для live lane; released peer не учитывается. `TestVP8DiagnosticsConcurrentWithPeerWriter` гоняет snapshot и writer/teardown под `-race`. Snapshot не содержит epoch или peer ID. Отдельно проверить `nil` ratio до первого push и указатель на `0.0` при реальном нуле доставки.

- [ ] **Step 2: Run tests to verify failure**

Run: `go test -race ./internal/transport/vp8channel -run 'TestVP8Diagnostics' -count=1`
Expected: FAIL, snapshot API отсутствует.

- [ ] **Step 3: Implement atomic lane snapshots**

`peerSession` хранит `laneDiagnostics` value, а созданный `dataLane` получает его адрес. После `flush` сохранять `len(out)+pending` и текущий `pace.rate` в atomics. Не читать обычные lane fields из HTTP goroutine. При teardown peer удаляется из table, поэтому его counters больше не читаются.

`deliveryTrack` получает atomic totals pushes/acks. Ratio текущего KCP conn равен `acks/pushes` и выставляется только при `pushes>0`; реальный ноль остаётся валидным измерением. При reconnect новый conn начинает собственные totals.

`streamTransport.Diagnostics()` объединяет single-peer lane и peer table: queue суммируется, pacing берётся максимальный действующий, delivered ratio берётся минимальный известный. Результат использует `*float64`, поэтому неизвестное значение omitted, а measured zero сохраняется.

- [ ] **Step 4: Add ordered-RTP benchmark**

Добавить `BenchmarkRTPOrderedFrameAssembly12KiB`: пакеты идут строго по SequenceNumber, остальные параметры совпадают с существующим reordered benchmark. Сохранить базу обоих:

```bash
go test ./internal/transport/vp8channel -run '^$' -bench 'RTP.*FrameAssembly|KCPConn' -benchmem -count=10 > /tmp/olcrtc-vp8-baseline.txt
```

- [ ] **Step 5: Run tests and commit**

Run: `go test -race ./internal/transport/vp8channel`
Expected: PASS без races и goroutine leaks.

```bash
git add internal/transport/vp8channel/diagnostics.go internal/transport/vp8channel/diagnostics_test.go internal/transport/vp8channel/transport.go internal/transport/vp8channel/peers.go internal/transport/vp8channel/writer.go internal/transport/vp8channel/lane.go internal/transport/vp8channel/frames.go internal/transport/vp8channel/benchmark_test.go
git commit -m "test: add vp8 diagnostics and ordered rtp benchmark"
```

### Task 6: Однопроходный Jitsi EndpointMessage marshal

**Files:**
- Modify: `internal/engine/jitsi/bridge.go`
- Create: `internal/engine/jitsi/bridge_json_test.go`
- Modify: `internal/engine/jitsi/jitsi_test.go`, `internal/engine/jitsi/relaywindow_test.go`

**Interfaces:**
- Consumes: baseline `/tmp/olcrtc-jitsi-baseline.txt`.
- Produces: `appendEndpointMessage(dst []byte, to string, raw []byte) ([]byte, error)` и `endpointMessage.MarshalJSON()`.
- Internal change: `endpointRawPayload.Raw` становится `[]byte`; JSON representation остаётся base64 string.

- [ ] **Step 1: Write wire compatibility tests**

`TestEndpointMessageJSONWireCompatibility` сравнивает полный JSON с точной строкой `{"colibriClass":"EndpointMessage","to":"...","msgPayload":{"raw":"<base64>"}}`. Отдельные cases: пустой payload, 12 KiB, endpoint ID с кавычкой, backslash и Unicode. `TestDecodeRawAcceptsCurrentAndLegacy` сохраняет оба входящих формата и отбрасывает invalid base64.

- [ ] **Step 2: Run tests to verify failure**

Run: `go test -race ./internal/engine/jitsi -run 'TestEndpointMessageJSON|TestDecodeRaw' -count=1`
Expected: FAIL до смены Raw на `[]byte` и реализации custom marshal.

- [ ] **Step 3: Implement the candidate**

`newEndpointMessage` больше не вызывает `EncodeToString`, а сохраняет raw bytes. `MarshalJSON` вызывает `appendEndpointMessage`. Helper заранее выделяет `base64.StdEncoding.EncodedLen(len(raw))` плюс JSON overhead, записывает поля в установленном порядке, endpoint ID экранирует через `encoding/json`, base64 пишет прямо в итоговый буфер через `Encode`. Legacy decode не менять без отдельного доказанного профиля.

- [ ] **Step 4: Run tests and compare benchmarks**

Run: `go test -race ./internal/engine/jitsi ./internal/transport/datachannel`
Expected: PASS.

Run тот же benchmark в `/tmp/olcrtc-jitsi-candidate.txt` и сравнить через доступный `benchstat` либо таблицу median `ns/op`, `B/op`, `allocs/op`. Если нет улучшения без регрессии других метрик больше 5 процентов, откатить этот task и остановиться на следующем профиле вместо сохранения бесполезного кода.

- [ ] **Step 5: Commit only a measured candidate**

```bash
git add internal/engine/jitsi/bridge.go internal/engine/jitsi/bridge_json_test.go internal/engine/jitsi/*_test.go
git commit -m "perf: encode jitsi endpoint messages in one pass"
```

### Task 7: Быстрый путь упорядоченных RTP-пакетов

**Files:**
- Modify: `internal/transport/vp8channel/rtp_reader.go`
- Create: `internal/transport/vp8channel/rtp_reader_fast_test.go`
- Modify: `internal/transport/vp8channel/benchmark_test.go` только если benchmark из Task 5 требует уточнения

**Interfaces:**
- Consumes: baseline `/tmp/olcrtc-vp8-baseline.txt`.
- Preserves: `(*reorderBuffer).push(pkt *rtp.Packet, deliver func(*rtp.Packet))`, сигнатура не меняется.

- [ ] **Step 1: Write ordering regression tests**

`TestReorderOrderedFastPathDeliversImmediately` проверяет первый и все следующие in-order пакеты без clone в `pkts`. `TestReorderFastPathThenGap` проверяет, что после пропуска пакета later packets ждут, затем drain идёт по порядку. Добавить duplicate, старый sequence, timeout gap и переход обратно к in-order; `gapSince` после пустого buffer должен быть zero.

- [ ] **Step 2: Run tests to verify failure**

Run: `go test -race ./internal/transport/vp8channel -run 'TestReorder.*Fast|TestReorderFastPathThenGap' -count=1`
Expected: FAIL на проверке отсутствия clone/ожидаемого состояния быстрого пути.

- [ ] **Step 3: Implement the ordered fast path**

В начале `push` инициализировать `nextSeq` как раньше. Если `pkt.SequenceNumber==nextSeq` и `len(pkts)==0`, инкрементировать `nextSeq`, синхронно вызвать `deliver(pkt)`, очистить `gapSince` и вернуться без `clone`. Все остальные случаи идут через существующий clone/reorder/drain. Не менять reorder window, hold или VP8 assembler.

- [ ] **Step 4: Run full transport tests and compare**

Run: `go test -race ./internal/transport/vp8channel`
Expected: PASS, включая RED, loss, reorder, peer lifecycle и chaos tests.

Повторить benchmarks в `/tmp/olcrtc-vp8-candidate.txt`. Ordered path должен снизить allocations/copies; reordered path не должен деградировать больше 5 процентов. При отсутствии эффекта откатить task.

- [ ] **Step 5: Commit only a measured candidate**

```bash
git add internal/transport/vp8channel/rtp_reader.go internal/transport/vp8channel/rtp_reader_fast_test.go internal/transport/vp8channel/benchmark_test.go
git commit -m "perf: skip reorder copies for ordered rtp"
```

### Task 8: Полная проверка и canary на ВДС

**Files:**
- Modify only if verification finds a regression; иначе product files не менять
- No report or documentation file создавать без отдельной необходимости

**Interfaces:**
- Consumes: принятые commits Tasks 1-7.
- Produces: измеренный baseline/candidate verdict и рабочий server binary на согласованных ВДС.

- [ ] **Step 1: Run static and unit verification**

```bash
mage check
mage lint
mage build
go test -race ./...
```

Expected: PASS, 0 lint issues. Проверить `git status --short`; удалить только созданные этими командами ненужные артефакты.

- [ ] **Step 2: Run provider gates**

Для Mamba:

```bash
go test -count=1 -timeout 25m ./internal/gate -run '^TestGate$' -v \
  -olcrtc.gate -olcrtc.gate-providers=jitsi -olcrtc.gate-transports=datachannel \
  -olcrtc.gate-jitsi-hosts=meet.mamba.group -olcrtc.gate-dir=/tmp/gate-mamba-perf
```

Для VK использовать `OLCRTC_GATE_VKCALLS_ROOMS` из существующего секретного env, не вписывая ссылки в команду или отчёт:

```bash
go test -count=1 -timeout 25m ./internal/gate -run '^TestGate$' -v \
  -olcrtc.gate -olcrtc.gate-providers=vkcalls -olcrtc.gate-transports=vp8channel \
  -olcrtc.gate-dir=/tmp/gate-vk-perf
```

Expected: нет новых S0-S7 failures; server metrics присутствуют в S2/S3. Сравнить с baseline через `go run ./cmd/gate-report compare`.

- [ ] **Step 3: Capture baseline live profiles**

На Германии выбрать свободные loopback ports и включить `stats.listen` только у canary configs `m1` и `v1`. Снять по 30 секунд CPU profile плюс heap/goroutine и `/stats` под одинаковой нагрузкой:

```bash
curl -sS 'http://127.0.0.1:PORT/debug/pprof/profile?seconds=30' -o /tmp/olcrtc-LABEL-cpu.pb.gz
curl -sS 'http://127.0.0.1:PORT/debug/pprof/heap' -o /tmp/olcrtc-LABEL-heap.pb.gz
curl -sS 'http://127.0.0.1:PORT/stats'
```

Профили хранить локально вне репозитория. Не открывать порты в firewall.

- [ ] **Step 4: Canary Mamba `m1`**

Сохранить текущий binary, загрузить candidate как отдельный файл, сверить SHA256, атомарно заменить и перезапустить только `ilyukhaline-olcrtc@m1.service`. Проверить `systemctl is-active`, link up и старый клиент. Выполнить три чередующиеся baseline/candidate пары на одной комнате, затем профили Mamba 4x/8x/12x.

Candidate принимается при медианном снижении `server_cpu_per_mbit` или росте throughput минимум на 10 процентов без новых reconnect, missed pong, обрывов и роста connect p95. Иначе восстановить резервный binary и перезапустить только `m1`.

- [ ] **Step 5: Canary VK `v1`**

Повторить ту же процедуру только для `ilyukhaline-olcrtc@v1.service`: одна комната и VK 4x, TCP/UDP, CPU/RSS/diagnostics. Проверить отдельно от Mamba, чтобы эффекты не смешивались. При провале откатить только `v1`.

- [ ] **Step 6: Roll out accepted build**

После двух независимо принятых canary проверок установить тот же accepted binary на остальные Mamba/VK rooms Германии последовательно, затем на Финляндию через существующие configs и systemd units. SaluteJazz не перезапускать без необходимости; его regression gate должен остаться зелёным.

После каждой ВДС проверить link up, старый клиент, TCP и поддерживаемый UDP. Финальный ответ должен отдельно назвать измеренный эффект Mamba, VK и то, что осталось ограничено SFU.
