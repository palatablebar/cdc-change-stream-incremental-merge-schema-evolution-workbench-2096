import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { MetricStrip } from '../components/common/MetricStrip';
import { PanelHead } from '../components/common/PanelHead';
import { StatusPill } from '../components/common/StatusPill';
import { EmptyState } from '../components/common/EmptyState';
import { RequireRole } from '../components/common/RequireRole';
import { publishQueueApi } from '../api/publishQueue';
import type { PqDashboard, PqOrderRecord, PqOrderStatus } from '../types/publishQueue';
import { errorMessage } from '../utils/errorMessage';
import { displayValue, formatCount, formatShortTime } from '../utils/format';

const STATUS_FILTERS: Array<{ value: PqOrderStatus | ''; label: string }> = [
  { value: '', label: '全部' },
  { value: 'waiting', label: '暂存中' },
  { value: 'published', label: '已放行' },
  { value: 'blocked', label: '已阻断' },
];

const STATUS_TONE: Record<PqOrderStatus, 'amber' | 'ok' | 'danger'> = {
  waiting: 'amber',
  published: 'ok',
  blocked: 'danger',
};

/** 百分比进度条：两条流各自的已消费/总量。 */
const ProgressBar = ({ processed, total }: { processed: number; total: number }) => {
  const pct = total === 0 ? 0 : Math.min(100, Math.round((processed / total) * 100));
  return (
    <div className="pq-progress" title={`${processed}/${total}`}>
      <div className="pq-progress-fill" style={{ width: `${pct}%` }} />
      <span className="pq-progress-text mono">{pct}%</span>
    </div>
  );
};

const OrderRow = ({ order }: { order: PqOrderRecord }) => (
  <tr>
    <td>
      <div className="mono">{order.order_id}</div>
      <div className="tiny faint">投递 {order.delivery_count} 次 · 尝试 {order.attempts}</div>
    </td>
    <td>
      <span className="mono">{order.customer_id}</span>
      <div className="tiny faint">客户状态：{order.customer_status === 'active' ? '有效' : order.customer_status === 'deregistered' ? '已注销' : '未到达'}</div>
    </td>
    <td>
      <StatusPill label={order.status_label} tone={STATUS_TONE[order.status]} />
      {order.blocked_reason ? <div className="tiny faint">{order.blocked_reason}</div> : null}
    </td>
    <td className="tiny muted">{displayValue(order.payload, 70)}</td>
    <td className="tiny">{formatShortTime(order.published_at ?? order.staged_at)}</td>
  </tr>
);

/** 跨流发布队列：两条变更流的处理进度 + 暂存/放行/缺依赖/阻断台账 + 审计时间线。 */
export const PublishQueuePage = () => {
  const [board, setBoard] = useState<PqDashboard | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [filter, setFilter] = useState<PqOrderStatus | ''>('');
  const [busy, setBusy] = useState(false);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const timer = useRef<number | null>(null);

  const load = useCallback(async () => {
    try {
      const data = await publishQueueApi.dashboard();
      setBoard(data);
      setError(null);
    } catch (err) {
      setError(errorMessage(err));
    }
  }, []);

  useEffect(() => {
    setLoading(true);
    void load().finally(() => setLoading(false));
  }, [load]);

  useEffect(() => {
    if (!autoRefresh) return;
    timer.current = window.setInterval(() => void load(), 3000);
    return () => {
      if (timer.current) window.clearInterval(timer.current);
    };
  }, [autoRefresh, load]);

  const run = useCallback(
    async (action: () => Promise<unknown>, okText?: string) => {
      setBusy(true);
      setError(null);
      setNotice(null);
      try {
        const result = await action();
        if (okText) setNotice(okText);
        await load();
        return result;
      } catch (err) {
        setError(errorMessage(err));
        return null;
      } finally {
        setBusy(false);
      }
    },
    [load],
  );

  const visibleOrders = useMemo(() => {
    if (!board) return [];
    return filter ? board.orders.filter((o) => o.status === filter) : board.orders;
  }, [board, filter]);

  const counts = board?.counts;

  return (
    <div>
      <PanelHead
        title="跨流发布队列"
        sub="客户资料流与订单流乱序到达：客户缺失先暂存订单，客户到齐自动回补放行；重复投递精确一次，注销客户关联订单阻断"
        actions={
          <>
            <label className="tiny faint" style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <input type="checkbox" checked={autoRefresh} onChange={(e) => setAutoRefresh(e.target.checked)} />
              3 秒自动刷新
            </label>
            <button className="btn small" onClick={() => void load()} disabled={busy}>
              刷新
            </button>
            <RequireRole
              permission="publish_queue:write"
              fallback={<span className="pill muted" title="仅 admin/engineer 可触发">只读角色</span>}
            >
              <button
                className="btn small"
                disabled={busy}
                onClick={() =>
                  run(() => publishQueueApi.pump(), undefined).then((r) => {
                    if (r && (r as { status?: string }).status === 'failed_injected') {
                      setError(`处理在故障点回滚，检查点未前进：${(r as { message?: string }).message ?? ''}`);
                    } else if (r) {
                      const stats = (r as { stats?: Record<string, number> }).stats;
                      setNotice(
                        stats
                          ? `处理完成：放行 ${stats.released ?? 0} · 暂存 ${stats.staged ?? 0} · 阻断 ${stats.blocked ?? 0} · 重复 ${stats.duplicate ?? 0}`
                          : '处理完成',
                      );
                    }
                  })
                }
              >
                立即处理（pump）
              </button>
            </RequireRole>
          </>
        }
      />

      {error ? <div className="notice danger">{error}</div> : null}
      {notice ? <div className="notice">{notice}</div> : null}

      <MetricStrip
        items={[
          { label: '暂存中（缺依赖）', value: counts ? String(counts.waiting) : '—', tone: counts && counts.waiting > 0 ? 'amber' : 'ok', note: '等待客户到齐' },
          { label: '已放行', value: counts ? String(counts.published) : '—', tone: 'ok', note: '已发布到 outbox' },
          { label: '已阻断', value: counts ? String(counts.blocked) : '—', tone: counts && counts.blocked > 0 ? 'danger' : 'default', note: '客户注销终态' },
          { label: 'outbox 发布行数', value: counts ? formatCount(counts.outbox) : '—', note: '精确一次账本' },
        ]}
      />

      {/* 两条流的处理进度 */}
      <div className="section">
        <div className="section-label"><span>两条变更流处理进度</span></div>
        <div className="split-even">
          {board?.streams.map((s) => (
            <div className="panel" key={s.stream}>
              <div className="panel-head-sm">
                {s.stream_label}
                <span className="tiny faint mono" style={{ marginLeft: 8 }}>{s.stream}</span>
              </div>
              <div className="panel-body">
                <ProgressBar processed={s.processed} total={s.total} />
                <div className="tiny muted" style={{ marginTop: 8 }}>
                  已处理 <span className="mono">{s.processed}</span> / 入库 <span className="mono">{s.total}</span>
                  {' · '}积压 <span className="mono" style={{ color: s.backlog > 0 ? 'var(--color-warn)' : undefined }}>{s.backlog}</span>
                  {' · '}检查点 <span className="mono">{s.last_seq}</span>
                </div>
              </div>
            </div>
          ))}
          {!board && loading ? <div className="faint tiny">加载中…</div> : null}
        </div>
      </div>

      {/* 仍缺依赖的客户 */}
      <div className="section">
        <div className="section-label"><span>仍缺依赖（客户未到达）</span></div>
        {board && board.missing_customers.length === 0 ? (
          <div className="panel"><div className="panel-body tiny muted">没有缺依赖的暂存订单，所有等待中的客户均已到齐。</div></div>
        ) : (
          <table className="table">
            <thead>
              <tr><th>客户 ID</th><th className="num">暂存订单数</th><th>原因</th></tr>
            </thead>
            <tbody>
              {board?.missing_customers.map((m) => (
                <tr key={m.customer_id}>
                  <td className="mono">{m.customer_id}</td>
                  <td className="num" style={{ color: 'var(--color-warn)' }}>{m.waiting_orders}</td>
                  <td className="tiny faint">{m.reason}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {/* 订单台账：暂存/放行/阻断 */}
      <div className="section">
        <div className="section-label">
          <span>订单台账（暂存 / 放行 / 阻断）</span>
          <span className="chip-row" style={{ marginLeft: 'auto' }}>
            {STATUS_FILTERS.map((f) => (
              <button
                key={f.value || 'all'}
                className={`chip${filter === f.value ? ' on' : ''}`}
                onClick={() => setFilter(f.value)}
              >
                {f.label}
              </button>
            ))}
          </span>
        </div>
        {visibleOrders.length === 0 ? (
          <EmptyState title="暂无匹配记录" hint={<>通过演示控制台灌入事件，或用 <code>POST /api/publish-queue/events/orders</code> 手动投递。</>} />
        ) : (
          <table className="table">
            <thead>
              <tr><th>订单 / 投递</th><th>客户</th><th>状态</th><th>载荷</th><th>时间</th></tr>
            </thead>
            <tbody>
              {visibleOrders.map((o) => <OrderRow key={o.order_id} order={o} />)}
            </tbody>
          </table>
        )}
      </div>

      {/* 演示控制台 */}
      <RequireRole
        permission="publish_queue:write"
        fallback={
          <div className="section">
            <div className="section-label"><span>演示控制台</span></div>
            <div className="panel"><div className="panel-body tiny muted">当前角色只读，事件注入与泵控需要 engineer/admin 权限。</div></div>
          </div>
        }
      >
        <div className="section">
          <div className="section-label"><span>演示控制台（可恢复性演练）</span></div>
          <div className="panel">
            <div className="panel-body">
              <div className="chip-row" style={{ marginBottom: 10 }}>
                <button
                  className="btn small"
                  disabled={busy}
                  onClick={() =>
                    run(() => publishQueueApi.advanceDemo()).then((r) => {
                      if (r) {
                        const b = r as { batch: number; customers: number; orders: number; finished: boolean };
                        setNotice(b.finished ? '演示批次已全部灌入' : `批次 ${b.batch} 已入库：客户 ${b.customers} 条、订单 ${b.orders} 条`);
                      }
                    })
                  }
                >
                  灌入下一演示批次（{board?.demo_batch ?? '0'}/5）
                </button>
                <button className="btn small" disabled={busy} onClick={() => run(() => publishQueueApi.pump(), 'pump 已触发')}>
                  处理积压
                </button>
                <RequireRole
                  permission="publish_queue:admin"
                  fallback={<span className="pill muted">故障注入/重置仅 admin</span>}
                >
                  <button
                    className="btn small"
                    disabled={busy}
                    onClick={() =>
                      run(
                        () => publishQueueApi.configureCrash(board?.crash_injection.enabled ? null : 'demo-o-007', true),
                        board?.crash_injection.enabled ? '故障注入已清除，可从原处续跑' : '已在 demo-o-007 设置故障点：下一次 pump 处理到该事件时回滚',
                      )
                    }
                  >
                    {board?.crash_injection.enabled ? '清除故障注入' : '注入故障（demo-o-007）'}
                  </button>
                  <button className="btn small" disabled={busy} onClick={() => run(() => publishQueueApi.resetDemo(), '队列已重置')}>
                    重置全部队列
                  </button>
                </RequireRole>
              </div>
              <div className="tiny muted">
                当前故障注入：
                {board?.crash_injection.enabled ? (
                  <span className="pill danger" style={{ marginLeft: 6 }}>{board.crash_injection.order_event_uid}（一次性）</span>
                ) : (
                  <span className="pill ok" style={{ marginLeft: 6 }}>关闭</span>
                )}
                <span style={{ marginLeft: 12 }}>
                  故障发生时该事件事务整体回滚、检查点不前进；清除注入后再次 pump 即从同一事件续跑，已发布订单不会重复。
                </span>
              </div>
            </div>
          </div>
        </div>
      </RequireRole>

      {/* pump 运行记录 */}
      <div className="section">
        <div className="section-label"><span>pump 运行记录（失败留痕，从检查点续跑）</span></div>
        <table className="table">
          <thead>
            <tr><th>#</th><th>触发方式</th><th>状态</th><th>处理统计</th><th>错误</th><th>开始</th></tr>
          </thead>
          <tbody>
            {board?.pump_runs.map((run) => (
              <tr key={run.id}>
                <td className="mono">{run.id}</td>
                <td className="tiny">{run.trigger}</td>
                <td>
                  <StatusPill
                    label={run.status === 'succeeded' ? '成功' : run.status === 'failed' ? '失败' : '运行中'}
                    tone={run.status === 'succeeded' ? 'ok' : run.status === 'failed' ? 'danger' : 'amber'}
                  />
                </td>
                <td className="tiny mono">{displayValue(run.stats, 80)}</td>
                <td className="tiny faint">{run.error ?? '—'}</td>
                <td className="tiny">{formatShortTime(run.started_at)}</td>
              </tr>
            ))}
            {board && board.pump_runs.length === 0 ? (
              <tr><td colSpan={6} className="faint tiny">还没有 pump 运行记录</td></tr>
            ) : null}
          </tbody>
        </table>
      </div>

      {/* 审计时间线 */}
      <div className="section">
        <div className="section-label"><span>状态迁移审计时间线</span></div>
        <table className="table">
          <thead>
            <tr><th>时间</th><th>动作</th><th>对象</th><th>明细</th></tr>
          </thead>
          <tbody>
            {board?.audit.map((a) => (
              <tr key={a.id}>
                <td className="tiny">{formatShortTime(a.created_at)}</td>
                <td><span className="pill muted">{a.action_label}</span></td>
                <td className="mono tiny">{a.entity_type}/{a.entity_id}</td>
                <td className="tiny faint">{displayValue(a.detail, 90)}</td>
              </tr>
            ))}
            {board && board.audit.length === 0 ? (
              <tr><td colSpan={4} className="faint tiny">暂无状态迁移</td></tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </div>
  );
};
