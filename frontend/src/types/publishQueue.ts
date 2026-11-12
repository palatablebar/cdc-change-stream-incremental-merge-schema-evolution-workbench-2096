/** 跨流发布队列类型：与 backend/src/constants/pq_enums.py、/api/publish-queue 对齐。 */

export type PqStreamKey = 'customer_stream' | 'order_stream';
export type PqOrderStatus = 'waiting' | 'published' | 'blocked';
export type PqPumpStatus = 'running' | 'succeeded' | 'failed';

export interface PqStreamProgress {
  stream: PqStreamKey;
  stream_label: string;
  total: number;
  processed: number;
  backlog: number;
  last_seq: number;
}

export interface PqOrderCounts {
  waiting: number;
  published: number;
  blocked: number;
  outbox: number;
}

export interface PqMissingCustomer {
  customer_id: string;
  waiting_orders: number;
  reason: string;
}

export interface PqOrderRecord {
  order_id: string;
  customer_id: string;
  status: PqOrderStatus;
  status_label: string;
  delivery_count: number;
  attempts: number;
  blocked_reason: string | null;
  customer_status: string;
  payload: Record<string, unknown>;
  staged_at: string | null;
  published_at: string | null;
  updated_at: string | null;
}

export interface PqAuditRecord {
  id: number;
  action: string;
  action_label: string;
  stream: PqStreamKey | null;
  entity_type: string;
  entity_id: string;
  detail: Record<string, unknown>;
  created_at: string | null;
}

export interface PqPumpRun {
  id: number;
  trigger: string;
  status: PqPumpStatus;
  stats: Record<string, number>;
  error: string | null;
  started_at: string | null;
  finished_at: string | null;
}

export interface PqCrashInjection {
  enabled: boolean;
  order_event_uid: string | null;
  once: boolean;
}

export interface PqDashboard {
  streams: PqStreamProgress[];
  counts: PqOrderCounts;
  missing_customers: PqMissingCustomer[];
  orders: PqOrderRecord[];
  audit: PqAuditRecord[];
  pump_runs: PqPumpRun[];
  crash_injection: PqCrashInjection;
  demo_batch: string;
  generated_at: string;
}

export interface PqIngestResult {
  accepted: boolean;
  event_uid: string;
  note: string;
}

export interface PqPumpResult {
  status: string;
  message?: string;
  resumable?: boolean;
  hint?: string;
  processed?: number;
  stats?: Record<string, number>;
  holder?: string;
}

export interface PqDemoBatchResult {
  batch: number;
  finished: boolean;
  customers: number;
  orders: number;
  total_batches?: number;
}
