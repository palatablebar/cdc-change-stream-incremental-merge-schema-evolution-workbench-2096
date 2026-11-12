import { api } from './client';
import type {
  PqAuditRecord,
  PqCrashInjection,
  PqDashboard,
  PqDemoBatchResult,
  PqIngestResult,
  PqOrderRecord,
  PqPumpResult,
} from '../types/publishQueue';

export const publishQueueApi = {
  dashboard: () => api.get<PqDashboard>('/publish-queue/status'),
  orders: (status?: string, limit = 100) =>
    api.get<{ items: PqOrderRecord[] }>(
      `/publish-queue/orders?limit=${limit}${status ? `&status=${encodeURIComponent(status)}` : ''}`,
    ),
  audit: (limit = 100) => api.get<{ items: PqAuditRecord[] }>(`/publish-queue/audit?limit=${limit}`),

  ingestCustomer: (body: { event_uid: string; customer_id: string; op: 'upsert' | 'delete'; payload: Record<string, unknown> }) =>
    api.post<PqIngestResult>('/publish-queue/events/customers', body),
  ingestOrder: (body: { event_uid: string; order_id: string; customer_id: string; payload: Record<string, unknown> }) =>
    api.post<PqIngestResult>('/publish-queue/events/orders', body),

  pump: () => api.post<PqPumpResult>('/publish-queue/pump'),
  configureCrash: (order_event_uid: string | null, once = true) =>
    api.post<PqCrashInjection>('/publish-queue/crash-injection', { order_event_uid, once }),
  resetDemo: () => api.post<{ reset: boolean }>('/publish-queue/demo/reset'),
  advanceDemo: () => api.post<PqDemoBatchResult>('/publish-queue/demo/advance'),
};
