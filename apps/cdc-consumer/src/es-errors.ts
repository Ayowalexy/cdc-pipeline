import { errors } from '@elastic/elasticsearch';

export type ErrorClass = 'retryable' | 'permanent';

const RETRYABLE_STATUS = new Set([408, 429, 500, 502, 503, 504]);

/**
 * Transient: Elasticsearch unreachable, timing out, overloaded or still starting.
 * Permanent: anything the same request would fail on again (mapping/parse errors, 4xx).
 * Version conflicts (409) are not errors in the fixed projection; they are classified there.
 */
export function classifyEsError(err: unknown): ErrorClass {
  if (
    err instanceof errors.ConnectionError ||
    err instanceof errors.TimeoutError ||
    err instanceof errors.NoLivingConnectionsError
  ) {
    return 'retryable';
  }
  if (err instanceof errors.ResponseError) {
    return RETRYABLE_STATUS.has(err.statusCode ?? 0) ? 'retryable' : 'permanent';
  }
  return 'permanent';
}
