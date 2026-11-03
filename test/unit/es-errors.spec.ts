import { errors } from '@elastic/elasticsearch';
import { classifyEsError } from '../../apps/cdc-consumer/src/es-errors';

const response = (statusCode: number) =>
  new errors.ResponseError({
    statusCode,
    body: { error: { type: 'x' } },
    headers: {},
    warnings: null,
    meta: {} as never,
  });

describe('classifyEsError', () => {
  it.each([429, 502, 503, 504])('treats HTTP %d as retryable', (code) => {
    expect(classifyEsError(response(code))).toBe('retryable');
  });

  it('treats connection failures and timeouts as retryable', () => {
    expect(classifyEsError(new errors.ConnectionError('ECONNREFUSED'))).toBe('retryable');
    expect(classifyEsError(new errors.TimeoutError('timeout'))).toBe('retryable');
    expect(classifyEsError(new errors.NoLivingConnectionsError('none', {} as never))).toBe('retryable');
  });

  it.each([400, 404])('treats HTTP %d (mapping/parse/request errors) as permanent', (code) => {
    expect(classifyEsError(response(code))).toBe('permanent');
  });

  it('treats unknown errors as permanent', () => {
    expect(classifyEsError(new Error('boom'))).toBe('permanent');
  });
});
