import {
  classifyVersionConflict,
  versionConflictCurrentVersion,
} from '../../apps/cdc-consumer/src/order-projection.service';

const conflict = (current: number, provided: number) => ({
  statusCode: 409,
  body: {
    error: {
      type: 'version_conflict_engine_exception',
      reason: `[ORD-1]: version conflict, current version [${current}] is higher or equal to the one provided [${provided}]`,
    },
  },
});

describe('version decision', () => {
  it('classifies an equal version as duplicate and a lower one as stale', () => {
    expect(classifyVersionConflict(4, 4)).toBe('duplicate');
    expect(classifyVersionConflict(2, 4)).toBe('ignored_stale');
  });

  it('extracts the stored version from an Elasticsearch version conflict', () => {
    expect(versionConflictCurrentVersion(conflict(4, 2))).toBe(4);
  });

  it('does not treat other errors as version conflicts', () => {
    expect(
      versionConflictCurrentVersion({ statusCode: 400, body: { error: { type: 'mapper_parsing_exception' } } }),
    ).toBeUndefined();
    expect(
      versionConflictCurrentVersion({ statusCode: 409, body: { error: { type: 'some_other_conflict' } } }),
    ).toBeUndefined();
    expect(versionConflictCurrentVersion(new Error('ECONNREFUSED'))).toBeUndefined();
  });
});
