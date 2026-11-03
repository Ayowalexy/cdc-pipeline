/** @type {import('jest').Config} */
// Type checking is done by `npm run typecheck`; ts-jest only transpiles.
const tsJest = ['ts-jest', { tsconfig: 'tsconfig.json', isolatedModules: true }];

module.exports = {
  testTimeout: 60000,
  projects: [
    {
      displayName: 'unit',
      testEnvironment: 'node',
      transform: { '^.+\\.ts$': tsJest },
      testMatch: ['<rootDir>/test/unit/**/*.spec.ts'],
    },
    {
      // Requires the Docker Compose stack (Elasticsearch on :9200, Kafka on :29092).
      displayName: 'integration',
      testEnvironment: 'node',
      transform: { '^.+\\.ts$': tsJest },
      testMatch: ['<rootDir>/test/integration/**/*.spec.ts'],
    },
  ],
};
