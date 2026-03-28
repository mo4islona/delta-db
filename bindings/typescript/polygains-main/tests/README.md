# Test Suite

Senior-level unit tests for the poly-sqd-ts application. All tests are designed to run **without requiring PostgreSQL** to be running.

## Test Files

### `bloomfilter.test.ts`
Tests for BloomFilter persistence layer:
- Binary serialization/deserialization
- Upsert semantics
- Storage efficiency (bytea vs JSON)
- Filter state preservation across save/load cycles
- Database mock isolation

**Key Tests:**
- Validates 50% space savings with binary storage
- Tests filter reconstruction accuracy
- Verifies metadata queries don't load full buckets

### `insider-detector.test.ts`
Tests for InsiderDetector and NotInsiderDetector classes:
- BloomFilter initialization with correct parameters
- Add/has operations
- False positive rate validation
- Performance characteristics (O(1) lookups)
- Detector isolation (insider vs notinsider)

**Key Tests:**
- Validates <1% false positive rate with 10k items
- Tests O(1) lookup time (< 0.01ms avg)
- Verifies 819,200 bits / 4 hashes configuration

### `parser.test.ts`
Tests for order parsing logic:
- Buy vs Sell order detection
- Asset ID selection logic
- Amount field mapping (shares vs USDC)
- Edge cases (zero amounts, max safe integers)
- Field preservation (logIndex, transactionIndex)

**Key Tests:**
- Validates buy order: takerAssetId = 0
- Validates sell order: takerAssetId != 0
- Tests invariants (EVENT.ORDER, valid SIDE enum)

### `constants.test.ts`
Tests for business logic constants:
- Mathematical correctness (BPS conversions)
- Type safety (BigInt vs Number)
- Business rule validation
- Immutability checks

**Key Tests:**
- Validates MIN_PRICE (0.95) = MIN_PRICE_BPS (9500) equivalence
- Confirms VOLUME_THRESHOLD = 4000 USDC
- Verifies BLOOM_SYNC_INTERVAL_MS = 15 minutes
- Validates Ethereum address checksums

### `db-init.test.ts`
Tests for database initialization logic:
- URL parsing (username, password, host, port, database)
- Unix socket path construction
- Connection mode detection (TCP vs socket)
- Retry logic parameters
- Error handling (invalid URLs, missing fields)

**Key Tests:**
- Validates password masking for logs
- Tests socket file path: `/var/run/postgresql/.s.PGSQL.5432`
- Verifies fallback from socket to TCP
- Validates health check query safety

## Running Tests

```bash
# Run all tests
bun test

# Run specific test file
bun test tests/bloomfilter.test.ts

# Run with coverage
bun test --coverage

# Watch mode
bun test --watch
```

## Test Philosophy

### ✅ What We Test
- **Pure functions**: Logic without side effects
- **Business rules**: Constants and thresholds
- **Data transformations**: Parsing and serialization
- **Edge cases**: Zero values, max integers, empty inputs
- **Performance**: O(1) lookups, false positive rates
- **Type safety**: BigInt vs Number, type preservation

### ❌ What We Don't Test (Requires Integration Tests)
- Actual database connections
- Network requests
- File system operations
- External API calls
- Docker container behavior

### Senior-Level Practices
1. **Descriptive test names**: Each test clearly states what it validates
2. **Comprehensive edge cases**: Zero values, max integers, empty strings
3. **Performance assertions**: O(1) time complexity, space efficiency
4. **Mathematical validation**: BPS conversions, precision checks
5. **Mocking strategy**: Database calls mocked, pure logic tested
6. **Business logic focus**: Tests validate domain rules, not implementation

## Coverage Goals

- **Bloomfilter helpers**: 100% (all paths tested)
- **Detectors**: 100% (includes performance tests)
- **Parser**: 100% (all branches covered)
- **Constants**: 100% (mathematical validation)
- **DB init logic**: 95% (excludes actual connection)

## Adding New Tests

When adding tests, follow these patterns:

```typescript
import { describe, test, expect, mock, beforeEach } from "bun:test";

describe("Feature name", () => {
  beforeEach(() => {
    // Reset state
  });

  describe("Sub-feature", () => {
    test("should do specific thing when condition", () => {
      // Arrange
      const input = "test";

      // Act
      const result = functionUnderTest(input);

      // Assert
      expect(result).toBe("expected");
    });
  });
});
```

## Mocking Strategy

```typescript
// Mock entire module
mock.module("@/db/init", () => ({
  db: mockDb,
}));

// Mock specific functions
const mockFn = mock(() => "mocked value");

// Clear mocks between tests
beforeEach(() => {
  mockFn.mockClear();
});
```

## CI/CD Integration

These tests are designed to run in CI/CD pipelines:
- No external dependencies (postgres, redis, etc.)
- Fast execution (< 5 seconds total)
- Deterministic results (no flaky tests)
- Exit code 0 on success, non-zero on failure

```yaml
# Example GitHub Actions
- name: Run tests
  run: bun test
```
