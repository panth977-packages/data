export class TestBench {
  static results = { passed: 0, failed: 0 };

  static section(name: string) {
    console.log(`\n=== Testing: ${name} ===`);
  }

  static assert(condition: boolean, message: string) {
    if (condition) {
      this.results.passed++;
      console.log(` ✅ PASS: ${message}`);
    } else {
      this.results.failed++;
      console.error(` ❌ FAIL: ${message}`);
    }
  }

  static assertDeep(actual: any, expected: any, message: string) {
    const isMatch = JSON.stringify(actual) === JSON.stringify(expected);
    this.assert(isMatch, `${message} (Expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)})`);
  }

  static summary() {
    console.log(`\n--- Test Summary ---`);
    console.log(`Total Passed: ${this.results.passed}`);
    console.log(`Total Failed: ${this.results.failed}`);
    if (this.results.failed === 0) console.log("🏆 ALL TESTS PASSED SUCCESSFULLY");
  }
}
