import { TestBench } from "./_utils.ts";
import { FlagDataArray, FloatDataArray, StrDataArray } from "../data_array.ts";

function runFullQA() {
  // --- FLOAT DATA ARRAY TESTS ---
  TestBench.section("FloatDataArray Logic");
  const fda = new FloatDataArray();
  fda.expand(10).fill(1.5);
  TestBench.assert(fda.length === 10, "Length expands correctly");
  TestBench.assert(fda.getAt(5) === 1.5, "Fill applies values correctly");

  fda.setAt(2, FloatDataArray.Undefined);
  TestBench.assert(
    fda.getAt(2) === undefined,
    "Undefined constant maps to JS undefined",
  );

  const fdaCopy = fda.copy();
  fdaCopy.setAt(0, 99.9);
  TestBench.assert(fda.getAt(0) === 1.5, "Copy is deep (original untouched)");

  // --- STR DATA ARRAY TESTS ---
  TestBench.section("StrDataArray Logic (Deduplication)");
  const sda = new StrDataArray();
  sda.expand(5);
  sda.setAt(0, "Alpha");
  sda.setAt(1, "Beta");
  sda.setAt(2, "Alpha"); // Re-use "Alpha"

  TestBench.assert(
    sda.getAt(0) === sda.getAt(2),
    "Duplicate strings return same value",
  );

  // Internal mapping check (using type casting to access private for QA)
  const internalMap = (sda as any).indexValMap as Map<string, number>;
  TestBench.assert(
    internalMap.size === 3,
    "Map contains exactly 3 entries ('', Alpha, Beta)",
  );

  // --- FLAG DATA ARRAY TESTS (The Hard Part) ---
  TestBench.section("FlagDataArray Logic (Bit-Level)");
  const flags = new FlagDataArray();
  flags.expand(20); // Spans 3 bytes

  // Test 1: Cross-byte boundaries
  flags.setAt(7, true);
  flags.setAt(8, true);
  TestBench.assert(flags.getAt(7) === true, "Bit 7 (end of Byte 0) is set");
  TestBench.assert(flags.getAt(8) === true, "Bit 8 (start of Byte 1) is set");
  TestBench.assert(flags.getAt(6) === false, "Neighboring bit 6 remains false");

  // Test 2: Bulk Fill
  flags.clear(); // Set all to false
  flags.fill(true, 4, 12); // Fill across the byte 0/1 boundary
  const bitResults = [];
  for (let i = 3; i <= 13; i++) bitResults.push(flags.getAt(i));
  TestBench.assertDeep(
    bitResults,
    [false, true, true, true, true, true, true, true, true, false, false],
    "Range fill (4 to 12) correctly sets bits across byte boundaries",
  );

  // Test 3: Iterator
  TestBench.section("FlagDataArray Iterator");
  const itResults: boolean[] = [];
  const idxResults: number[] = [];
  flags.setAt(5, false);
  for (const [idx, val] of flags.getValues(4, 7)) {
    itResults.push(val);
    idxResults.push(idx);
  }
  TestBench.assertDeep(
    itResults,
    [true, false, true],
    "Iterator yields correct partial range",
  );
  TestBench.assertDeep(idxResults, [4, 5, 6], "Iterator yields correct int");

  // Test 4: CopyWithin (Shift logic)
  TestBench.section("FlagDataArray copyWithin");
  const shifter = new FlagDataArray();
  shifter.expand(10).clear();
  shifter.setAt(0, true);
  shifter.setAt(1, true); // [T, T, F, F, ...]
  shifter.copyWithin(2, 0, 2); // Should result in [T, T, T, T, F...]
  TestBench.assert(
    shifter.getAt(2) === true && shifter.getAt(3) === true,
    "Bits shifted correctly via copyWithin",
  );

  TestBench.summary();
}

runFullQA();
