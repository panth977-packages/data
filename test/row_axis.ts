import { KeyAxis } from "../col_axis.ts";
import { FloatDataArray, StrDataArray } from "../data_array.ts";
import { PredefinedEpochAxis, RelativeEpochAxis } from "../row_axis.ts";
import { TestBench } from "./_utils.ts";

function runSystemQA() {
  TestBench.section("RowAxis Integration (Pre-Allocation Flow)");

  // --- 1. Setup ---
  const table = new RelativeEpochAxis({
    temp: new KeyAxis(new FloatDataArray()).expand(2), // Space for 2 sensors
    status: new KeyAxis(new StrDataArray()).expand(2), // Space for 2 strings
  }).optimize({ gte: 1000, lte: 5000, gap: 1000 });

  // CRITICAL: Must expand rows before creating them

  // --- 2. Test: Row Creation within Space ---
  const time1 = 2000;
  const rIdx = table.getRIdx(time1, true);

  const sensorA = table.getCIdx("temp", "Sensor_A", true);
  table.set(rIdx, sensorA, 25.5);

  TestBench.assert(
    table.get(rIdx, sensorA) === 25.5,
    "Stored and retrieved float correctly",
  );

  // --- 3. Test: Stride Integrity (The Stride Bug Test) ---
  TestBench.section("Memory Stride Integrity");
  // We have 1 row. Let's add more space and a second row.
  const time2 = 3000;
  const rIdx2 = table.getRIdx(time2, true);

  table.set(rIdx2, sensorA, 30.0);

  // Check if writing to Row 2 corrupted Row 1
  TestBench.assert(
    table.get(rIdx, sensorA) === 25.5,
    "Row 1 data preserved after Row 2 write",
  );
  TestBench.assert(
    table.get(rIdx2, sensorA) === 30.0,
    "Row 2 data stored correctly",
  );

  // --- 4. Test: PredefinedEpochAxis (Fixed Window) ---
  TestBench.section("Predefined Window Tests");
  const pAxis = new PredefinedEpochAxis({
    val: new KeyAxis(new FloatDataArray()).expand(1),
  }).optimize({ gte: 100, lte: 500, gap: 100 }); // Allocates space for 5 slots

  console.log(pAxis.toPrettyTable());
  const pIdx = pAxis.getRIdx(200, true);
  pAxis.set(pIdx, ["val", 0], 99);

  TestBench.assert(
    pAxis.get(pIdx, ["val", 0]) === 99,
    "Predefined axis handles offset indexing",
  );

  // --- 5. Test: Out of Bounds (Failure Case) ---
  TestBench.section("Boundary Enforcement");
  try {
    // Attempt to make a 6th row when only 4-5 were optimized/expanded
    pAxis.getRIdx(1000, true);
    TestBench.assert(false, "Should have thrown 'No space left' error");
  } catch (_e) {
    TestBench.assert(
      true,
      "Correctly blocked creation beyond allocated capacity",
    );
  }

  TestBench.summary();
}

runSystemQA();
