import { KeyAxis } from "../col_axis.ts";
import { FloatDataArray } from "../data_array.ts";
import { TestBench } from "./_utils.ts";

export function runColAxisQA() {
  TestBench.section("KeyAxis Initialization & Expansion");

  // 1. Setup a Float-backed KeyAxis
  const fda = new FloatDataArray();
  const axis = new KeyAxis(fda);

  // Initial state: 2 rows, 2 columns
  axis.expandRowSize(2); // rowSize = 2
  axis.expand(2); // capacity = 2

  TestBench.assert(axis.capacity === 2, "Capacity should be 2");
  TestBench.assert(axis.used === 0, "Used should be 0 initially");

  // 2. Testing Key Allocation ('make')
  axis.make("X");
  axis.make("Y");
  const xIdx = axis.getIdxId("X")!;
  const yIdx = axis.getIdxId("Y")!;

  TestBench.assert(
    typeof xIdx === "number" && typeof yIdx === "number",
    "Keys should have numeric indices",
  );

  // 3. Testing 2D Data Storage
  // Row 0: [10, 20], Row 1: [30, 40]
  axis.setAt(0, xIdx, 10);
  axis.setAt(0, yIdx, 20);
  axis.setAt(1, xIdx, 30);
  axis.setAt(1, yIdx, 40);

  TestBench.assert(
    axis.getAt(1, xIdx) === 30,
    "Retrieve data from Row 1 correctly",
  );

  // 4. THE ACID TEST: Expand Columns (Stride Change)
  // This forces the class to shift [30, 40] further down the array to make room for new columns in Row 0.
  axis.expand(1); // Capacity becomes 3.
  // Old Layout: [10, 20, 30, 40]
  // New Layout: [10, 20, U, 30, 40, U] (U = Undefined/Clear)

  TestBench.assert(axis.capacity === 3, "Capacity expanded to 3");
  TestBench.assert(
    axis.getAt(0, xIdx) === 10,
    "Row 0 data preserved after column expand",
  );
  TestBench.assert(
    axis.getAt(1, xIdx) === 30,
    "Row 1 data shifted and preserved correctly (Stride check)",
  );

  // 5. Testing Row Operations
  axis.expandRowSize(1); // rowSize becomes 3
  axis.setAt(2, xIdx, 50);
  TestBench.assert(axis.getAt(2, xIdx) === 50, "New row accessible");

  // 6. Testing Key Removal & Data Clearing
  axis.remove("X");
  TestBench.assert(
    axis.getAt(0, xIdx) === undefined,
    "Data cleared after key removal (Row 0)",
  );
  TestBench.assert(
    axis.getAt(1, xIdx) === undefined,
    "Data cleared after key removal (Row 1)",
  );
  TestBench.assert(axis.getIdxId("X") === undefined, "Key mapping removed");
  TestBench.assert(axis.used === 1, "Only 1 key used now");

  // 7. Testing copyWithinFromTime (Moving Rows)
  // Row 1 is [U, 40, U]. Move Row 1 to Row 2.
  axis.copyWithinFromRow(2, 1, 2);
  TestBench.assert(
    axis.getAt(2, yIdx) === 40,
    "Row data copied correctly across time",
  );

  TestBench.summary();
}
