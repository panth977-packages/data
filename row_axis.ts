import { GenericParser, IdVersionParser, Parsers } from "./_parser.ts";
import type { ColumnAxis } from "./col_axis.ts";
import { FlagDataArray, type inferDTFromDC } from "./data_array.ts";

type inferAT<
  C extends Record<string, ColumnAxis<any, any>>,
  K extends keyof C,
> = C[K] extends ColumnAxis<infer AT, any> ? AT : never;
type inferDT<
  C extends Record<string, ColumnAxis<any, any>>,
  K extends keyof C,
> = C[K] extends ColumnAxis<any, infer DC> ? inferDTFromDC<DC> : never;
export abstract class RowAxis<
  RT,
  C extends Record<string, ColumnAxis<any, any>>,
> {
  protected columns: C;
  private static _id = IdVersionParser.create("ColumnAxis", 1);
  protected static _encode<
    RT,
    C extends Record<string, ColumnAxis<any, any>>,
  >(
    parser: { [K in keyof C]: GenericParser<C[K]> },
    rowAxis: RowAxis<RT, C>,
  ): ArrayBuffer[] {
    const topics = Object.keys(rowAxis.columns);
    const topicsBuffer = Parsers.StringList.encode(topics);
    const columnsBuffers = Parsers.ArrayBufferList.encode(
      topics.map((topic) =>
        parser[topic].encode(rowAxis.columns[topic] as never)
      ),
    );
    return [
      this._id,
      topicsBuffer,
      columnsBuffers,
    ];
  }
  protected static _decode<
    C extends Record<string, ColumnAxis<any, any>>,
  >(
    parser: { [K in keyof C]: GenericParser<C[K]> },
    buff: ArrayBuffer[],
  ): C {
    const [id, topicsBuffer, columnsBuffers] = buff;
    IdVersionParser.check(id, this._id);
    const topics = Parsers.StringList.decode(topicsBuffer);
    const columns = Parsers.ArrayBufferList.decode(columnsBuffers);
    return Object.fromEntries(
      topics.map((topic, i) => [topic, parser[topic].decode(columns[i])]),
    ) as never;
  }
  protected static _create<
    C extends Record<string, ColumnAxis<any, any>>,
  >(
    parser: { [K in keyof C]: GenericParser<C[K]> },
  ): C {
    return Object.fromEntries(
      Object.keys(parser).map((topic) => [topic, parser[topic].create()]),
    ) as never;
  }
  protected static _check<
    C extends Record<string, ColumnAxis<any, any>>,
  >(
    parser: { [K in keyof C]: GenericParser<C[K]> },
    value: unknown,
  ): value is C {
    return typeof value === "object" && value !== null &&
      Object.keys(parser).every((topic) =>
        topic in value && parser[topic].check((value as C)[topic])
      );
  }
  constructor(from: RowAxis<RT, C> | C) {
    if (from instanceof RowAxis) {
      this.columns = { ...from.columns };
      for (const k in this.columns) {
        this.columns[k] = this.columns[k].copy() as C[typeof k];
      }
      this.columns = Object.freeze(this.columns);
    } else {
      this.columns = from;
    }
  }
  abstract copy(): this;
  protected abstract get capacity(): number;
  protected abstract get used(): number;
  protected abstract expand(add: number): this;
  protected abstract make(id: RT): this;
  protected abstract remove(id: RT): this;
  protected abstract getIdAt(idx: number): RT | undefined;
  protected abstract getIdxId(id: RT): number | undefined;
  protected abstract getIdsWithIndex(): IterableIterator<[RT, number]>;
  protected abstract getIds(): IterableIterator<RT>;
  protected abstract getIdIndex(): IterableIterator<number>;

  expandCol<K extends keyof C>(topic: K, add: number) {
    this.columns[topic].expand(add);
  }

  rows(type: "[row,rIdx]"): IterableIterator<[RT, number]>;
  rows(type: "row"): IterableIterator<RT>;
  rows(type: "rIdx"): IterableIterator<number>;
  rows(type: "[row,rIdx]" | "row" | "rIdx") {
    if (type === "[row,rIdx]") return this.getIdsWithIndex();
    if (type === "row") return this.getIds();
    return this.getIdIndex();
  }

  cols<K extends keyof C>(
    topic: K,
    type: "[col,cIdx]",
  ): IterableIterator<[inferAT<C, K>, [K, number]]>;
  cols<K extends keyof C>(
    topic: K,
    type: "col",
  ): IterableIterator<inferAT<C, K>>;
  cols<K extends keyof C>(
    topic: K,
    type: "cIdx",
  ): IterableIterator<[K, number]>;
  *cols<K extends keyof C>(topic: K, type: "[col,cIdx]" | "col" | "cIdx") {
    const column = this.columns[topic];
    if (type === "[col,cIdx]") {
      for (const [col, cIdx] of column.getIdsWithIndex()) {
        yield [col, [topic, cIdx]];
      }
      return;
    }
    if (type === "col") {
      yield* column.getIds();
    }
    for (const cIdx of column.getIdIndex()) {
      yield [topic, cIdx];
    }
  }

  getRIdx(row: RT, addIfNot?: false): number | undefined;
  getRIdx(row: RT, addIfNot: true): number;
  getRIdx(row: RT, addIfNot?: boolean): number | undefined {
    let idx = this.getIdxId(row);
    if (idx == undefined && addIfNot) {
      this.make(row);
      idx = this.getIdxId(row);
    }
    return idx;
  }
  getCIdx<K extends keyof C>(
    topic: K,
    col: inferAT<C, K>,
    addIfNot?: false,
  ): [K, number] | undefined;
  getCIdx<K extends keyof C>(
    topic: K,
    col: inferAT<C, K>,
    addIfNot: true,
  ): [K, number];
  getCIdx<K extends keyof C>(
    topic: K,
    col: inferAT<C, K>,
    addIfNot?: boolean,
  ): [K, number] | undefined {
    const column = this.columns[topic];
    let idx = column.getIdxId(col);
    if (idx == undefined && addIfNot) {
      column.make(col);
      idx = column.getIdxId(col);
    }
    if (idx == undefined) return undefined;
    return [topic, idx];
  }

  removeRow(row: RT) {
    if (this.getIdxId(row) != undefined) {
      this.remove(row);
    }
  }

  removeCol<K extends keyof C>(
    topic: K,
    col: inferAT<C, K>,
  ) {
    const column = this.columns[topic];
    if (column.getIdxId(col) != undefined) {
      column.remove(col);
    }
  }

  get<K extends keyof C>(
    rIdx: number,
    cIdx: [K, number],
  ): inferDT<C, K> | undefined {
    return this.columns[cIdx[0]].getAt(rIdx, cIdx[1]) as never;
  }
  set<K extends keyof C>(
    rIdx: number,
    cIdx: [K, number],
    val: inferDT<C, K>,
  ) {
    this.columns[cIdx[0]].setAt(rIdx, cIdx[1], val);
  }
  del<K extends keyof C>(rIdx: number, cIdx: [K, number]) {
    this.columns[cIdx[0]].delAt(rIdx, cIdx[1]);
  }

  mapCol<K extends keyof C, M>(
    topic: K,
    col: inferAT<C, K>,
    map: (row: RT, val: inferDT<C, K> | undefined) => M,
  ): M[] {
    const column = this.columns[topic];
    const cIdx = column.getIdxId(col);
    if (!cIdx) return [];
    const res = [];
    for (const [row, rIdx] of this.rows("[row,rIdx]")) {
      res.push(map(row, column.getAt(rIdx, cIdx) as never));
    }
    return res;
  }

  usedOfRow(): number {
    return this.used;
  }
  capacityOfRow(): number {
    return this.capacity;
  }
  getRowAt(rIdx: number): RT | undefined {
    return this.getIdAt(rIdx);
  }
  usedOfCol<K extends keyof C>(topic: K): number {
    return this.columns[topic].used;
  }
  capacityOfCol<K extends keyof C>(topic: K): number {
    return this.columns[topic].capacity;
  }
  getColAt<K extends keyof C>(cIdx: [K, number]): inferAT<C, K> | undefined {
    return this.columns[cIdx[0]].getIdAt(cIdx[1]);
  }

  toPrettyTable(colWidth = 15): string {
    const topics = Object.keys(this.columns);
    if (topics.length === 0) return "No data available.";

    // 1. Render all sub-tables and prepare the Row Labels column
    const topicTables = topics.map((t) =>
      this.columns[t].toPrettyTable(colWidth + 1).split("\n")
    );
    const horizontalBar = "─".repeat(colWidth + 2);

    const rowIdLines = [
      "│ " + "ROW".padEnd(colWidth) + " │",
      "├" + horizontalBar + "┼",
      "│ " + "ID".padEnd(colWidth) + " │",
      "├" + horizontalBar + "┼",
    ];

    for (let rIdx = 0; rIdx < this.capacityOfRow(); rIdx++) {
      const id = this.idToStr(this.getRowAt(rIdx)).substring(0, colWidth)
        .padEnd(colWidth);
      rowIdLines.push(`│ ${id} │`);
    }
    rowIdLines.push("└" + horizontalBar + "┴");

    // 2. Build the Super Header (The Topic Names)
    let superTop = "┌" + horizontalBar + "┬";
    let topicRow = rowIdLines[0]; // Start with the "ROW" label cell

    topics.forEach((topic, idx) => {
      const cap = this.columns[topic].capacity;
      // Calculation: (cell + 2 spaces + 1 divider) * capacity - the very last divider
      const totalSpan = (colWidth + 2) * cap + (cap - 1);
      const label = topic.toString().substring(0, totalSpan).padEnd(totalSpan);

      topicRow += ` ${label} │`;
      superTop += "─".repeat(totalSpan + 2) +
        (idx === topics.length - 1 ? "┐" : "┬");
    });

    // 3. Stitch everything together
    const finalLines = [superTop, topicRow];

    // We start from index 1 of rowIdLines because index 0 was the 'ROW' label
    for (let i = 1; i < rowIdLines.length; i++) {
      let combined = rowIdLines[i];
      topicTables.forEach((tableLines, tIdx) => {
        // tableLines[0] is the ┌───┐ of the sub-table
        // We align rowIdLines[1] (the ├───┼) with tableLines[0]
        let line = tableLines[i - 1];
        if (i === 1) {
          // Connecting the Topic row to the Sub-table headers
          line = line
            .replace("┌", "┬")
            .replace("┐", tIdx === topics.length - 1 ? "┤" : "┼")
            .replace(/┬/g, "┬");
        } else if (i === rowIdLines.length - 1) {
          // Bottom border
          line = line.replace("└", "┴").replace("┘", "┘").replace(/┴/g, "┴");
        }

        combined += line.substring(1);
      });
      finalLines.push(combined);
    }

    return finalLines.join("\n");
  }
  abstract idToStr(val: RT | undefined): string;

  protected _filter<T extends RowAxis<RT, C>>(
    predicate: ((row: RT, rIdx: number) => boolean) | [RT, number][],
    create: (rows: (readonly [RT, number])[]) => T,
  ): T {
    const rows = Array.isArray(predicate) ? predicate : [];
    if (typeof predicate === "function") {
      for (const [row, rIdx] of this.rows("[row,rIdx]")) {
        if (predicate(row, rIdx)) {
          rows.push([row, rIdx] as const);
        }
      }
    }
    if (rows.length === 0) return create(rows);
    const filteredRows = create(rows);
    const cols = [];
    for (const topic in filteredRows.columns) {
      filteredRows.expandCol(topic, this.usedOfCol(topic));
      for (const [col, cIdx] of this.cols(topic, "[col,cIdx]")) {
        cols.push([cIdx, filteredRows.getCIdx(topic, col, true)] as const);
      }
    }
    for (const [row, rIdx] of rows) {
      const rIdxF = filteredRows.getRIdx(row, true);
      for (const [cIdx, cIdxF] of cols) {
        const val = this.get(rIdx, cIdx);
        if (val != undefined) filteredRows.set(rIdxF, cIdxF, val);
      }
    }
    return filteredRows;
  }
}
export type RowAxisClass<
  O,
  RT,
  C extends Record<string, ColumnAxis<any, any>>,
  I extends RowAxis<RT, C>,
> = { new (from?: I): I };
export type EpochAxisOpt = { gte: number; lte: number; gap: number };
export abstract class EpochAxis<C extends Record<string, ColumnAxis<any, any>>>
  extends RowAxis<number, C> {
  static readonly minPer: Readonly<{
    min: 1;
    hr: 60;
    day: 1440;
    week: 10080;
  }> = Object.freeze({
    min: 1,
    hr: 60,
    day: 1440,
    week: 10080,
  });
  static readonly secPer: Readonly<
    {
      sec: 1;
      min: 60;
      hr: 3600;
      day: 86400;
      week: 604800;
    }
  > = Object.freeze({
    sec: 1,
    min: 60,
    hr: 3600,
    day: 86400,
    week: 604800,
  });
  static readonly msPer: Readonly<
    {
      ms: 1;
      sec: 1000;
      min: 60000;
      hr: 3600000;
      day: 86400000;
      week: 604800000;
    }
  > = Object.freeze({
    ms: 1,
    sec: 1000,
    min: 60000,
    hr: 3600000,
    day: 86400000,
    week: 604800000,
  });
  abstract optimize(otp: EpochAxisOpt): this;
  constructor(from: EpochAxis<C> | C) {
    super(from);
  }
}
export class RelativeEpochAxis<C extends Record<string, ColumnAxis<any, any>>>
  extends EpochAxis<C> {
  private minEpoch: number; // min(epochs) - 1
  private epoch: Uint32Array; // delta
  private epochMapping: Map<number, number>;
  private unusedEpochIdx: Array<number>;
  static readonly Undefined: number = 0;
  private static id = IdVersionParser.create("RelativeEpochAxis", 1);
  protected static encode<C extends Record<string, ColumnAxis<any, any>>>(
    parser: { [K in keyof C]: GenericParser<C[K]> },
    epochAxis: RelativeEpochAxis<C>,
  ): ArrayBuffer {
    const minEpoch = Parsers.Number.encode(epochAxis.minEpoch);
    const epoch = Parsers.Uint32Array.encode(epochAxis.epoch);
    const columns = RowAxis._encode(parser, epochAxis);
    return Parsers.ArrayBufferList.encode([
      this.id,
      minEpoch,
      epoch,
      ...columns,
    ]);
  }
  protected static decode<C extends Record<string, ColumnAxis<any, any>>>(
    parser: { [K in keyof C]: GenericParser<C[K]> },
    buff: ArrayBuffer,
  ): RelativeEpochAxis<C> {
    const [id, minEpoch, epoch, ...columns] = Parsers.ArrayBufferList.decode(
      buff,
    );
    IdVersionParser.check(id, this.id);
    const epochAxis = new RelativeEpochAxis(
      RowAxis._decode(parser, columns),
    );
    epochAxis.minEpoch = Parsers.Number.decode(minEpoch);
    epochAxis.epoch = Parsers.Uint32Array.decode(epoch);
    for (let i = 0; i < epochAxis.epoch.length; i++) {
      if (epochAxis.epoch[i] == 0) {
        epochAxis.unusedEpochIdx.push(i);
      } else {
        epochAxis.epochMapping.set(epochAxis.epoch[i], i);
      }
    }
    return epochAxis;
  }
  protected static create<C extends Record<string, ColumnAxis<any, any>>>(
    parser: { [K in keyof C]: GenericParser<C[K]> },
    relativeEpochAxis?: RelativeEpochAxis<C>,
  ): RelativeEpochAxis<C> {
    if (relativeEpochAxis) {
      return new RelativeEpochAxis(relativeEpochAxis);
    } else {
      return new RelativeEpochAxis(RowAxis._create(parser));
    }
  }
  protected static check<C extends Record<string, ColumnAxis<any, any>>>(
    parser: { [K in keyof C]: GenericParser<C[K]> },
    value: unknown,
  ): value is RelativeEpochAxis<C> {
    return RowAxis._check(parser, value) &&
      value instanceof RelativeEpochAxis;
  }
  static parser<C extends Record<string, ColumnAxis<any, any>>>(
    parser: { [K in keyof C]: GenericParser<C[K]> },
  ): GenericParser<RelativeEpochAxis<C>> {
    return new GenericParser({
      encode: (RelativeEpochAxis.encode<C>).bind(RelativeEpochAxis, parser),
      decode: (RelativeEpochAxis.decode<C>).bind(RelativeEpochAxis, parser),
      create: (RelativeEpochAxis.create<C>).bind(RelativeEpochAxis, parser),
      check: (RelativeEpochAxis.check<C>).bind(
        RelativeEpochAxis,
        parser,
      ) as GenericParser<RelativeEpochAxis<C>>["check"],
    });
  }
  constructor(from: RelativeEpochAxis<C> | C) {
    super(from);
    if (from instanceof RelativeEpochAxis) {
      this.minEpoch = from.minEpoch;
      this.epochMapping = new Map(from.epochMapping);
      this.epoch = new Uint32Array(from.epoch);
      this.unusedEpochIdx = [...from.unusedEpochIdx];
    } else {
      this.minEpoch = 0;
      this.epoch = new Uint32Array(0).fill(RelativeEpochAxis.Undefined);
      this.epochMapping = new Map();
      this.unusedEpochIdx = [];
    }
  }
  optimize(opt: EpochAxisOpt): this {
    this.setMin(opt.gte);
    const size = (opt.lte - opt.gte) / opt.gap;
    const needMore = size - (this.epoch.length - this.epochMapping.size);
    if (needMore > 0) this.expand(needMore);
    return this;
  }
  copy(): this {
    return new RelativeEpochAxis(this) as this;
  }
  protected get used(): number {
    return this.epochMapping.size;
  }
  protected get capacity(): number {
    return this.epoch.length;
  }
  protected expand(add: number): this {
    if (add < 1) throw new Error("Cannot reduce space");
    for (
      let idx = this.epoch.length + add - 1;
      idx >= this.epoch.length;
      idx--
    ) {
      this.unusedEpochIdx.push(idx);
    }
    const epoch = new Uint32Array(this.epoch.length + add).fill(
      RelativeEpochAxis.Undefined,
    );
    epoch.set(this.epoch);
    this.epoch = epoch;
    for (const k in this.columns) {
      this.columns[k].expandRowSize(add);
    }
    return this;
  }
  private setMin(epoch: number) {
    epoch--;
    if (!this.minEpoch) {
      this.minEpoch = epoch;
    } else if (this.minEpoch > epoch) {
      const delta = this.minEpoch - epoch;
      const epochMapping = new Map<number, number>();
      for (const [e, idx] of this.epochMapping) {
        const n = e + delta;
        epochMapping.set(n, idx);
        this.epoch[idx] = n;
      }
      this.minEpoch = epoch;
    }
  }
  protected make(id: number): this {
    if (this.epochMapping.has(id - this.minEpoch)) {
      throw new Error("Already Exists");
    }
    this.setMin(id);
    id -= this.minEpoch;
    const idx = this.unusedEpochIdx.pop();
    if (idx == undefined) throw new Error("No space left!");
    this.epoch[idx] = id;
    this.epochMapping.set(id, idx);
    return this;
  }
  protected remove(id: number): this {
    id -= this.minEpoch;
    const idx = this.epochMapping.get(id);
    if (idx == undefined) throw new Error("Not found");
    this.epoch[idx] = RelativeEpochAxis.Undefined;
    for (const k in this.columns) {
      this.columns[k].delFromRow(idx);
    }
    this.epochMapping.delete(id);
    this.unusedEpochIdx.push(idx);
    return this;
  }
  protected getIdAt(idx: number): number | undefined {
    if (this.epoch[idx] === RelativeEpochAxis.Undefined) return undefined;
    return this.epoch[idx] + this.minEpoch;
  }
  protected getIdxId(id: number): number | undefined {
    id -= this.minEpoch;
    return this.epochMapping.get(id);
  }

  protected getIdIndex(): IterableIterator<number> {
    return this.epochMapping.values();
  }
  protected *getIds(): IterableIterator<number> {
    for (const val of this.epochMapping.keys()) {
      yield this.minEpoch + val;
    }
  }
  protected *getIdsWithIndex(): IterableIterator<[number, number]> {
    for (const [val, idx] of this.epochMapping.entries()) {
      yield [this.minEpoch + val, idx];
    }
  }
  idToStr(val: number | undefined): string {
    return val?.toString() ?? "";
  }
  filter(
    parser: GenericParser<RelativeEpochAxis<C>>,
    predicate: ((row: number, rIdx: number) => boolean) | [number, number][],
  ): RelativeEpochAxis<C> {
    return this._filter(
      predicate,
      (rows) => parser.create().expand(rows.length),
    );
  }
}

export class PredefinedEpochAxis<C extends Record<string, ColumnAxis<any, any>>>
  extends EpochAxis<C> {
  private firstEpoch: number; // first epoch
  private epoch: FlagDataArray; // delta
  private factor: number;
  private _used: number;
  private firstEpochIdxInUse: number; // first epoch
  private lastEpochIdxInUse: number; // last epoch

  private static id = IdVersionParser.create("RelativeEpochAxis", 1);
  private static epochParser = FlagDataArray.parser();
  protected static encode<C extends Record<string, ColumnAxis<any, any>>>(
    parser: { [K in keyof C]: GenericParser<C[K]> },
    epochAxis: PredefinedEpochAxis<C>,
  ): ArrayBuffer {
    const firstEpoch = Parsers.Number.encode(epochAxis.firstEpoch);
    const factor = Parsers.Number.encode(epochAxis.factor);
    const epoch = PredefinedEpochAxis.epochParser.encode(epochAxis.epoch);
    const columns = RowAxis._encode(parser, epochAxis);
    return Parsers.ArrayBufferList.encode([
      this.id,
      firstEpoch,
      factor,
      epoch,
      ...columns,
    ]);
  }
  protected static decode<C extends Record<string, ColumnAxis<any, any>>>(
    parser: { [K in keyof C]: GenericParser<C[K]> },
    buff: ArrayBuffer,
  ): PredefinedEpochAxis<C> {
    const [id, firstEpoch, factor, epoch, ...columns] = Parsers.ArrayBufferList
      .decode(buff);
    IdVersionParser.check(id, this.id);
    const epochAxis = new PredefinedEpochAxis(
      RowAxis._decode(parser, columns),
    );
    epochAxis.firstEpoch = Parsers.Number.decode(firstEpoch);
    epochAxis.factor = Parsers.Number.decode(factor);
    epochAxis.epoch = PredefinedEpochAxis.epochParser.decode(epoch);
    for (const idx of epochAxis.epoch.getFlagedIdx()) {
      if (epochAxis._used === 0) {
        epochAxis._used = 1;
        epochAxis.firstEpochIdxInUse = idx;
        epochAxis.lastEpochIdxInUse = idx;
      } else {
        epochAxis._used++;
        epochAxis.lastEpochIdxInUse = idx;
      }
    }
    return epochAxis;
  }
  protected static create<C extends Record<string, ColumnAxis<any, any>>>(
    parser: { [K in keyof C]: GenericParser<C[K]> },
    relativeEpochAxis?: PredefinedEpochAxis<C>,
  ): PredefinedEpochAxis<C> {
    if (relativeEpochAxis) {
      return new PredefinedEpochAxis(relativeEpochAxis);
    } else {
      return new PredefinedEpochAxis(RowAxis._create(parser));
    }
  }
  protected static check<C extends Record<string, ColumnAxis<any, any>>>(
    parser: { [K in keyof C]: GenericParser<C[K]> },
    value: unknown,
  ): value is PredefinedEpochAxis<C> {
    return RowAxis._check(parser, value) &&
      value instanceof PredefinedEpochAxis;
  }
  static parser<C extends Record<string, ColumnAxis<any, any>>>(
    parser: { [K in keyof C]: GenericParser<C[K]> },
  ): GenericParser<PredefinedEpochAxis<C>> {
    return new GenericParser({
      encode: (PredefinedEpochAxis.encode<C>).bind(
        PredefinedEpochAxis,
        parser,
      ),
      decode: (PredefinedEpochAxis.decode<C>).bind(
        PredefinedEpochAxis,
        parser,
      ),
      create: (PredefinedEpochAxis.create<C>).bind(
        PredefinedEpochAxis,
        parser,
      ),
      check: PredefinedEpochAxis.check.bind(
        PredefinedEpochAxis,
        parser,
      ) as GenericParser<PredefinedEpochAxis<C>>["check"],
    });
  }
  constructor(from: PredefinedEpochAxis<C> | C) {
    super(from);
    if (from instanceof PredefinedEpochAxis) {
      this.firstEpoch = from.firstEpoch;
      this.epoch = new FlagDataArray(from.epoch);
      this.factor = from.factor;
      this._used = from._used;
      this.lastEpochIdxInUse = from.lastEpochIdxInUse;
      this.firstEpochIdxInUse = from.firstEpochIdxInUse;
    } else {
      this.epoch = new FlagDataArray();
      this.firstEpoch = 0;
      this.factor = 1;
      this._used = 0;
      this.lastEpochIdxInUse = -1;
      this.firstEpochIdxInUse = -1;
    }
  }
  optimize(opt: EpochAxisOpt): this {
    if (!this._used) this.factor = opt.gap;
    opt.gte = Math.floor(opt.gte / this.factor) * this.factor;
    opt.lte = Math.ceil(opt.lte / this.factor) * this.factor;
    const size = (opt.lte - opt.gte) / opt.gap;
    const needMore = size - (this.epoch.length - this._used);
    if (needMore > 0) this.expand(needMore);
    this.setFirst(opt.gte);
    return this;
  }
  private setFirst(id: number) {
    if (!this._used) {
      if (!this.firstEpoch) {
        this.firstEpoch = id;
      } else if (this.firstEpoch > id) {
        this.firstEpoch = id;
      }
    } else if (!this.firstEpoch) {
      this.firstEpoch = id;
    } else if (this.firstEpoch > id) {
      const needSpace = (id - this.firstEpoch) / this.factor;
      if (this.epoch.length - 1 - this.lastEpochIdxInUse < needSpace) {
        throw new Error("No space left!");
      }
      this.epoch
        .copyWithin(
          needSpace + this.firstEpochIdxInUse,
          this.firstEpochIdxInUse,
          this.lastEpochIdxInUse + 1,
        )
        .clear(this.firstEpochIdxInUse, needSpace + this.firstEpochIdxInUse);
      for (const k in this.columns) {
        this.columns[k]
          .copyWithinFromRow(
            needSpace + this.firstEpochIdxInUse,
            this.firstEpochIdxInUse,
            this.lastEpochIdxInUse + 1,
          )
          .clearFromRow(
            this.firstEpochIdxInUse,
            needSpace + this.firstEpochIdxInUse,
          );
      }
      this.firstEpoch += needSpace * this.factor;
      this.lastEpochIdxInUse += needSpace;
      this.firstEpochIdxInUse += needSpace;
    } else if (this.epoch.length <= (id - this.firstEpoch) / this.factor) {
      const needSpace = (id - this.firstEpoch) / this.factor -
        this.epoch.length;
      if (this.firstEpochIdxInUse < needSpace) {
        throw new Error("No space left!");
      }
      this.epoch
        .copyWithin(
          this.firstEpochIdxInUse - needSpace,
          this.firstEpochIdxInUse,
          this.lastEpochIdxInUse + 1,
        )
        .clear(this.lastEpochIdxInUse - needSpace, this.lastEpochIdxInUse + 1);
      for (const k in this.columns) {
        this.columns[k]
          .copyWithinFromRow(
            this.firstEpochIdxInUse - needSpace,
            this.firstEpochIdxInUse,
            this.lastEpochIdxInUse + 1,
          )
          .clearFromRow(
            this.lastEpochIdxInUse - needSpace,
            this.lastEpochIdxInUse + 1,
          );
      }
      this.firstEpoch -= needSpace * this.factor;
      this.lastEpochIdxInUse -= needSpace;
      this.firstEpochIdxInUse -= needSpace;
    }
  }

  copy(): this {
    return new PredefinedEpochAxis(this) as this;
  }
  protected get used(): number {
    return this._used;
  }
  protected get capacity(): number {
    return this.epoch.length;
  }
  protected expand(add: number): this {
    this.epoch.expand(add);
    for (const k in this.columns) {
      this.columns[k].expandRowSize(add);
    }
    return this;
  }
  protected make(id: number): this {
    id = Math.floor(id / this.factor) * this.factor;
    if (this.epoch.getAt(id - this.firstEpoch)) {
      throw new Error("Already Exists");
    }
    this.setFirst(id);
    const idx = (id - this.firstEpoch) / this.factor;
    if (idx >= this.epoch.length) throw new Error("No space left!");
    this.epoch.setAt(idx, true);
    if (this._used == 0) {
      this._used = 1;
      this.firstEpochIdxInUse = idx;
      this.lastEpochIdxInUse = idx;
    } else {
      this._used++;
      if (this.firstEpochIdxInUse > idx) this.firstEpochIdxInUse = idx;
      if (this.lastEpochIdxInUse < idx) this.lastEpochIdxInUse = idx;
    }
    return this;
  }
  protected remove(id: number): this {
    id = Math.floor(id / this.factor) * this.factor;
    const idx = (id - this.firstEpoch) / this.factor;
    if (this.epoch.getAt(idx) == undefined) throw new Error("Not found");
    this.epoch.delAt(idx);
    for (const k in this.columns) {
      this.columns[k].delFromRow(idx);
    }
    if (this._used === 2) {
      if (this.firstEpochIdxInUse === idx) {
        this.firstEpochIdxInUse = this.lastEpochIdxInUse;
      } else {
        this.lastEpochIdxInUse = this.firstEpochIdxInUse;
      }
      this._used = 1;
    } else if (this.used === 1) {
      this.firstEpochIdxInUse = -1;
      this.lastEpochIdxInUse = -1;
    } else {
      this._used--;
      if (idx === this.firstEpochIdxInUse) {
        for (
          let i = this.firstEpochIdxInUse + 1;
          i < this.lastEpochIdxInUse;
          i++
        ) {
          if (this.epoch.getAt(i)) {
            this.firstEpochIdxInUse = i;
          }
        }
      } else if (idx === this.lastEpochIdxInUse) {
        for (
          let i = this.lastEpochIdxInUse - 1;
          i > this.firstEpochIdxInUse;
          i--
        ) {
          if (this.epoch.getAt(i)) {
            this.lastEpochIdxInUse = i;
          }
        }
      }
    }
    return this;
  }
  protected getIdAt(idx: number): number | undefined {
    if (this.epoch.getAt(idx)) return this.firstEpoch + idx * this.factor;
    return undefined;
  }
  protected getIdxId(id: number): number | undefined {
    id = Math.floor(id / this.factor) * this.factor;
    const idx = (id - this.firstEpoch) / this.factor;
    if (this.epoch.getAt(idx)) return idx;
    return undefined;
  }
  protected *getIdsWithIndex(): IterableIterator<[number, number]> {
    if (this._used) {
      for (
        const idx of this.epoch.getFlagedIdx(
          this.firstEpochIdxInUse,
          this.lastEpochIdxInUse + 1,
        )
      ) {
        yield [this.firstEpoch + idx * this.factor, idx];
      }
    }
  }
  protected *getIds(): IterableIterator<number> {
    if (this._used) {
      for (
        const idx of this.epoch.getFlagedIdx(
          this.firstEpochIdxInUse,
          this.lastEpochIdxInUse + 1,
        )
      ) {
        yield this.firstEpoch + idx * this.factor;
      }
    }
  }
  protected *getIdIndex(): IterableIterator<number> {
    if (this._used) {
      for (
        const idx of this.epoch.getFlagedIdx(
          this.firstEpochIdxInUse,
          this.lastEpochIdxInUse + 1,
        )
      ) {
        yield idx;
      }
    }
  }
  idToStr(val: number | undefined): string {
    return val?.toString() ?? "";
  }

  filter(
    parser: GenericParser<PredefinedEpochAxis<C>>,
    predicate: ((row: number, rIdx: number) => boolean) | [number, number][],
  ): PredefinedEpochAxis<C> {
    const gap = this.factor;
    return this._filter(predicate, (rows) => {
      return parser.create().optimize({
        gap,
        gte: Math.min(...rows.map((x) => x[0])),
        lte: Math.max(...rows.map((x) => x[0])) + gap,
      });
    });
  }
}
