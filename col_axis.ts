import type { DataArr, inferDTFromDC } from "./data_array.ts";

export abstract class ColumnAxis<CT, DC extends DataArr<any>> {
  protected rowSize: number;
  protected data: DC;
  constructor(from: ColumnAxis<CT, DC> | DC) {
    if (from instanceof ColumnAxis) {
      this.data = from.data.copy();
      this.rowSize = from.rowSize;
    } else {
      this.data = from.copy();
      this.rowSize = 0;
    }
  }
  abstract get used(): number;
  abstract get capacity(): number;
  abstract copy(): this;
  abstract expand(add: number): this;
  abstract make(id: CT): this;
  abstract remove(id: CT): this;
  abstract getIdAt(idx: number): CT | undefined;
  abstract getIdxId(id: CT): number | undefined;
  abstract getIdsWithIndex(): IterableIterator<[CT, number]>;
  abstract getIds(): IterableIterator<CT>;
  abstract getIdIndex(): IterableIterator<number>;
  getAt(rIdx: number, idx: number): inferDTFromDC<DC> | undefined {
    return this.data.getAt(rIdx * this.capacity + idx);
  }
  setAt(rIdx: number, idx: number, value: inferDTFromDC<DC>) {
    this.data.setAt(rIdx * this.capacity + idx, value);
  }
  delAt(rIdx: number, idx: number) {
    this.data.delAt(rIdx * this.capacity + idx);
  }
  expandRowSize(add: number): this {
    if (add < 1) throw new Error("Cannot add");
    this.rowSize += add;
    if (this.capacity > 0) this.data.expand(add * this.capacity);
    return this;
  }
  copyWithinFromRow(targetIdx: number, startIdx: number, endIdx: number): this {
    this.data.copyWithin(
      targetIdx * this.capacity,
      startIdx * this.capacity,
      endIdx * this.capacity,
    );
    return this;
  }
  delFromRow(idx: number) {
    this.data.clear(idx * this.capacity, (idx + 1) * this.capacity);
  }
  clearFromRow(startIdx: number, endIdx: number): this {
    this.data.clear(startIdx * this.capacity, endIdx * this.capacity);
    return this;
  }

  toPrettyTable(colWidth = 15) {
    const horizontalBar = "─".repeat(colWidth + 2);
    const topBar = "┌" + Array(this.capacity).fill(horizontalBar).join("┬") +
      "┐";
    const midBar = "├" + Array(this.capacity).fill(horizontalBar).join("┼") +
      "┤";
    const botBar = "└" + Array(this.capacity).fill(horizontalBar).join("┴") +
      "┘";
    const lines = [topBar];
    let headerRow = "│";
    for (let cIdx = 0; cIdx < this.capacity; cIdx++) {
      const rawKey = this.idToStr(this.getIdAt(cIdx));
      const cleanKey = rawKey.toString().substring(0, colWidth).padEnd(
        colWidth,
      );
      headerRow += ` ${cleanKey} │`;
    }
    lines.push(headerRow);
    lines.push(midBar);
    for (let rIdx = 0; rIdx < this.rowSize; rIdx++) {
      let rowStr = "│";
      for (let cIdx = 0; cIdx < this.capacity; cIdx++) {
        const displayVal = this.data.valToStr(this.getAt(rIdx, cIdx));
        rowStr += ` ${displayVal.substring(0, colWidth).padEnd(colWidth)} │`;
      }
      lines.push(rowStr);
    }
    lines.push(botBar);
    return lines.join("\n");
  }
  abstract idToStr(val: CT | undefined): string;
}
export class KeyAxis<DC extends DataArr<any>> extends ColumnAxis<string, DC> {
  private keys: Array<string>;
  private keyMapping: Map<string, number>;
  private unusedKeyIdx: Array<number>;
  static readonly Undefined: string = "";
  constructor(from: KeyAxis<DC> | DC) {
    super(from);
    if (from instanceof KeyAxis) {
      this.keyMapping = new Map(from.keyMapping);
      this.keys = [...from.keys];
      this.unusedKeyIdx = [...from.unusedKeyIdx];
    } else {
      this.keys = new Array(0).fill(KeyAxis.Undefined);
      this.keyMapping = new Map();
      this.unusedKeyIdx = [];
    }
  }
  get used(): number {
    return this.keyMapping.size;
  }
  get capacity(): number {
    return this.keys.length;
  }
  copy(): this {
    return new KeyAxis(this) as this;
  }
  expand(add: number): this {
    if (add < 1) throw new Error("Cannot reduce space");
    const currentKeyLen = this.keys.length;
    const newKeyLen = currentKeyLen + add;
    if (this.rowSize) {
      this.data.expand(this.rowSize * add);
      for (let tIdx = this.rowSize - 1; tIdx > -1; tIdx--) {
        this.data
          .copyWithin(
            tIdx * newKeyLen,
            tIdx * currentKeyLen,
            (tIdx + 1) * currentKeyLen,
          )
          .clear(tIdx * newKeyLen + currentKeyLen, (tIdx + 1) * newKeyLen);
      }
    }
    for (let idx = this.keys.length + add - 1; idx >= this.keys.length; idx--) {
      this.unusedKeyIdx.push(idx);
    }
    this.keys = [...this.keys, ...new Array(add).fill(KeyAxis.Undefined)];
    return this;
  }
  make(id: string): this {
    if (this.keyMapping.has(id)) throw new Error("Already Exists");
    const idx = this.unusedKeyIdx.pop();
    if (idx == undefined) throw new Error("No space!");
    this.keys[idx] = id;
    this.keyMapping.set(id, idx);
    return this;
  }
  remove(id: string): this {
    const idx = this.keyMapping.get(id);
    if (idx == undefined) throw new Error("Not found");
    for (let rIdx = 0; rIdx < this.rowSize; rIdx++) {
      this.data.delAt(rIdx * this.capacity + idx);
    }
    this.keys[idx] = KeyAxis.Undefined;
    this.keyMapping.delete(id);
    this.unusedKeyIdx.push(idx);
    return this;
  }
  getIdAt(idx: number): string | undefined {
    return this.keys[idx];
  }
  getIdxId(id: string): number | undefined {
    return this.keyMapping.get(id);
  }

  getIdIndex(): IterableIterator<number> {
    return this.keyMapping.values();
  }
  getIds(): IterableIterator<string> {
    return this.keyMapping.keys();
  }
  getIdsWithIndex(): IterableIterator<[string, number]> {
    return this.keyMapping.entries();
  }
  idToStr(val: string | undefined): string {
    return val ?? "";
  }
}
