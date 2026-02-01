export abstract class DataArr<DT> {
  constructor() {}
  abstract get length(): number;
  abstract copy(): this;
  abstract copyWithin(
    targetIndex: number,
    startIndex: number,
    endIndex: number,
  ): this;
  abstract expand(add: number): this;
  abstract fill(value: DT, startIndex?: number, endIndex?: number): this;
  abstract clear(startIndex?: number, endIndex?: number): this;
  abstract getAt(index: number): DT | undefined;
  abstract setAt(index: number, value: DT): void;
  abstract delAt(index: number): void;
  abstract valToStr(val: DT | undefined): string;
}
export type inferDTFromDC<DC extends DataArr<any>> = DC extends
  DataArr<infer DT> ? DT : never;
export class StrDataArray extends DataArr<string> {
  private data: Uint16Array;
  private indexVal: string[];
  private indexValMap: Map<string, number>; // inverse mapping of indexVal
  static readonly Undefined: string = "";
  static isUndefined(str: string): boolean {
    return str == StrDataArray.Undefined;
  }
  constructor(from?: StrDataArray) {
    super();
    if (!from) {
      this.data = new Uint16Array(0).fill(0);
      this.indexVal = [StrDataArray.Undefined];
      this.indexValMap = new Map([[StrDataArray.Undefined, 0]]);
    } else {
      this.data = new Uint16Array(from.data);
      this.indexVal = [...from.indexVal];
      this.indexValMap = new Map(from.indexValMap);
    }
  }
  get length(): number {
    return this.data.length;
  }
  copy(): this {
    return new StrDataArray(this) as this;
  }
  copyWithin(targetIndex: number, startIndex: number, endIndex: number): this {
    this.data.copyWithin(targetIndex, startIndex, endIndex);
    return this;
  }
  expand(add: number): this {
    if (add < 1) throw new Error("Cannot reduce space");
    const data = new Uint16Array(this.data.length + add);
    data.set(this.data);
    this.data = data;
    return this;
  }
  fill(value: string, startIndex?: number, endIndex?: number): this {
    let valIdx = this.indexValMap.get(value)!;
    if (valIdx === undefined) {
      valIdx = this.indexVal.length;
      this.indexVal.push(value);
      this.indexValMap.set(value, valIdx);
    }
    this.data.fill(valIdx, startIndex, endIndex);
    return this;
  }
  clear(startIndex?: number, endIndex?: number): this {
    this.data.fill(0, startIndex, endIndex);
    return this;
  }
  getAt(index: number): string | undefined {
    const val = this.indexVal[this.data[index]];
    if (StrDataArray.isUndefined(val)) return undefined;
    return val;
  }
  setAt(index: number, value: string): void {
    let valIdx = this.indexValMap.get(value)!;
    if (valIdx === undefined) {
      valIdx = this.indexVal.length;
      this.indexVal.push(value);
      this.indexValMap.set(value, valIdx);
    }
    this.data[index] = valIdx;
  }
  delAt(index: number): void {
    this.data[index] = 0;
  }

  valToStr(val: string | undefined): string {
    return val ?? "";
  }
}
export class FloatDataArray extends DataArr<number> {
  private data: Float32Array;
  static readonly Undefined = NaN;
  static isUndefined(val: number): boolean {
    return isNaN(val);
  }
  constructor(from?: FloatDataArray) {
    super();
    if (!from) {
      this.data = new Float32Array(0).fill(FloatDataArray.Undefined);
    } else {
      this.data = new Float32Array(from.data);
    }
  }
  get length(): number {
    return this.data.length;
  }
  copy(): this {
    return new FloatDataArray(this) as this;
  }

  copyWithin(targetIndex: number, startIndex: number, endIndex: number): this {
    this.data.copyWithin(targetIndex, startIndex, endIndex);
    return this;
  }
  expand(add: number): this {
    if (add < 1) throw new Error("Cannot reduce space");
    const data = new Float32Array(this.data.length + add);
    data.set(this.data);
    data.fill(FloatDataArray.Undefined, this.data.length);
    this.data = data;
    return this;
  }
  fill(value: number, startIndex?: number, endIndex?: number): this {
    this.data.fill(value, startIndex, endIndex);
    return this;
  }
  clear(startIndex?: number, endIndex?: number): this {
    this.data.fill(FloatDataArray.Undefined, startIndex, endIndex);
    return this;
  }
  getAt(index: number): number | undefined {
    const val = this.data[index];
    if (FloatDataArray.isUndefined(val)) return undefined;
    return val;
  }
  setAt(index: number, value: number): void {
    this.data[index] = value;
  }
  delAt(index: number): void {
    this.data[index] = FloatDataArray.Undefined;
  }

  valToStr(val: number | undefined): string {
    return val?.toFixed(4) ?? "";
  }
}
export class FlagDataArray extends DataArr<boolean> {
  private data: Uint8Array;
  private _length: number;

  constructor(from?: FlagDataArray) {
    super();
    if (from) {
      this._length = from._length;
      this.data = new Uint8Array(from.data);
    } else {
      this._length = 0;
      this.data = new Uint8Array(0);
    }
  }

  get length(): number {
    return this._length;
  }

  getAt(index: number): boolean {
    if (index < 0 || index >= this._length) return false;
    return (this.data[index >> 3] & (1 << (index & 7))) !== 0;
  }

  setAt(index: number, value: boolean): void {
    if (index < 0 || index >= this._length) return;
    if (value) {
      this.data[index >> 3] |= 1 << (index & 7);
    } else {
      this.data[index >> 3] &= ~(1 << (index & 7));
    }
  }

  expand(add: number): this {
    if (add < 1) throw new Error("Cannot reduce space");
    const _length = this._length + add;
    const newByteLength = Math.ceil(_length / 8);
    if (newByteLength > this.data.length) {
      const newData = new Uint8Array(newByteLength);
      newData.set(this.data);
      this.data = newData;
    }
    this._length = _length;
    return this;
  }

  copy(): this {
    return new FlagDataArray(this) as this;
  }

  fill(
    value: boolean,
    startIndex: number = 0,
    endIndex: number = this._length,
  ): this {
    if (startIndex >= endIndex) return this;
    while (startIndex < endIndex && (startIndex & 7) !== 0) {
      this.setAt(startIndex++, value);
    }
    const startByte = startIndex >> 3;
    const endByte = endIndex >> 3;
    if (startByte < endByte) {
      this.data.fill(value ? 0xff : 0, startByte, endByte);
      startIndex = endByte << 3;
    }
    while (startIndex < endIndex) {
      this.setAt(startIndex++, value);
    }
    return this;
  }

  clear(startIndex?: number, endIndex?: number): this {
    return this.fill(false, startIndex, endIndex);
  }

  delAt(index: number): void {
    this.setAt(index, false);
  }

  copyWithin(targetIndex: number, startIndex: number, endIndex: number): this {
    const diff = targetIndex - startIndex;
    if (targetIndex < startIndex) {
      for (let i = startIndex; i < endIndex; i++) {
        this.setAt(i + diff, this.getAt(i));
      }
    } else {
      for (let i = endIndex - 1; i >= startIndex; i--) {
        this.setAt(i + diff, this.getAt(i));
      }
    }
    return this;
  }

  *getValues(
    start: number = 0,
    end: number = this._length,
    dir: 1 | -1 = 1,
  ): IterableIterator<[number, boolean]> {
    if (dir === 1) {
      for (let i = start; i < end; i++) {
        yield [i, (this.data[i >> 3] & (1 << (i & 7))) !== 0];
      }
    } else {
      for (let i = end - 1; i >= start; i--) {
        yield [i, (this.data[i >> 3] & (1 << (i & 7))) !== 0];
      }
    }
  }

  valToStr(val: boolean | undefined): string {
    return val ? "ׂ̇·" : "";
  }
}
