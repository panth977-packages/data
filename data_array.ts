import { GenericParser, IdVersionParser, Parsers } from "./_parser.ts";

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
  private static id: ArrayBuffer = IdVersionParser.create("StrDataArray", 1);
  protected static encode(str: StrDataArray): ArrayBuffer {
    const data = Parsers.Uint16Array.encode(str.data);
    const indexVal = Parsers.StringList.encode(str.indexVal);
    return Parsers.ArrayBufferList.encode([this.id, data, indexVal]);
  }
  protected static decode(buff: ArrayBuffer): StrDataArray {
    const [id, data, indexVal] = Parsers.ArrayBufferList.decode(buff);
    IdVersionParser.check(id, this.id);
    const str = new StrDataArray();
    str.data = Parsers.Uint16Array.decode(data);
    str.indexVal = Parsers.StringList.decode(indexVal);
    str.indexValMap = new Map(str.indexVal.map((val, i) => [val, i]));
    return str;
  }

  protected static create(value?: StrDataArray): StrDataArray {
    return new StrDataArray(value);
  }
  protected static check(value: unknown): value is StrDataArray {
    return value instanceof StrDataArray;
  }

  static parser(): GenericParser<StrDataArray> {
    return new GenericParser<StrDataArray>({
      encode: StrDataArray.encode.bind(StrDataArray),
      decode: StrDataArray.decode.bind(StrDataArray),
      create: StrDataArray.create.bind(StrDataArray),
      check: StrDataArray.check.bind(StrDataArray),
    });
  }
  private static undefined = 65535;

  constructor(from?: StrDataArray) {
    super();
    if (!from) {
      this.data = new Uint16Array(0).fill(StrDataArray.undefined);
      this.indexVal = [];
      this.indexValMap = new Map();
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
    data.fill(StrDataArray.undefined, this.data.length);
    this.data = data;
    return this;
  }
  fill(value: string, startIndex?: number, endIndex?: number): this {
    let valIdx = this.indexValMap.get(value)!;
    if (valIdx === undefined) {
      valIdx = this.indexVal.length;
      if (StrDataArray.undefined === valIdx) {
        throw new Error("All unique values are already used");
      }
      this.indexVal.push(value);
      this.indexValMap.set(value, valIdx);
    }
    this.data.fill(valIdx, startIndex, endIndex);
    return this;
  }
  clear(startIndex?: number, endIndex?: number): this {
    this.data.fill(StrDataArray.undefined, startIndex, endIndex);
    return this;
  }
  getAt(index: number): string | undefined {
    const i = this.data[index];
    if (i === StrDataArray.undefined) return undefined;
    return this.indexVal[i];
  }
  setAt(index: number, value: string): void {
    let valIdx = this.indexValMap.get(value)!;
    if (valIdx === undefined) {
      valIdx = this.indexVal.length;
      if (StrDataArray.undefined === valIdx) {
        throw new Error("All unique values are already used");
      }
      this.indexVal.push(value);
      this.indexValMap.set(value, valIdx);
    }
    this.data[index] = valIdx;
  }
  delAt(index: number): void {
    this.data[index] = StrDataArray.undefined;
  }

  valToStr(val: string | undefined): string {
    return val ?? "";
  }
}
export class FloatDataArray extends DataArr<number> {
  private data: Float32Array;
  private static id: ArrayBuffer = IdVersionParser.create("FloatDataArray", 1);
  private static undefined: number = -(2 ** 49);
  protected static encode(float: FloatDataArray): ArrayBuffer {
    const data = Parsers.Float32Array.encode(float.data);
    return Parsers.ArrayBufferList.encode([this.id, data]);
  }
  protected static decode(buff: ArrayBuffer): FloatDataArray {
    const [id, data] = Parsers.ArrayBufferList.decode(buff);
    IdVersionParser.check(id, this.id);
    const float = new FloatDataArray();
    float.data = Parsers.Float32Array.decode(data);
    return float;
  }
  protected static create(value?: FloatDataArray): FloatDataArray {
    return new FloatDataArray(value);
  }
  protected static check(value: unknown): value is FloatDataArray {
    return value instanceof FloatDataArray;
  }

  static parser(): GenericParser<FloatDataArray> {
    return new GenericParser({
      encode: FloatDataArray.encode.bind(FloatDataArray),
      decode: FloatDataArray.decode.bind(FloatDataArray),
      create: FloatDataArray.create.bind(FloatDataArray),
      check: FloatDataArray.check.bind(FloatDataArray),
    });
  }
  constructor(from?: FloatDataArray) {
    super();
    if (!from) {
      this.data = new Float32Array(0).fill(FloatDataArray.undefined);
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
    data.fill(FloatDataArray.undefined, this.data.length);
    this.data = data;
    return this;
  }
  fill(value: number, startIndex?: number, endIndex?: number): this {
    this.data.fill(value, startIndex, endIndex);
    return this;
  }
  clear(startIndex?: number, endIndex?: number): this {
    this.data.fill(FloatDataArray.undefined, startIndex, endIndex);
    return this;
  }
  getAt(index: number): number | undefined {
    const val = this.data[index];
    if (val === FloatDataArray.undefined) return undefined;
    return val;
  }
  setAt(index: number, value: number): void {
    this.data[index] = value;
  }
  delAt(index: number): void {
    this.data[index] = FloatDataArray.undefined;
  }

  valToStr(val: number | undefined): string {
    return val?.toFixed(4) ?? "";
  }
}
export class FlagDataArray extends DataArr<true> {
  private data: Uint8Array;
  private _length: number;
  private static id: ArrayBuffer = IdVersionParser.create("FlagDataArray", 1);
  protected static encode(flag: FlagDataArray): ArrayBuffer {
    const data = Parsers.Uint8Array.encode(flag.data);
    const len = Parsers.Number.encode(flag._length);
    return Parsers.ArrayBufferList.encode([this.id, data, len]);
  }
  protected static decode(buff: ArrayBuffer): FlagDataArray {
    const [id, data, len] = Parsers.ArrayBufferList.decode(buff);
    IdVersionParser.check(id, this.id);
    const flag = new FlagDataArray();
    flag.data = Parsers.Uint8Array.decode(data);
    flag._length = Parsers.Number.decode(len);
    return flag;
  }
  protected static create(flag?: FlagDataArray): FlagDataArray {
    return new FlagDataArray(flag);
  }
  protected static check(value: unknown): value is FlagDataArray {
    return value instanceof FlagDataArray;
  }
  static parser(): GenericParser<FlagDataArray> {
    return new GenericParser({
      encode: FlagDataArray.encode.bind(FlagDataArray),
      decode: FlagDataArray.decode.bind(FlagDataArray),
      create: FlagDataArray.create.bind(FlagDataArray),
      check: FlagDataArray.check.bind(FlagDataArray),
    });
  }
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

  getAt(index: number): true | undefined {
    if (index < 0 || index >= this._length) return undefined;
    return ((this.data[index >> 3] & (1 << (index & 7))) !== 0) || undefined;
  }

  setAt(index: number, _value: true): void {
    if (index < 0 || index >= this._length) return;
    this.data[index >> 3] |= 1 << (index & 7);
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
    _value: true,
    startIndex: number = 0,
    endIndex: number = this._length,
  ): this {
    if (startIndex >= endIndex) return this;
    while (startIndex < endIndex && (startIndex & 7) !== 0) {
      this.setAt(startIndex++, true);
    }
    const startByte = startIndex >> 3;
    const endByte = endIndex >> 3;
    if (startByte < endByte) {
      this.data.fill(0xff, startByte, endByte);
      startIndex = endByte << 3;
    }
    while (startIndex < endIndex) {
      this.setAt(startIndex++, true);
    }
    return this;
  }

  clear(startIndex: number = 0, endIndex: number = this._length): this {
    if (startIndex >= endIndex) return this;
    while (startIndex < endIndex && (startIndex & 7) !== 0) {
      this.delAt(startIndex++);
    }
    const startByte = startIndex >> 3;
    const endByte = endIndex >> 3;
    if (startByte < endByte) {
      this.data.fill(0, startByte, endByte);
      startIndex = endByte << 3;
    }
    while (startIndex < endIndex) {
      this.delAt(startIndex++);
    }
    return this;
  }

  delAt(index: number): void {
    if (index < 0 || index >= this._length) return;
    this.data[index >> 3] &= ~(1 << (index & 7));
  }

  copyWithin(targetIndex: number, startIndex: number, endIndex: number): this {
    const diff = targetIndex - startIndex;
    if (targetIndex < startIndex) {
      for (let i = startIndex; i < endIndex; i++) {
        if (this.getAt(i)) {
          this.setAt(i + diff, true);
        } else {
          this.delAt(i + diff);
        }
      }
    } else {
      for (let i = endIndex - 1; i >= startIndex; i--) {
        if (this.getAt(i)) {
          this.setAt(i + diff, true);
        } else {
          this.delAt(i + diff);
        }
      }
    }
    return this;
  }

  *getFlagedIdx(
    start: number = 0,
    end: number = this._length,
    dir: 1 | -1 = 1,
  ): IterableIterator<number> {
    if (dir === 1) {
      for (let i = start; i < end; i++) {
        if ((this.data[i >> 3] & (1 << (i & 7))) !== 0) yield i;
      }
    } else {
      for (let i = end - 1; i >= start; i--) {
        if ((this.data[i >> 3] & (1 << (i & 7))) !== 0) yield i;
      }
    }
  }

  valToStr(val: true | undefined): string {
    return val ? "·" : "";
  }
}
