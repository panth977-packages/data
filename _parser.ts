export abstract class Parser<T> {
  // Helper for string encoding/decoding
  protected static readonly encoder: TextEncoder = new TextEncoder();
  protected static readonly decoder: TextDecoder = new TextDecoder();

  // static toBase64(buff: ArrayBuffer): string {
  //   // Determine environment to choose the fastest native method
  //   if (typeof Buffer !== "undefined") {
  //     // Node.js
  //     return Buffer.from(buff).toString("base64");
  //   } else {
  //     // Browser: Efficiently map huge buffers without stack overflow
  //     let binary = "";
  //     const bytes = new Uint8Array(buff);
  //     const len = bytes.byteLength;
  //     for (let i = 0; i < len; i++) {
  //       binary += String.fromCharCode(bytes[i]);
  //     }
  //     return btoa(binary);
  //   }
  // }

  // static fromBase64(base64: string): ArrayBuffer {
  //   if (typeof Buffer !== "undefined") {
  //     // Node.js
  //     const buf = Buffer.from(base64, "base64");
  //     return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
  //   } else {
  //     // Browser
  //     const binary_string = atob(base64);
  //     const len = binary_string.length;
  //     const bytes = new Uint8Array(len);
  //     for (let i = 0; i < len; i++) {
  //       bytes[i] = binary_string.charCodeAt(i);
  //     }
  //     return bytes.buffer;
  //   }
  // }

  abstract encode(data: T): ArrayBuffer;
  abstract decode(data: ArrayBuffer): T;
}

// ---------------------------------------------------------
// 1. String List Parser
// Format: [Total Count (Uint32)] -> [Str Len (Uint32)][Str Bytes]...
// ---------------------------------------------------------
export class StringListParser extends Parser<string[]> {
  encode(data: string[]): ArrayBuffer {
    // 1. Encode all strings to Uint8Arrays first to know exact size
    const encodedStrings: Uint8Array[] = new Array(data.length);
    let totalByteLength = 4; // Start with 4 bytes for the list count

    for (let i = 0; i < data.length; i++) {
      const bytes = Parser.encoder.encode(data[i]);
      encodedStrings[i] = bytes;
      totalByteLength += 4 + bytes.byteLength; // 4 bytes for length header + content
    }

    // 2. Allocate exact buffer
    const buffer = new ArrayBuffer(totalByteLength);
    const view = new DataView(buffer);
    const u8View = new Uint8Array(buffer);

    // 3. Write Data
    let offset = 0;

    // Write List Count
    view.setUint32(offset, data.length, true); // true = Little Endian
    offset += 4;

    for (const bytes of encodedStrings) {
      // Write String Length
      view.setUint32(offset, bytes.byteLength, true);
      offset += 4;
      // Write String Bytes
      u8View.set(bytes, offset);
      offset += bytes.byteLength;
    }

    return buffer;
  }

  decode(data: ArrayBuffer): string[] {
    const view = new DataView(data);
    const u8View = new Uint8Array(data);
    let offset = 0;

    // Read Count
    const count = view.getUint32(offset, true);
    offset += 4;

    const result: string[] = new Array(count);

    for (let i = 0; i < count; i++) {
      const len = view.getUint32(offset, true);
      offset += 4;

      // Slice is cheap on TypedArrays (creates a view, not a copy)
      // but TextDecoder needs a view of the specific range.
      const strBytes = u8View.subarray(offset, offset + len);
      result[i] = Parser.decoder.decode(strBytes);
      offset += len;
    }

    return result;
  }
}

// ---------------------------------------------------------
// 2. TypedArray Parsers
// These are extremely fast (Zero Copy where possible)
// ---------------------------------------------------------

export class Uint16ArrayParser extends Parser<Uint16Array> {
  encode(data: Uint16Array): ArrayBuffer {
    // Create a copy of the underlying buffer to ensure no side effects
    // If you want zero-copy encode, you can just return data.buffer,
    // but slicing ensures we respect offset/length if it's a partial view.
    return data.buffer.slice(
      data.byteOffset,
      data.byteOffset + data.byteLength,
    );
  }

  decode(data: ArrayBuffer): Uint16Array {
    return new Uint16Array(data);
  }
}

export class Float32ArrayParser extends Parser<Float32Array> {
  encode(data: Float32Array): ArrayBuffer {
    return data.buffer.slice(
      data.byteOffset,
      data.byteOffset + data.byteLength,
    );
  }

  decode(data: ArrayBuffer): Float32Array {
    return new Float32Array(data);
  }
}

export class Uint8ArrayParser extends Parser<Uint8Array> {
  encode(data: Uint8Array): ArrayBuffer {
    return data.buffer.slice(
      data.byteOffset,
      data.byteOffset + data.byteLength,
    );
  }

  decode(data: ArrayBuffer): Uint8Array {
    return new Uint8Array(data);
  }
}

export class Uint32ArrayParser extends Parser<Uint32Array> {
  encode(data: Uint32Array): ArrayBuffer {
    // Create a copy of the underlying buffer to ensure no side effects
    // If you want zero-copy encode, you can just return data.buffer,
    // but slicing ensures we respect offset/length if it's a partial view.
    return data.buffer.slice(
      data.byteOffset,
      data.byteOffset + data.byteLength,
    );
  }

  decode(data: ArrayBuffer): Uint32Array {
    return new Uint32Array(data);
  }
}
// ---------------------------------------------------------
// 3. Tuple Parser [ID, Version]
// Format: [ID Len (Uint32)][ID Bytes][Version (Float64)]
// We use Float64 for version to support all JS numbers safely.
// ---------------------------------------------------------
export class IdVersionParser extends Parser<[string, number]> {
  static create(id: string, version: number): ArrayBuffer {
    return Parsers.IdVersion.encode([id, version]);
  }

  static check(found: ArrayBuffer, expected: ArrayBuffer): void {
    const [foundId, foundVersion] = Parsers.IdVersion.decode(found);
    const [expectedId, expectedVersion] = Parsers.IdVersion.decode(expected);
    if (foundId !== expectedId) {
      throw new Error(`Expected ID "${expectedId}", but found "${foundId}"`);
    }
    if (foundVersion !== expectedVersion) {
      throw new Error(
        `Expected version "${expectedVersion}", but found "${foundVersion}"`,
      );
    }
  }
  encode(data: [string, number]): ArrayBuffer {
    const [id, version] = data;
    const idBytes = Parser.encoder.encode(id);

    // Layout: [4 bytes ID len] + [N bytes ID] + [8 bytes Version Number]
    const totalSize = 4 + idBytes.byteLength + 8;

    const buffer = new ArrayBuffer(totalSize);
    const view = new DataView(buffer);
    const u8View = new Uint8Array(buffer);

    let offset = 0;

    // Write ID Length
    view.setUint32(offset, idBytes.byteLength, true);
    offset += 4;

    // Write ID Bytes
    u8View.set(idBytes, offset);
    offset += idBytes.byteLength;

    // Write Version (Float64 to match JS number precision)
    view.setFloat64(offset, version, true);

    return buffer;
  }

  decode(data: ArrayBuffer): [string, number] {
    const view = new DataView(data);
    const u8View = new Uint8Array(data);
    let offset = 0;

    // Read ID Length
    const idLen = view.getUint32(offset, true);
    offset += 4;

    // Read ID
    const idBytes = u8View.subarray(offset, offset + idLen);
    const id = Parser.decoder.decode(idBytes);
    offset += idLen;

    // Read Version
    const version = view.getFloat64(offset, true);

    return [id, version];
  }
}

// ---------------------------------------------------------
// 4. ArrayBuffer List Parser
// Format: [Count (Uint32)] -> [Buf Len (Uint32)][Buf Bytes]...
// ---------------------------------------------------------
export class ArrayBufferListParser extends Parser<ArrayBuffer[]> {
  encode(data: ArrayBuffer[]): ArrayBuffer {
    let totalByteLength = 4; // Start with 4 bytes for count

    // 1. Calculate size
    for (const buf of data) {
      totalByteLength += 4 + buf.byteLength;
    }

    // 2. Allocate
    const resultBuffer = new ArrayBuffer(totalByteLength);
    const view = new DataView(resultBuffer);
    const u8View = new Uint8Array(resultBuffer);

    let offset = 0;

    // 3. Write Count
    view.setUint32(offset, data.length, true);
    offset += 4;

    // 4. Write Buffers
    for (const buf of data) {
      // Write Length
      view.setUint32(offset, buf.byteLength, true);
      offset += 4;

      // Write Bytes
      u8View.set(new Uint8Array(buf), offset);
      offset += buf.byteLength;
    }

    return resultBuffer;
  }

  decode(data: ArrayBuffer): ArrayBuffer[] {
    const view = new DataView(data);
    let offset = 0;

    // Read Count
    const count = view.getUint32(offset, true);
    offset += 4;

    const result: ArrayBuffer[] = new Array(count);

    for (let i = 0; i < count; i++) {
      // Read Length
      const len = view.getUint32(offset, true);
      offset += 4;

      // Extract Buffer
      // We use .slice() here to create a distinct ArrayBuffer for the consumer
      // If we didn't slice, all buffers in the list would share the same memory slab.
      result[i] = data.slice(offset, offset + len);

      offset += len;
    }

    return result;
  }
}

export class NumberParser extends Parser<number> {
  encode(value: number): ArrayBuffer {
    const buffer = new ArrayBuffer(4);
    const view = new DataView(buffer);
    view.setFloat32(0, value, true);
    return buffer;
  }

  decode(data: ArrayBuffer): number {
    const view = new DataView(data);
    return view.getFloat32(0, true);
  }
}

export class GenericParser<T> extends Parser<T> {
  constructor(
    private readonly parser: {
      encode(value: T): ArrayBuffer;
      decode(data: ArrayBuffer): T;
      create(value?: T): T;
      check(value: unknown): value is T;
    },
  ) {
    super();
  }

  encode(value: T): ArrayBuffer {
    return this.parser.encode(value);
  }

  decode(data: ArrayBuffer): T {
    return this.parser.decode(data);
  }
  create(): T {
    return this.parser.create();
  }
  copy(value: T): T {
    return this.parser.create(value);
  }
  check(value: unknown): value is T {
    return this.parser.check(value);
  }
}
export const Parsers: Readonly<{
  StringList: StringListParser;
  Uint16Array: Uint16ArrayParser;
  Float32Array: Float32ArrayParser;
  Uint8Array: Uint8ArrayParser;
  Uint32Array: Uint32ArrayParser;
  IdVersion: IdVersionParser;
  ArrayBufferList: ArrayBufferListParser;
  Number: NumberParser;
}> = Object.freeze({
  StringList: new StringListParser(),
  Uint16Array: new Uint16ArrayParser(),
  Float32Array: new Float32ArrayParser(),
  Uint8Array: new Uint8ArrayParser(),
  Uint32Array: new Uint32ArrayParser(),
  IdVersion: new IdVersionParser(),
  ArrayBufferList: new ArrayBufferListParser(),
  Number: new NumberParser(),
});
