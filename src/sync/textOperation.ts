import * as DiffMatchPatch from 'diff-match-patch';

export type TextComponent =
    | { kind: 'retain'; count: number }
    | { kind: 'insert'; text: string }
    | { kind: 'delete'; count: number };

export interface LegacyTextEdit {
    p: number;
    i?: string;
    d?: string;
    u?: boolean;
}

type MutableComponent =
    | { kind: 'retain'; count: number }
    | { kind: 'insert'; text: string }
    | { kind: 'delete'; count: number };

function componentLength(component: MutableComponent): number {
    return component.kind === 'insert' ? component.text.length : component.count;
}

function takeComponent(component: MutableComponent, length: number): MutableComponent {
    if (length <= 0 || length > componentLength(component)) {
        throw new Error(`Invalid component slice length: ${length}`);
    }
    if (component.kind === 'insert') {
        return { kind: 'insert', text: component.text.slice(0, length) };
    }
    return { kind: component.kind, count: length };
}

function consumeComponent(component: MutableComponent, length: number): MutableComponent | undefined {
    const remaining = componentLength(component) - length;
    if (remaining === 0) {
        return undefined;
    }
    if (remaining < 0) {
        throw new Error('Consumed beyond the end of a text operation component');
    }
    if (component.kind === 'insert') {
        return { kind: 'insert', text: component.text.slice(length) };
    }
    return { kind: component.kind, count: remaining };
}

/**
 * A small UTF-16 text OT primitive compatible with the retain/insert/delete
 * semantics used by OT.js and Overleaf's legacy ShareJS protocol.
 *
 * JavaScript and VS Code offsets are UTF-16 code-unit offsets, so counts use
 * String.length rather than UTF-8 byte length.
 */
export class TextOperation {
    private readonly _components: TextComponent[] = [];
    private _baseLength = 0;
    private _targetLength = 0;

    get components(): readonly TextComponent[] {
        return this._components;
    }

    get baseLength(): number {
        return this._baseLength;
    }

    get targetLength(): number {
        return this._targetLength;
    }

    get isNoop(): boolean {
        return this._components.every(component => component.kind === 'retain');
    }

    retain(count: number): this {
        if (!Number.isInteger(count) || count < 0) {
            throw new Error('retain expects a non-negative integer');
        }
        if (count === 0) {
            return this;
        }
        this._baseLength += count;
        this._targetLength += count;
        const last = this._components.at(-1);
        if (last?.kind === 'retain') {
            last.count += count;
        } else {
            this._components.push({ kind: 'retain', count });
        }
        return this;
    }

    insert(text: string): this {
        if (typeof text !== 'string') {
            throw new Error('insert expects a string');
        }
        assertWellFormedUtf16(text);
        if (text.length === 0) {
            return this;
        }
        this._targetLength += text.length;
        const last = this._components.at(-1);
        if (last?.kind === 'insert') {
            last.text += text;
        } else if (last?.kind === 'delete') {
            const beforeDelete = this._components.at(-2);
            if (beforeDelete?.kind === 'insert') {
                beforeDelete.text += text;
            } else {
                this._components.splice(this._components.length - 1, 0, { kind: 'insert', text });
            }
        } else {
            this._components.push({ kind: 'insert', text });
        }
        return this;
    }

    remove(count: number): this {
        if (!Number.isInteger(count) || count < 0) {
            throw new Error('remove expects a non-negative integer');
        }
        if (count === 0) {
            return this;
        }
        this._baseLength += count;
        const last = this._components.at(-1);
        if (last?.kind === 'delete') {
            last.count += count;
        } else {
            this._components.push({ kind: 'delete', count });
        }
        return this;
    }

    apply(input: string): string {
        assertWellFormedUtf16(input);
        if (input.length !== this._baseLength) {
            throw new Error(`Operation base length ${this._baseLength} does not match input length ${input.length}`);
        }
        let cursor = 0;
        let output = '';
        for (const component of this._components) {
            switch (component.kind) {
                case 'retain':
                    output += input.slice(cursor, cursor + component.count);
                    cursor += component.count;
                    break;
                case 'insert':
                    output += component.text;
                    break;
                case 'delete':
                    cursor += component.count;
                    break;
            }
        }
        if (cursor !== input.length) {
            throw new Error('Operation did not consume the complete input');
        }
        return output;
    }

    compose(next: TextOperation): TextOperation {
        if (this._targetLength !== next._baseLength) {
            throw new Error('Cannot compose operations with incompatible lengths');
        }
        const result = new TextOperation();
        const left = this.mutableComponents();
        const right = next.mutableComponents();
        let leftIndex = 0;
        let rightIndex = 0;
        let a = left[leftIndex++];
        let b = right[rightIndex++];

        while (a !== undefined || b !== undefined) {
            if (a?.kind === 'delete') {
                result.remove(a.count);
                a = left[leftIndex++];
                continue;
            }
            if (b?.kind === 'insert') {
                result.insert(b.text);
                b = right[rightIndex++];
                continue;
            }
            if (a === undefined || b === undefined) {
                throw new Error('Cannot compose malformed operations');
            }

            const length = Math.min(componentLength(a), componentLength(b));
            if (a.kind === 'retain' && b.kind === 'retain') {
                result.retain(length);
            } else if (a.kind === 'insert' && b.kind === 'retain') {
                result.insert(takeComponent(a, length).kind === 'insert'
                    ? (takeComponent(a, length) as {kind:'insert';text:string}).text
                    : '');
            } else if (a.kind === 'retain' && b.kind === 'delete') {
                result.remove(length);
            } else if (a.kind === 'insert' && b.kind === 'delete') {
                // The second operation removes text inserted by the first.
            } else {
                throw new Error('Unsupported operation composition');
            }

            a = consumeComponent(a, length) ?? left[leftIndex++];
            b = consumeComponent(b, length) ?? right[rightIndex++];
        }
        return result;
    }

    /**
     * Transform concurrent operations. Insert/insert ties are resolved in
     * favour of the left operation, matching the classic OT.js algorithm.
     */
    static transform(leftOperation: TextOperation, rightOperation: TextOperation): [TextOperation, TextOperation] {
        if (leftOperation._baseLength !== rightOperation._baseLength) {
            throw new Error('Concurrent operations must have the same base length');
        }
        const leftPrime = new TextOperation();
        const rightPrime = new TextOperation();
        const left = leftOperation.mutableComponents();
        const right = rightOperation.mutableComponents();
        let leftIndex = 0;
        let rightIndex = 0;
        let a = left[leftIndex++];
        let b = right[rightIndex++];

        while (a !== undefined || b !== undefined) {
            if (a?.kind === 'insert') {
                leftPrime.insert(a.text);
                rightPrime.retain(a.text.length);
                a = left[leftIndex++];
                continue;
            }
            if (b?.kind === 'insert') {
                leftPrime.retain(b.text.length);
                rightPrime.insert(b.text);
                b = right[rightIndex++];
                continue;
            }
            if (a === undefined || b === undefined) {
                throw new Error('Cannot transform malformed operations');
            }

            const length = Math.min(componentLength(a), componentLength(b));
            if (a.kind === 'retain' && b.kind === 'retain') {
                leftPrime.retain(length);
                rightPrime.retain(length);
            } else if (a.kind === 'delete' && b.kind === 'delete') {
                // Both operations delete the same source range.
            } else if (a.kind === 'delete' && b.kind === 'retain') {
                leftPrime.remove(length);
            } else if (a.kind === 'retain' && b.kind === 'delete') {
                rightPrime.remove(length);
            } else {
                throw new Error('Unsupported operation transformation');
            }

            a = consumeComponent(a, length) ?? left[leftIndex++];
            b = consumeComponent(b, length) ?? right[rightIndex++];
        }
        return [leftPrime, rightPrime];
    }

    equals(other: TextOperation): boolean {
        return JSON.stringify(this.toJSON()) === JSON.stringify(other.toJSON());
    }

    toJSON(): TextComponent[] {
        return this._components.map(component => ({ ...component }));
    }

    static fromJSON(components: readonly TextComponent[]): TextOperation {
        const operation = new TextOperation();
        for (const component of components) {
            switch (component.kind) {
                case 'retain': operation.retain(component.count); break;
                case 'insert': operation.insert(component.text); break;
                case 'delete': operation.remove(component.count); break;
                default: throw new Error('Unknown text operation component');
            }
        }
        return operation;
    }

    static fromDiff(before: string, after: string): TextOperation {
        assertWellFormedUtf16(before);
        assertWellFormedUtf16(after);
        if (before === after) {
            return new TextOperation().retain(before.length);
        }
        const dmp = new DiffMatchPatch();
        const diffs = dmp.diff_main(before, after);
        const operation = new TextOperation();
        for (const [kind, text] of diffs) {
            if (kind === 0) {
                operation.retain(text.length);
            } else if (kind === 1) {
                operation.insert(text);
            } else if (kind === -1) {
                operation.remove(text.length);
            }
        }
        return operation;
    }

    toLegacyEdits(base: string): LegacyTextEdit[] {
        if (base.length !== this._baseLength) {
            throw new Error('Legacy conversion base does not match operation length');
        }
        const edits: LegacyTextEdit[] = [];
        let inputCursor = 0;
        let outputCursor = 0;
        for (const component of this._components) {
            switch (component.kind) {
                case 'retain':
                    inputCursor += component.count;
                    outputCursor += component.count;
                    break;
                case 'insert':
                    edits.push({ p: outputCursor, i: component.text });
                    outputCursor += component.text.length;
                    break;
                case 'delete':
                    edits.push({ p: outputCursor, d: base.slice(inputCursor, inputCursor + component.count) });
                    inputCursor += component.count;
                    break;
            }
        }
        if (inputCursor !== base.length) {
            throw new Error('Legacy conversion did not consume the complete base');
        }
        return edits;
    }

    static fromLegacyEdits(base: string, edits: readonly LegacyTextEdit[], decode = (value: string) => value): TextOperation {
        let target = base;
        for (const edit of edits) {
            if (!Number.isInteger(edit.p) || edit.p < 0 || edit.p > target.length) {
                throw new Error(`Invalid legacy edit position: ${edit.p}`);
            }
            if (edit.i !== undefined) {
                const insertion = decode(edit.i);
                target = target.slice(0, edit.p) + insertion + target.slice(edit.p);
            }
            if (edit.d !== undefined) {
                const deletion = decode(edit.d);
                if (target.slice(edit.p, edit.p + deletion.length) !== deletion) {
                    throw new Error('Legacy deletion does not match the current document content');
                }
                target = target.slice(0, edit.p) + target.slice(edit.p + deletion.length);
            }
        }
        return TextOperation.fromDiff(base, target);
    }

    private mutableComponents(): MutableComponent[] {
        return this._components.map(component => ({ ...component })) as MutableComponent[];
    }
}

function assertWellFormedUtf16(value: string): void {
    for (let index = 0; index < value.length; index++) {
        const unit = value.charCodeAt(index);
        if (unit >= 0xd800 && unit <= 0xdbff) {
            const next = value.charCodeAt(index + 1);
            if (!(next >= 0xdc00 && next <= 0xdfff)) {
                throw new Error('Text contains an unpaired UTF-16 high surrogate');
            }
            index++;
        } else if (unit >= 0xdc00 && unit <= 0xdfff) {
            throw new Error('Text contains an unpaired UTF-16 low surrogate');
        }
    }
}
