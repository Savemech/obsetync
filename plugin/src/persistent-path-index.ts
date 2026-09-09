interface PathNode<V> {
    readonly key: string;
    readonly value: V;
    readonly left: PathNode<V> | null;
    readonly right: PathNode<V> | null;
    readonly height: number;
    readonly size: number;
}

function height<V>(node: PathNode<V> | null): number { return node?.height ?? 0; }
function size<V>(node: PathNode<V> | null): number { return node?.size ?? 0; }

function node<V>(key: string, value: V, left: PathNode<V> | null, right: PathNode<V> | null): PathNode<V> {
    return { key, value, left, right, height: 1 + Math.max(height(left), height(right)),
        size: 1 + size(left) + size(right) };
}

function rotateLeft<V>(root: PathNode<V>): PathNode<V> {
    const pivot = root.right!;
    return node(pivot.key, pivot.value,
        node(root.key, root.value, root.left, pivot.left), pivot.right);
}

function rotateRight<V>(root: PathNode<V>): PathNode<V> {
    const pivot = root.left!;
    return node(pivot.key, pivot.value, pivot.left,
        node(root.key, root.value, pivot.right, root.right));
}

function balance<V>(root: PathNode<V>): PathNode<V> {
    const difference = height(root.left) - height(root.right);
    if (difference > 1) {
        const left = root.left!;
        return rotateRight(height(left.left) >= height(left.right) ? root
            : node(root.key, root.value, rotateLeft(left), root.right));
    }
    if (difference < -1) {
        const right = root.right!;
        return rotateLeft(height(right.right) >= height(right.left) ? root
            : node(root.key, root.value, root.left, rotateRight(right)));
    }
    return root;
}

function insert<V>(root: PathNode<V> | null, key: string, value: V): PathNode<V> {
    if (!root) return node(key, value, null, null);
    if (key === root.key) {
        return Object.is(value, root.value) ? root : node(key, value, root.left, root.right);
    }
    if (key < root.key) {
        const left = insert(root.left, key, value);
        return left === root.left ? root : balance(node(root.key, root.value, left, root.right));
    }
    const right = insert(root.right, key, value);
    return right === root.right ? root : balance(node(root.key, root.value, root.left, right));
}

function remove<V>(root: PathNode<V> | null, key: string): PathNode<V> | null {
    if (!root) return null;
    if (key < root.key) {
        const left = remove(root.left, key);
        return left === root.left ? root : balance(node(root.key, root.value, left, root.right));
    }
    if (key > root.key) {
        const right = remove(root.right, key);
        return right === root.right ? root : balance(node(root.key, root.value, root.left, right));
    }
    if (!root.left) return root.right;
    if (!root.right) return root.left;
    let successor = root.right;
    while (successor.left) successor = successor.left;
    return balance(node(successor.key, successor.value, root.left, remove(root.right, successor.key)));
}

function* iterate<V>(root: PathNode<V> | null): IterableIterator<[string, V]> {
    const stack: PathNode<V>[] = [];
    let current = root;
    while (current || stack.length > 0) {
        while (current) { stack.push(current); current = current.left; }
        const next = stack.pop()!;
        yield [next.key, next.value];
        current = next.right;
    }
}

/** Immutable AVL map with O(log n) path-copy updates and O(1) captured roots.
 * Iteration uses deterministic JS string ordering (UTF-16 code units), not
 * locale-dependent collation, and an O(log n) traversal stack.
 *
 * Values are shared, not cloned/frozen: callers MUST treat stored values as
 * immutable. Keeping historical roots/iterators keeps their reachable nodes
 * and values alive. This is an O(n) live metadata index, not a disk-paged map. */
export class PersistentPathIndex<V> {
    private root: PathNode<V> | null = null;

    get size(): number { return size(this.root); }

    get(key: string): V | undefined {
        let current = this.root;
        while (current) {
            if (key === current.key) return current.value;
            current = key < current.key ? current.left : current.right;
        }
        return undefined;
    }

    set(key: string, value: V): PersistentPathIndex<V> {
        return this.withRoot(insert(this.root, key, value));
    }

    delete(key: string): PersistentPathIndex<V> {
        return this.withRoot(remove(this.root, key));
    }

    entries(): IterableIterator<[string, V]> { return iterate(this.root); }

    *keys(): IterableIterator<string> {
        for (const [key] of this.entries()) yield key;
    }

    private withRoot(root: PathNode<V> | null): PersistentPathIndex<V> {
        if (root === this.root) return this;
        const next = new PersistentPathIndex<V>();
        next.root = root;
        return next;
    }
}
